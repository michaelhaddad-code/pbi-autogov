# -*- coding: utf-8 -*-
"""
Skill 7: tmdl_cleanup.py
PBI AutoGov — Power BI Data Governance Automation Pipeline

Directly removes unused column and measure blocks from TMDL source files.
Uses Function5 output as the source of truth for what to remove.

Modes:
    tmdl_only — remove measures (SourceColumn == "[Measure]") and
                calculated columns (SourceColumn empty)
    all       — remove everything with Remove_column == "Yes"

Safety:
    - Creates .tmdl.bak backup before editing any file
    - Logs every removal and skip
    - Cascade-removes hierarchies that reference deleted columns
    - Cascade-removes variation sub-blocks that reference deleted hierarchies (cross-file)
    - Never touches partition or annotation blocks at table level, or the table declaration

Input:  Function5_Output.xlsx, TMDL tables directory
Output: TMDL_CLEANUP_REPORT.xlsx (2 sheets: Removed, Skipped)
"""

import argparse
import re
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


# ============================================================
# Block detection
# ============================================================

def find_block_range(lines: List[str], start_idx: int) -> Tuple[int, int]:
    """Find the line range of a column/measure block.

    Scans forward from the declaration line until the next sibling block
    (column, measure, hierarchy, partition, or annotation at single-tab level)
    or end-of-file.

    Args:
        lines: All lines in the TMDL file (split by newline).
        start_idx: Index of the block's declaration line.

    Returns:
        (start_idx, end_idx) — the block spans lines[start_idx:end_idx].
    """
    # Sibling-level pattern: next block at single-tab (\t) indentation
    sibling_re = re.compile(r"^\t(?:column|measure|hierarchy|partition|annotation)\s")

    end_idx = start_idx + 1
    while end_idx < len(lines):
        if sibling_re.match(lines[end_idx]):
            break
        end_idx += 1

    return start_idx, end_idx


def find_variation_range(lines: List[str], start_idx: int) -> Tuple[int, int]:
    """Find the line range of a variation sub-block within a column.

    Variation blocks sit at \\t\\t level inside column blocks.
    Scans forward until the next \\t\\t-level sibling, \\t-level block, or EOF.
    Trailing blank lines are included in the range so removal is clean.

    Args:
        lines: All lines in the TMDL file.
        start_idx: Index of the ``\\t\\tvariation`` declaration line.

    Returns:
        (start_idx, end_idx) — the sub-block spans lines[start_idx:end_idx].
    """
    end_idx = start_idx + 1
    while end_idx < len(lines):
        line = lines[end_idx]
        if line.strip() == "":
            # Blank line — consume it (it's a separator between sub-blocks)
            end_idx += 1
            continue
        if line.startswith("\t\t\t"):
            # Deeper content (part of variation block)
            end_idx += 1
        elif line.startswith("\t\t") or (line.startswith("\t") and not line.startswith("\t\t")):
            # Sibling at \t\t level or parent at \t level — stop
            break
        else:
            # Table-level or less — stop
            break
    return start_idx, end_idx


# ============================================================
# Block removal from a single TMDL file
# ============================================================

def remove_blocks_from_tmdl(
    tmdl_path: Path,
    items_to_remove: List[Dict],
) -> Tuple[int, List[Dict], List[Dict]]:
    """Remove matching column/measure blocks from a TMDL file.

    Creates a .tmdl.bak backup before writing any changes.
    Processes removals bottom-to-top so line indices stay valid.

    Args:
        tmdl_path: Path to the .tmdl file.
        items_to_remove: List of dicts with keys:
            name (str): Column/measure name (unquoted).
            block_type (str): "column" or "measure" — controls the match regex.
            item_type (str): Display label (Measure / Calculated Column / Imported Column).
            table (str): Table name (for reporting).

    Returns:
        (removed_count, removed_items, skipped_items, deleted_hierarchies)
        where deleted_hierarchies is a set of qualified names like
        "TableName.'Date Hierarchy'" for cross-file variation cleanup.
    """
    content = tmdl_path.read_text(encoding="utf-8-sig")
    lines = content.split("\n")

    blocks = []       # (start, end) tuples for lines to delete
    removed = []      # items successfully matched
    skipped = []      # items not found

    for item in items_to_remove:
        name_esc = re.escape(item["name"])

        if item["block_type"] == "measure":
            # Match: \tmeasure 'Name' = DAX  or  \tmeasure Name = DAX
            pat = re.compile(rf"^\tmeasure\s+'?{name_esc}'?\s*=")
        else:
            # Match: \tcolumn 'Name'  or  \tcolumn Name = DAX (calculated)  or  \tcolumn Name
            pat = re.compile(rf"^\tcolumn\s+'?{name_esc}'?\s*(?:=.*)?$")

        found = False
        for i, line in enumerate(lines):
            if pat.match(line):
                start, end = find_block_range(lines, i)
                blocks.append((start, end))
                removed.append(item)
                found = True
                break

        if not found:
            skipped.append({**item, "reason": "Not found in TMDL"})
            print(f"  WARNING: {item['block_type']} '{item['name']}' not found in {tmdl_path.name}")

    if not blocks:
        return 0, removed, skipped, set()

    # Cascade: find hierarchy blocks that reference any removed column.
    # If a hierarchy level points to a column we're deleting, the hierarchy
    # must also be removed — otherwise PBI Desktop rejects the broken reference.
    removed_col_names = {item["name"] for item in removed if item["block_type"] == "column"}
    if removed_col_names:
        hierarchy_re = re.compile(r"^\thierarchy\s+")
        # level column references sit at 3-tab depth: \t\t\tcolumn: Name
        col_ref_re = re.compile(r"^\t\t\tcolumn:\s*(.+)$")
        existing_starts = {b[0] for b in blocks}

        # Match: \thierarchy 'Name'  or  \thierarchy Name
        h_name_re = re.compile(r"^\thierarchy\s+'?(.+?)'?\s*$")

        for i, line in enumerate(lines):
            if hierarchy_re.match(line) and i not in existing_starts:
                h_start, h_end = find_block_range(lines, i)
                # Check if any level in this hierarchy references a removed column
                for j in range(h_start, h_end):
                    col_match = col_ref_re.match(lines[j])
                    if col_match:
                        ref_name = col_match.group(1).strip().strip("'")
                        if ref_name in removed_col_names:
                            blocks.append((h_start, h_end))
                            # Extract hierarchy name for the removed list
                            h_match = h_name_re.match(line)
                            h_label = h_match.group(1) if h_match else "(unknown)"
                            removed.append({
                                "table": tmdl_path.stem,
                                "name": h_label,
                                "item_type": "Hierarchy (cascade)",
                                "block_type": "hierarchy",
                            })
                            print(f"    Cascade: removing hierarchy (references deleted column '{ref_name}')")
                            break

    # Extract qualified names of deleted hierarchies (for cross-file variation cleanup).
    # Must happen BEFORE lines are spliced so we can read the declaration lines.
    deleted_hierarchies = set()
    # Match: \thierarchy 'Date Hierarchy'  or  \thierarchy DateHierarchy
    hierarchy_name_re = re.compile(r"^\thierarchy\s+'?(.+?)'?\s*$")
    table_name = tmdl_path.stem  # filename without .tmdl
    for start, end in blocks:
        h_match = hierarchy_name_re.match(lines[start])
        if h_match:
            h_name = h_match.group(1)
            qualified = f"{table_name}.'{h_name}'"
            deleted_hierarchies.add(qualified)

    # Create backup before editing
    backup_path = tmdl_path.with_suffix(".tmdl.bak")
    shutil.copy2(tmdl_path, backup_path)
    print(f"  Backup: {backup_path.name}")

    # Remove blocks bottom-to-top to preserve earlier indices
    blocks.sort(key=lambda x: x[0], reverse=True)
    for start, end in blocks:
        del lines[start:end]

    # Write without BOM — Power BI Desktop requires UTF-8 without BOM for TMDL files
    tmdl_path.write_text("\n".join(lines), encoding="utf-8")

    return len(removed), removed, skipped, deleted_hierarchies


# ============================================================
# Cross-file variation cleanup
# ============================================================

def remove_orphaned_variations(
    tables_dir: Path,
    deleted_hierarchies: set,
) -> List[Dict]:
    """Remove variation sub-blocks whose defaultHierarchy points to a deleted hierarchy.

    Variation blocks sit inside column blocks at \\t\\t level and reference
    hierarchies in OTHER tables via ``defaultHierarchy: TableName.'HierarchyName'``.
    When that hierarchy has been cascade-removed, the variation must also go
    or PBI Desktop will reject the broken reference.

    Also strips the ``showAsVariationsOnly`` property from any table that is
    no longer the target of any variation — PBI Desktop requires that tables
    with this flag are referenced by at least one variation block.

    Args:
        tables_dir: Path to the semantic model tables/ directory.
        deleted_hierarchies: Set of qualified hierarchy names
            (e.g. ``"LocalDateTable_xxx.'Date Hierarchy'"``).

    Returns:
        List of removed item dicts (for inclusion in the cleanup report).
    """
    cascade_removed = []

    if not deleted_hierarchies:
        return cascade_removed

    # \t\tvariation Variation  (sub-block within a column)
    variation_re = re.compile(r"^\t\tvariation\s+'?(.+?)'?\s*$")
    # \t\t\tdefaultHierarchy: TableName.'HierarchyName'
    default_h_re = re.compile(r"^\t\t\tdefaultHierarchy:\s*(.+)$")

    for tmdl_path in sorted(tables_dir.glob("*.tmdl")):
        content = tmdl_path.read_text(encoding="utf-8-sig")
        lines = content.split("\n")

        blocks_to_remove = []

        for i, line in enumerate(lines):
            v_match = variation_re.match(line)
            if v_match:
                v_start, v_end = find_variation_range(lines, i)
                # Check if defaultHierarchy references a deleted hierarchy
                for j in range(v_start, v_end):
                    dh_match = default_h_re.match(lines[j])
                    if dh_match:
                        ref = dh_match.group(1).strip()
                        if ref in deleted_hierarchies:
                            blocks_to_remove.append((v_start, v_end))
                            cascade_removed.append({
                                "table": tmdl_path.stem,
                                "name": v_match.group(1),
                                "item_type": "Variation (cascade)",
                                "block_type": "variation",
                            })
                            print(f"    Cascade: removing variation in {tmdl_path.name} "
                                  f"(references deleted hierarchy '{ref}')")
                            break

        if not blocks_to_remove:
            continue

        # Backup if not already backed up (file may have been edited in the main pass)
        backup_path = tmdl_path.with_suffix(".tmdl.bak")
        if not backup_path.exists():
            shutil.copy2(tmdl_path, backup_path)
            print(f"  Backup: {backup_path.name}")

        # Remove bottom-to-top
        blocks_to_remove.sort(key=lambda x: x[0], reverse=True)
        for start, end in blocks_to_remove:
            del lines[start:end]

        tmdl_path.write_text("\n".join(lines), encoding="utf-8")

    # --- Strip showAsVariationsOnly from tables no longer targeted by any variation ---
    # PBI Desktop requires: if showAsVariationsOnly=1, the table must be referenced
    # by at least one variation's defaultHierarchy somewhere in the model.
    # Extract table names from deleted hierarchies (part before the first dot)
    potentially_orphaned = set()
    for h in deleted_hierarchies:
        table_name = h.split(".", 1)[0]
        potentially_orphaned.add(table_name)

    if not potentially_orphaned:
        return cascade_removed

    # Scan ALL TMDL files for remaining defaultHierarchy references to these tables
    still_referenced = set()
    for tmdl_path in tables_dir.glob("*.tmdl"):
        content = tmdl_path.read_text(encoding="utf-8-sig")
        for tname in potentially_orphaned:
            if f"defaultHierarchy: {tname}." in content:
                still_referenced.add(tname)

    truly_orphaned = potentially_orphaned - still_referenced
    for tname in sorted(truly_orphaned):
        table_file = tables_dir / f"{tname}.tmdl"
        if not table_file.is_file():
            continue

        content = table_file.read_text(encoding="utf-8-sig")
        if "\tshowAsVariationsOnly" not in content:
            continue

        # Backup if not already backed up
        backup_path = table_file.with_suffix(".tmdl.bak")
        if not backup_path.exists():
            shutil.copy2(table_file, backup_path)
            print(f"  Backup: {backup_path.name}")

        # Remove the showAsVariationsOnly line
        lines = content.split("\n")
        lines = [l for l in lines if l.strip() != "showAsVariationsOnly"]
        table_file.write_text("\n".join(lines), encoding="utf-8")
        print(f"    Stripped showAsVariationsOnly from {table_file.name} (no longer a variation target)")

    return cascade_removed


# ============================================================
# Item classification helper
# ============================================================

def _classify_item(row) -> Tuple[str, str]:
    """Classify a Function5 row by SourceColumn.

    Returns:
        (item_type, block_type) where:
        - item_type: "Measure", "Calculated Column", or "Imported Column"
        - block_type: "measure" or "column" (for TMDL regex matching)
    """
    src = str(row.get("SourceColumn", "")).strip() if pd.notna(row.get("SourceColumn")) else ""
    if src == "[Measure]":
        return "Measure", "measure"
    elif src == "":
        return "Calculated Column", "column"
    else:
        return "Imported Column", "column"


# ============================================================
# Variation-aware protection
# ============================================================

def _find_protected_hierarchy_columns(
    tables_path: Path,
    to_remove_set: set,
) -> set:
    """Find columns that must be protected because they support hierarchies
    referenced by variations in kept columns.

    When a kept column (e.g. DateTable.Date) has a variation pointing to a
    hierarchy in another table (e.g. LocalDateTable_xxx.'Date Hierarchy'),
    the hierarchy and all columns it references (Year, Quarter, Month, Day)
    must be protected from removal — otherwise PBI Desktop loses the date
    drill-down capability.

    Args:
        tables_path: Path to TMDL tables directory.
        to_remove_set: Set of ``"TableName$$ColumnName"`` keys flagged for removal.

    Returns:
        Set of ``"TableName$$ColumnName"`` keys that must be protected.
    """
    protected = set()

    # Pattern matchers
    # Match: \tcolumn 'Name'  or  \tcolumn Name  or  \tcolumn Name = DAX
    column_re = re.compile(r"^\tcolumn\s+'?(.+?)'?\s*(?:=.*)?$")
    variation_re = re.compile(r"^\t\tvariation\s+")
    default_h_re = re.compile(r"^\t\t\tdefaultHierarchy:\s*(.+)$")
    hierarchy_re = re.compile(r"^\thierarchy\s+'?(.+?)'?\s*$")
    # \t\t\tcolumn: Name  (level column reference inside a hierarchy)
    col_ref_re = re.compile(r"^\t\t\tcolumn:\s*(.+)$")
    # Sibling-level blocks at \t indentation (used to track current column scope)
    sibling_re = re.compile(r"^\t(?:column|measure|hierarchy|partition|annotation)\s")

    # Step 1: Find variations in KEPT columns → collect protected hierarchy references
    protected_hierarchies = set()  # "TableName.'HierarchyName'" strings

    for tmdl_path in tables_path.glob("*.tmdl"):
        table_name = tmdl_path.stem
        content = tmdl_path.read_text(encoding="utf-8-sig")
        lines = content.split("\n")

        current_column = None
        for i, line in enumerate(lines):
            col_match = column_re.match(line)
            if col_match:
                current_column = col_match.group(1)
            elif sibling_re.match(line) and not line.lstrip("\t").startswith("column "):
                # Non-column sibling block — reset scope
                current_column = None

            if variation_re.match(line) and current_column is not None:
                key = f"{table_name}$${current_column}"
                if key not in to_remove_set:
                    # This column is KEPT — its variation's hierarchy must be protected
                    v_start, v_end = find_variation_range(lines, i)
                    for j in range(v_start, v_end):
                        dh_match = default_h_re.match(lines[j])
                        if dh_match:
                            protected_hierarchies.add(dh_match.group(1).strip())
                            break

    if not protected_hierarchies:
        return protected

    # Step 2: For each protected hierarchy, protect its level columns from removal
    for tmdl_path in tables_path.glob("*.tmdl"):
        table_name = tmdl_path.stem
        content = tmdl_path.read_text(encoding="utf-8-sig")
        lines = content.split("\n")

        for i, line in enumerate(lines):
            h_match = hierarchy_re.match(line)
            if h_match:
                h_name = h_match.group(1)
                qualified = f"{table_name}.'{h_name}'"
                if qualified in protected_hierarchies:
                    # Hierarchy is protected — protect its column references
                    h_start, h_end = find_block_range(lines, i)
                    for j in range(h_start, h_end):
                        cr_match = col_ref_re.match(lines[j])
                        if cr_match:
                            ref_col = cr_match.group(1).strip().strip("'")
                            protected.add(f"{table_name}$${ref_col}")

    # Step 3: Protect sortByColumn targets of any protected column.
    # E.g. Month (protected) has sortByColumn: MonthNo → MonthNo must also be kept.
    # \t\tsortByColumn: ColumnName  (at \t\t level inside a column block)
    sort_by_re = re.compile(r"^\t\tsortByColumn:\s*(.+)$")
    newly_protected = set()

    for tmdl_path in tables_path.glob("*.tmdl"):
        table_name = tmdl_path.stem
        content = tmdl_path.read_text(encoding="utf-8-sig")
        lines = content.split("\n")

        current_column = None
        for line in lines:
            col_match = column_re.match(line)
            if col_match:
                current_column = col_match.group(1)
            elif sibling_re.match(line) and not line.lstrip("\t").startswith("column "):
                current_column = None

            if current_column is not None:
                col_key = f"{table_name}$${current_column}"
                # Check both originally-kept columns AND hierarchy-protected columns
                if col_key in protected or col_key not in to_remove_set:
                    sb_match = sort_by_re.match(line)
                    if sb_match:
                        sort_col = sb_match.group(1).strip().strip("'")
                        sort_key = f"{table_name}$${sort_col}"
                        if sort_key in to_remove_set and sort_key not in protected:
                            newly_protected.add(sort_key)

    protected.update(newly_protected)
    return protected


# ============================================================
# Main cleanup entry point
# ============================================================

def run_tmdl_cleanup(
    function5_file: str,
    tables_dir: str,
    mode: str = "tmdl_only",
) -> Tuple[List[Dict], List[Dict]]:
    """Read Function5 output and remove flagged items from TMDL files.

    Args:
        function5_file: Path to Function5_Output.xlsx.
        tables_dir: Path to the semantic model tables/ directory.
        mode: "tmdl_only" (measures + calculated columns) or "all" (everything flagged).

    Returns:
        (all_removed, all_skipped) — lists of item dicts.
    """
    print("=" * 60)
    print("PBI AutoGov — TMDL Cleanup")
    print("=" * 60)
    print(f"  Mode: {'measures + calculated columns' if mode == 'tmdl_only' else 'all flagged items'}")

    f5 = pd.read_excel(function5_file)
    tables_path = Path(tables_dir)

    # Filter to Remove=Yes rows
    to_remove = f5[f5["Remove_column"].astype(str).str.strip().str.lower() == "yes"].copy()

    if mode == "tmdl_only":
        # Only measures (SourceColumn == "[Measure]") and calculated columns (SourceColumn empty)
        src = to_remove["SourceColumn"].fillna("").astype(str).str.strip()
        to_remove = to_remove[(src == "") | (src == "[Measure]")]

    if to_remove.empty:
        print("  No items to remove from TMDL files.")
        return [], []

    # Protect columns that support hierarchies referenced by kept columns' variations.
    # E.g. LocalDateTable.Year must be kept if DateTable.Date (kept) has a variation
    # pointing to LocalDateTable.'Date Hierarchy' which references Year.
    to_remove_set = {
        f"{row['TableName']}$${row['ColumnName']}" for _, row in to_remove.iterrows()
    }
    protected_cols = _find_protected_hierarchy_columns(tables_path, to_remove_set)
    if protected_cols:
        before = len(to_remove)
        to_remove = to_remove[
            ~to_remove.apply(
                lambda r: f"{r['TableName']}$${r['ColumnName']}" in protected_cols, axis=1
            )
        ]
        print(f"  Protected {before - len(to_remove)} column(s) supporting active date hierarchies")

    if to_remove.empty:
        print("  No items to remove from TMDL files (all protected).")
        return [], []

    print(f"  Items to remove: {len(to_remove)}")

    all_removed = []
    all_skipped = []
    all_deleted_hierarchies = set()

    for table_name, group in to_remove.groupby("TableName"):
        tmdl_file = tables_path / f"{table_name}.tmdl"

        if not tmdl_file.is_file():
            print(f"  WARNING: TMDL file not found: {tmdl_file}")
            for _, row in group.iterrows():
                item_type, _ = _classify_item(row)
                all_skipped.append({
                    "table": str(table_name),
                    "name": str(row["ColumnName"]),
                    "item_type": item_type,
                    "block_type": "column",
                    "reason": "TMDL file not found",
                })
            continue

        items = []
        for _, row in group.iterrows():
            item_type, block_type = _classify_item(row)
            items.append({
                "table": str(table_name),
                "name": str(row["ColumnName"]),
                "item_type": item_type,
                "block_type": block_type,
            })

        count, removed, skipped, deleted_hierarchies = remove_blocks_from_tmdl(tmdl_file, items)
        all_removed.extend(removed)
        all_skipped.extend(skipped)
        all_deleted_hierarchies.update(deleted_hierarchies)

        if count > 0:
            print(f"  {tmdl_file.name}: removed {count} item(s)")

    # Cross-file cascade: remove variation sub-blocks that reference deleted hierarchies.
    # Variations sit inside KEPT columns in OTHER files and have defaultHierarchy
    # pointing to a hierarchy that was cascade-deleted above.
    if all_deleted_hierarchies:
        print(f"\n  Checking for orphaned variations referencing {len(all_deleted_hierarchies)} deleted hierarchy(ies)...")
        cascade_items = remove_orphaned_variations(tables_path, all_deleted_hierarchies)
        all_removed.extend(cascade_items)

    # Delete TMDL files for tables that have no columns or measures left.
    # After cleanup, if a file has no \tcolumn or \tmeasure lines, it's an empty
    # shell (just table declaration + partition + annotations) and can be deleted.
    col_or_measure_re = re.compile(r"^\t(?:column|measure)\s", re.MULTILINE)
    deleted_tables = []
    for tmdl_path in sorted(tables_path.glob("*.tmdl")):
        content = tmdl_path.read_text(encoding="utf-8-sig")
        if not col_or_measure_re.search(content):
            tmdl_path.unlink()
            deleted_tables.append(tmdl_path.stem)
            all_removed.append({
                "table": tmdl_path.stem,
                "name": "(entire table)",
                "item_type": "Empty Table Deleted",
                "block_type": "table",
            })
            print(f"  Deleted empty table file: {tmdl_path.name}")

    # Summary
    measure_count = sum(1 for r in all_removed if r.get("item_type") == "Measure")
    calc_count = sum(1 for r in all_removed if r.get("item_type") == "Calculated Column")
    imported_count = sum(1 for r in all_removed if r.get("item_type") == "Imported Column")
    hierarchy_count = sum(1 for r in all_removed if r.get("item_type") == "Hierarchy (cascade)")
    variation_count = sum(1 for r in all_removed if r.get("item_type") == "Variation (cascade)")
    empty_table_count = sum(1 for r in all_removed if r.get("item_type") == "Empty Table Deleted")

    print(f"\n{'=' * 60}")
    print(f"TMDL Cleanup complete!")
    print(f"  Removed: {len(all_removed)} total")
    if measure_count:
        print(f"    Measures: {measure_count}")
    if calc_count:
        print(f"    Calculated columns: {calc_count}")
    if imported_count:
        print(f"    Imported columns: {imported_count}")
    if hierarchy_count:
        print(f"    Hierarchies (cascade): {hierarchy_count}")
    if variation_count:
        print(f"    Variations (cascade): {variation_count}")
    if empty_table_count:
        print(f"    Empty tables deleted: {empty_table_count}")
    if all_skipped:
        print(f"  Skipped: {len(all_skipped)} (see report for details)")

    # Explain items that could NOT be deleted and why
    not_deletable = []
    # 1. Protected hierarchy/sortByColumn columns (LocalDateTables)
    if protected_cols:
        # Group by table for cleaner output
        from collections import defaultdict
        prot_by_table = defaultdict(list)
        for key in sorted(protected_cols):
            tbl, col = key.split("$$", 1)
            prot_by_table[tbl].append(col)
        not_deletable.append(
            f"    Date hierarchy columns ({len(protected_cols)} columns in "
            f"{len(prot_by_table)} tables): PBI Desktop requires these for date "
            f"drill-down (Year/Quarter/Month/Day + sortByColumn targets MonthNo/QuarterNo). "
            f"They support variations in kept date columns."
        )
    # 2. Implicit Value columns in measure-only tables
    src = f5["SourceColumn"].fillna("").astype(str).str.strip()
    value_rows = f5[(f5["ColumnName"].astype(str).str.strip() == "Value") & (src == "[Value]")]
    if not value_rows.empty:
        tbl_names = sorted(value_rows["TableName"].unique())
        not_deletable.append(
            f"    Value columns ({len(value_rows)} in measure-only tables): "
            f"Auto-generated by PBI from 'partition source = {{0}}'. "
            f"Removed from TMDL but PBI regenerates them at runtime. "
            f"Tables: {', '.join(tbl_names)}"
        )

    if not_deletable:
        print(f"\n  Not deletable (structural PBI requirements):")
        for line in not_deletable:
            print(line)

    print(f"{'=' * 60}")

    return all_removed, all_skipped


# ============================================================
# Cleanup report export
# ============================================================

def export_cleanup_report(
    removed: List[Dict],
    skipped: List[Dict],
    output_path: str,
):
    """Write cleanup summary to Excel with Removed and Skipped sheets."""
    removed_rows = [
        {
            "TableName": r["table"],
            "ItemName": r["name"],
            "ItemType": r.get("item_type", ""),
            "TMDL_File": r["table"] + ".tmdl",
        }
        for r in removed
    ]
    skipped_rows = [
        {
            "TableName": s["table"],
            "ItemName": s["name"],
            "ItemType": s.get("item_type", ""),
            "Reason": s.get("reason", ""),
        }
        for s in skipped
    ]

    removed_df = pd.DataFrame(
        removed_rows,
        columns=["TableName", "ItemName", "ItemType", "TMDL_File"],
    ) if removed_rows else pd.DataFrame(columns=["TableName", "ItemName", "ItemType", "TMDL_File"])

    skipped_df = pd.DataFrame(
        skipped_rows,
        columns=["TableName", "ItemName", "ItemType", "Reason"],
    ) if skipped_rows else pd.DataFrame(columns=["TableName", "ItemName", "ItemType", "Reason"])

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        removed_df.to_excel(writer, sheet_name="Removed", index=False)
        skipped_df.to_excel(writer, sheet_name="Skipped", index=False)

    print(f"Cleanup report saved to: {output_path}")


# ============================================================
# Standalone execution
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PBI AutoGov — TMDL Cleanup")
    parser.add_argument(
        "--function5", required=True,
        help="Path to Function5_Output.xlsx",
    )
    parser.add_argument(
        "--tables-dir", required=True,
        help="Path to semantic model tables/ directory containing .tmdl files",
    )
    parser.add_argument(
        "--mode", choices=["tmdl_only", "all"], default="tmdl_only",
        help="tmdl_only: measures + calculated columns; all: everything flagged (default: tmdl_only)",
    )
    parser.add_argument(
        "--output", default="TMDL_CLEANUP_REPORT.xlsx",
        help="Output cleanup report path (default: TMDL_CLEANUP_REPORT.xlsx)",
    )
    args = parser.parse_args()

    removed, skipped = run_tmdl_cleanup(args.function5, args.tables_dir, args.mode)
    export_cleanup_report(removed, skipped, args.output)
