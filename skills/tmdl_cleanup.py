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
    - Never touches partition, hierarchy, annotation blocks or the table declaration

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
        (removed_count, removed_items, skipped_items)
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
        return 0, removed, skipped

    # Create backup before editing
    backup_path = tmdl_path.with_suffix(".tmdl.bak")
    shutil.copy2(tmdl_path, backup_path)
    print(f"  Backup: {backup_path.name}")

    # Remove blocks bottom-to-top to preserve earlier indices
    blocks.sort(key=lambda x: x[0], reverse=True)
    for start, end in blocks:
        del lines[start:end]

    tmdl_path.write_text("\n".join(lines), encoding="utf-8-sig")

    return len(removed), removed, skipped


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

    print(f"  Items to remove: {len(to_remove)}")

    all_removed = []
    all_skipped = []

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

        count, removed, skipped = remove_blocks_from_tmdl(tmdl_file, items)
        all_removed.extend(removed)
        all_skipped.extend(skipped)

        if count > 0:
            print(f"  {tmdl_file.name}: removed {count} item(s)")

    # Summary
    measure_count = sum(1 for r in all_removed if r.get("item_type") == "Measure")
    calc_count = sum(1 for r in all_removed if r.get("item_type") == "Calculated Column")
    imported_count = sum(1 for r in all_removed if r.get("item_type") == "Imported Column")

    print(f"\n{'=' * 60}")
    print(f"TMDL Cleanup complete!")
    print(f"  Removed: {len(all_removed)} total")
    if measure_count:
        print(f"    Measures: {measure_count}")
    if calc_count:
        print(f"    Calculated columns: {calc_count}")
    if imported_count:
        print(f"    Imported columns: {imported_count}")
    if all_skipped:
        print(f"  Skipped: {len(all_skipped)} (see report for details)")
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
