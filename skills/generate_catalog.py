# -*- coding: utf-8 -*-
"""
Skill 2: generate_catalog.py
PBI AutoGov — Power BI Data Governance Automation Pipeline

Parses TMDL files to build a complete inventory of the semantic model:
all tables, columns (including measures and calculated columns), and relationships.
Outputs a 3-sheet Excel catalog.

Input:  Semantic model definition root (tables/, relationships.tmdl)
Output: Gold_Layer_Tables_Columns.xlsx (3 sheets: Tables, Columns, Relations)
"""

import argparse
import re
from pathlib import Path

import pandas as pd


# ============================================================
# Step 1: Parse tables and columns from TMDL files
# ============================================================

def parse_tables_and_columns(tables_dir: Path) -> tuple:
    """Extract all tables and columns from TMDL files.
    Auto-generates numeric IDs for compatibility with the pipeline.

    Returns:
        (tables_df, columns_df, table_name_to_id, col_lookup)
        - table_name_to_id: dict mapping table name -> generated ID
        - col_lookup: dict mapping (table_name, col_name) -> column ID
    """
    tables = []
    columns = []

    table_id_counter = 1
    column_id_counter = 1

    table_name_to_id = {}
    col_lookup = {}

    if not tables_dir.is_dir():
        print(f"WARNING: Tables directory not found: {tables_dir}")
        return pd.DataFrame(), pd.DataFrame(), table_name_to_id, col_lookup

    for tmdl_file in sorted(tables_dir.glob("**/*.tmdl")):
        content = tmdl_file.read_text(encoding="utf-8-sig")

        # Extract table name
        table_match = re.match(r"^table\s+(.+?)$", content, re.MULTILINE)
        if not table_match:
            continue

        table_name = table_match.group(1).strip().strip("'")
        table_id = table_id_counter
        table_id_counter += 1

        tables.append({"ID": table_id, "Name": table_name})
        table_name_to_id[table_name] = table_id

        # Extract columns (both regular and calculated: column name = DAX)
        col_pattern = re.compile(
            r"^\tcolumn\s+'?([^'=\n]+?)'?\s*(?:=.*)?$",
            re.MULTILINE,
        )

        for col_match in col_pattern.finditer(content):
            col_name = col_match.group(1).strip().strip("'")
            col_id = column_id_counter
            column_id_counter += 1

            # Try to find sourceColumn within this column's definition block
            col_start = col_match.end()
            next_block = re.search(
                r"^\t(?:column|measure|hierarchy|partition|annotation)\s",
                content[col_start:], re.MULTILINE,
            )
            block_end = col_start + next_block.start() if next_block else len(content)
            col_block = content[col_start:block_end]

            source_col_match = re.search(
                r"^\t\tsourceColumn:\s*(.+?)$", col_block, re.MULTILINE,
            )
            source_column = source_col_match.group(1).strip() if source_col_match else ""

            columns.append({
                "ID": col_id,
                "ExplicitName": col_name,
                "SourceColumn": source_column,
                "TableID": table_id,
            })
            col_lookup[(table_name, col_name)] = col_id

        # Extract measures (included in Columns sheet with SourceColumn = '[Measure]')
        measure_pattern = re.compile(
            r"^\tmeasure\s+'?([^'=\n]+?)'?\s*=",
            re.MULTILINE,
        )

        for meas_match in measure_pattern.finditer(content):
            meas_name = meas_match.group(1).strip().strip("'")
            col_id = column_id_counter
            column_id_counter += 1

            columns.append({
                "ID": col_id,
                "ExplicitName": meas_name,
                "SourceColumn": "[Measure]",
                "TableID": table_id,
            })
            col_lookup[(table_name, meas_name)] = col_id

    tables_df = pd.DataFrame(tables, columns=["ID", "Name"])
    columns_df = pd.DataFrame(columns, columns=["ID", "ExplicitName", "SourceColumn", "TableID"])

    return tables_df, columns_df, table_name_to_id, col_lookup


# ============================================================
# Step 2: Parse relationships
# ============================================================

def parse_relationships(rel_file: Path, table_name_to_id: dict, col_lookup: dict) -> pd.DataFrame:
    """Parse relationships.tmdl and resolve table/column names to IDs.

    Args:
        rel_file: Path to relationships.tmdl
        table_name_to_id: dict from parse_tables_and_columns
        col_lookup: dict from parse_tables_and_columns
    """
    relationships = []

    if not rel_file.is_file():
        print(f"WARNING: Relationships file not found: {rel_file}")
        return pd.DataFrame()

    content = rel_file.read_text(encoding="utf-8-sig")

    # Split into individual relationship blocks
    rel_blocks = re.split(r"^relationship\s+", content, flags=re.MULTILINE)

    for block in rel_blocks:
        if not block.strip():
            continue

        # Extract fromColumn and toColumn
        # Supported formats: TableName.ColumnName, 'TableName'.'ColumnName', [TableName].[ColumnName]
        # Bare names can contain word chars, spaces, and hyphens (e.g. LocalDateTable_ad3f-...)
        _rel_col_pattern = r"(?:'([^']+)'|\[([^\]]+)\]|([A-Za-z_][\w\s\-]*?))\s*\.\s*(?:'([^']+)'|\[([^\]]+)\]|([A-Za-z_][\w\s\-]*?))\s*$"
        from_match = re.search(r"fromColumn:\s*" + _rel_col_pattern, block, re.MULTILINE)
        to_match = re.search(r"toColumn:\s*" + _rel_col_pattern, block, re.MULTILINE)

        if not from_match or not to_match:
            continue

        # Groups: (1=quoted table, 2=bracketed table, 3=bare table,
        #          4=quoted col, 5=bracketed col, 6=bare col)
        from_table = (from_match.group(1) or from_match.group(2) or from_match.group(3) or "").strip()
        from_col = (from_match.group(4) or from_match.group(5) or from_match.group(6) or "").strip()
        to_table = (to_match.group(1) or to_match.group(2) or to_match.group(3) or "").strip()
        to_col = (to_match.group(4) or to_match.group(5) or to_match.group(6) or "").strip()

        # Resolve names to IDs
        from_table_id = table_name_to_id.get(from_table)
        to_table_id = table_name_to_id.get(to_table)
        from_col_id = col_lookup.get((from_table, from_col))
        to_col_id = col_lookup.get((to_table, to_col))

        if not all([from_table_id, to_table_id, from_col_id, to_col_id]):
            missing = []
            if not from_table_id:
                missing.append(f"FromTable '{from_table}'")
            if not to_table_id:
                missing.append(f"ToTable '{to_table}'")
            if not from_col_id:
                missing.append(f"FromCol '{from_table}.{from_col}'")
            if not to_col_id:
                missing.append(f"ToCol '{to_table}.{to_col}'")
            print(f"  WARNING: Unresolved relationship: {', '.join(missing)}")
            continue

        # Extract crossFilteringBehavior (default: oneDirection)
        cross_filter_match = re.search(
            r"crossFilteringBehavior:\s*(\S+)", block,
        )
        cross_filter = cross_filter_match.group(1) if cross_filter_match else "oneDirection"

        # Extract isActive flag (default: True — omitted means active)
        is_active_match = re.search(r"isActive:\s*(\S+)", block)
        is_active = is_active_match.group(1).lower() != "false" if is_active_match else True

        relationships.append({
            "FromTableID": from_table_id,
            "FromColumnID": from_col_id,
            "ToTableID": to_table_id,
            "ToColumnID": to_col_id,
            "CrossFilteringBehavior": cross_filter,
            "IsActive": is_active,
        })

    return pd.DataFrame(relationships, columns=[
        "FromTableID", "FromColumnID", "ToTableID", "ToColumnID",
        "CrossFilteringBehavior", "IsActive",
    ])


# ============================================================
# Main catalog generation
# ============================================================

def generate_catalog(model_root: str) -> tuple:
    """Generate the full semantic model catalog.

    Args:
        model_root: Path to semantic model definition root

    Returns:
        (tables_df, columns_df, relations_df)
    """
    tables_dir = Path(model_root) / "tables"
    relationships_file = Path(model_root) / "relationships.tmdl"

    print("=" * 60)
    print("PBI AutoGov — Semantic Model Catalog Generator")
    print("=" * 60)

    # [1] Parse tables and columns
    print(f"\n[1] Parsing tables and columns: {tables_dir}")
    tables_df, columns_df, table_name_to_id, col_lookup = parse_tables_and_columns(tables_dir)
    print(f"    Tables: {len(tables_df)}")
    print(f"    Columns: {len(columns_df)}")

    if not tables_df.empty:
        print("    Sample tables:")
        for _, row in tables_df.head(5).iterrows():
            print(f"      ID={row['ID']}, Name={row['Name']}")

    # [2] Parse relationships
    print(f"\n[2] Parsing relationships: {relationships_file}")
    relations_df = parse_relationships(relationships_file, table_name_to_id, col_lookup)
    print(f"    Relationships: {len(relations_df)}")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Tables: {len(tables_df)}")
    print(f"Columns: {len(columns_df)}")
    print(f"Relationships: {len(relations_df)}")
    print(f"{'=' * 60}")

    return tables_df, columns_df, relations_df


def export_catalog(tables_df, columns_df, relations_df, output_path: str):
    """Export the catalog to Excel with 3 sheets."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        tables_df.to_excel(writer, sheet_name="Tables", index=False)
        columns_df.to_excel(writer, sheet_name="Columns", index=False)
        relations_df.to_excel(writer, sheet_name="Relations", index=False)

        for sheet_name in ["Tables", "Columns", "Relations"]:
            ws = writer.sheets[sheet_name]
            df = {"Tables": tables_df, "Columns": columns_df, "Relations": relations_df}[sheet_name]
            for col_idx, col_name in enumerate(df.columns, 1):
                max_len = max(
                    len(str(col_name)),
                    df[col_name].astype(str).str.len().max() if len(df) > 0 else 0,
                )
                ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max_len + 2, 50)

    print(f"\nCatalog saved to: {output_path}")


# ============================================================
# Standalone execution
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PBI AutoGov — Catalog Generator")
    parser.add_argument("--model-root", required=True, help="Path to semantic model definition root")
    parser.add_argument("--output", default="Gold_Layer_Tables_Columns.xlsx", help="Output Excel file path")
    args = parser.parse_args()

    tables_df, columns_df, relations_df = generate_catalog(args.model_root)
    if not tables_df.empty:
        export_catalog(tables_df, columns_df, relations_df, args.output)
    else:
        print("No data extracted. Check paths.")
