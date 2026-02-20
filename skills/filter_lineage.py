# -*- coding: utf-8 -*-
"""
Skill 6: filter_lineage.py
PBI AutoGov — Power BI Data Governance Automation Pipeline

Analyzes filter propagation through the semantic model's relationship graph.
Builds a directed graph from relationships (respecting filter direction and
active/inactive status) and computes transitive filter sources for every table
and measure via BFS.

Input:  Gold_Layer_Tables_Columns.xlsx (catalog from Skill 2)
Output: Filter_Lineage.xlsx (2 sheets: Table_Lineage, Measure_Lineage)
"""

import argparse
from collections import deque
from pathlib import Path

import pandas as pd


# ============================================================
# Step 1: Build directed filter graph from relationships
# ============================================================

def build_filter_graph(tables_df: pd.DataFrame, relations_df: pd.DataFrame) -> dict:
    """Build a directed adjacency list representing filter flow.

    Power BI filter semantics (TMDL):
      - fromColumn = many-side (FK), toColumn = one-side (PK)
      - Default (oneDirection): filter flows toTable → fromTable
        (dimension filters fact)
      - bothDirections: filter flows both ways
      - Inactive relationships are excluded (require USERELATIONSHIP in DAX)

    Returns:
        dict mapping table_name -> set of table_names that this table
        can directly filter.
    """
    # Build ID-to-name lookup
    id_to_name = dict(zip(tables_df["ID"], tables_df["Name"]))
    all_tables = set(tables_df["Name"])

    graph = {t: set() for t in all_tables}

    for _, rel in relations_df.iterrows():
        # Skip inactive relationships
        # Backward-compatible: if IsActive column missing, treat as active
        is_active = rel.get("IsActive", True)
        if isinstance(is_active, str):
            is_active = is_active.lower() != "false"
        if not is_active:
            continue

        from_table = id_to_name.get(rel["FromTableID"])
        to_table = id_to_name.get(rel["ToTableID"])

        if not from_table or not to_table:
            continue

        # Determine filter direction
        # Backward-compatible: if column missing, default to oneDirection
        cross_filter = rel.get("CrossFilteringBehavior", "oneDirection")
        if pd.isna(cross_filter):
            cross_filter = "oneDirection"

        # Default: toTable (PK/dimension) filters fromTable (FK/fact)
        # Edge means "to_table can filter from_table"
        graph[to_table].add(from_table)

        if cross_filter == "bothDirections":
            graph[from_table].add(to_table)

    return graph


# ============================================================
# Step 2: BFS transitive closure — find all filter sources
# ============================================================

def compute_transitive_closure(graph: dict, all_tables: list) -> list:
    """BFS from each table to find all tables that can filter it, with hop count.

    For each target table, we do a reverse traversal: find all tables whose
    filter can *reach* the target through the directed graph.

    Returns:
        List of dicts: {Table, Filtered_By_Table, Hops}
        Tables with no filter sources get a single row with (none) / -1.
    """
    # Build reverse graph: for each table, who can be reached from it?
    # We need: for target T, which tables S have a directed path S→...→T?
    # Equivalently, in the forward graph (A filters B), we want all A that
    # can reach T via chains of "filters" edges.
    # So: reverse BFS from T in the *reverse* of the filter graph.

    # Actually, the graph maps A -> {B} meaning "A can filter B".
    # To find all tables that can filter T, we need all A where A ->* T.
    # That means: BFS from T in the REVERSE graph.

    # Build reverse graph
    reverse_graph = {t: set() for t in all_tables}
    for source, targets in graph.items():
        for target in targets:
            reverse_graph[target].add(source)

    rows = []

    for table in all_tables:
        # BFS in reverse graph from this table
        visited = {}  # table_name -> hops
        queue = deque()

        # Seed: direct filter sources (hop 1)
        for neighbor in reverse_graph.get(table, set()):
            if neighbor != table:  # exclude self-filtering
                visited[neighbor] = 1
                queue.append((neighbor, 1))

        while queue:
            current, hops = queue.popleft()
            for neighbor in reverse_graph.get(current, set()):
                if neighbor != table and neighbor not in visited:
                    visited[neighbor] = hops + 1
                    queue.append((neighbor, hops + 1))

        if visited:
            for source_table, hops in sorted(visited.items(), key=lambda x: (x[1], x[0])):
                rows.append({
                    "Table": table,
                    "Filtered_By_Table": source_table,
                    "Hops": hops,
                })
        else:
            rows.append({
                "Table": table,
                "Filtered_By_Table": "(none)",
                "Hops": -1,
            })

    return rows


# ============================================================
# Step 3: Measure lineage — measures inherit home table's sources
# ============================================================

def compute_measure_lineage(
    table_lineage: list,
    columns_df: pd.DataFrame,
    tables_df: pd.DataFrame,
) -> list:
    """Each measure inherits the filter sources of its home table.

    Returns:
        List of dicts: {MeasureName, HomeTable, Filtered_By_Table, Hops}
    """
    # Build table ID -> name lookup
    id_to_name = dict(zip(tables_df["ID"], tables_df["Name"]))

    # Build table_name -> lineage lookup from the flat list
    table_sources = {}
    for row in table_lineage:
        table = row["Table"]
        if table not in table_sources:
            table_sources[table] = []
        table_sources[table].append((row["Filtered_By_Table"], row["Hops"]))

    # Find all measures (SourceColumn == '[Measure]')
    measures = columns_df[columns_df["SourceColumn"] == "[Measure]"]

    rows = []
    for _, meas in measures.iterrows():
        meas_name = meas["ExplicitName"]
        home_table = id_to_name.get(meas["TableID"], "")

        sources = table_sources.get(home_table, [("(none)", -1)])
        for source_table, hops in sources:
            rows.append({
                "MeasureName": meas_name,
                "HomeTable": home_table,
                "Filtered_By_Table": source_table,
                "Hops": hops,
            })

    return rows


# ============================================================
# Main entry point
# ============================================================

def compute_filter_lineage(catalog_file: str) -> tuple:
    """Load catalog and compute full filter lineage.

    Args:
        catalog_file: Path to Gold_Layer_Tables_Columns.xlsx

    Returns:
        (table_lineage_df, measure_lineage_df)
    """
    print("=" * 60)
    print("PBI AutoGov — Filter Lineage Analysis")
    print("=" * 60)

    # Load catalog sheets
    print(f"\n[1] Loading catalog: {catalog_file}")
    tables_df = pd.read_excel(catalog_file, sheet_name="Tables")
    columns_df = pd.read_excel(catalog_file, sheet_name="Columns")
    relations_df = pd.read_excel(catalog_file, sheet_name="Relations")

    print(f"    Tables: {len(tables_df)}")
    print(f"    Columns: {len(columns_df)}")
    print(f"    Relationships: {len(relations_df)}")

    # Build filter graph
    print("\n[2] Building filter graph")
    graph = build_filter_graph(tables_df, relations_df)

    active_edges = sum(len(targets) for targets in graph.values())
    print(f"    Active filter edges: {active_edges}")

    # Compute transitive closure
    print("\n[3] Computing transitive filter closure (BFS)")
    all_tables = list(tables_df["Name"])
    table_lineage = compute_transitive_closure(graph, all_tables)
    table_lineage_df = pd.DataFrame(table_lineage, columns=["Table", "Filtered_By_Table", "Hops"])

    reachable = table_lineage_df[table_lineage_df["Hops"] > 0]
    isolated = table_lineage_df[table_lineage_df["Hops"] == -1]
    print(f"    Table lineage pairs: {len(reachable)}")
    print(f"    Isolated tables (no filter sources): {len(isolated)}")

    # Compute measure lineage
    print("\n[4] Computing measure lineage")
    measure_lineage = compute_measure_lineage(table_lineage, columns_df, tables_df)
    measure_lineage_df = pd.DataFrame(
        measure_lineage,
        columns=["MeasureName", "HomeTable", "Filtered_By_Table", "Hops"],
    )

    num_measures = len(columns_df[columns_df["SourceColumn"] == "[Measure]"])
    print(f"    Measures: {num_measures}")
    print(f"    Measure lineage pairs: {len(measure_lineage_df[measure_lineage_df['Hops'] > 0])}")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Table lineage pairs: {len(reachable)}")
    print(f"Isolated tables: {len(isolated)}")
    print(f"Measure lineage pairs: {len(measure_lineage_df[measure_lineage_df['Hops'] > 0])}")
    print(f"{'=' * 60}")

    return table_lineage_df, measure_lineage_df


def export_filter_lineage(
    table_lineage_df: pd.DataFrame,
    measure_lineage_df: pd.DataFrame,
    output_path: str,
):
    """Write filter lineage to Excel with 2 sheets."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        table_lineage_df.to_excel(writer, sheet_name="Table_Lineage", index=False)
        measure_lineage_df.to_excel(writer, sheet_name="Measure_Lineage", index=False)

        for sheet_name, df in [
            ("Table_Lineage", table_lineage_df),
            ("Measure_Lineage", measure_lineage_df),
        ]:
            ws = writer.sheets[sheet_name]
            for col_idx, col_name in enumerate(df.columns, 1):
                max_len = max(
                    len(str(col_name)),
                    df[col_name].astype(str).str.len().max() if len(df) > 0 else 0,
                )
                ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max_len + 2, 50)

    print(f"\nFilter lineage saved to: {output_path}")


# ============================================================
# Standalone execution
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PBI AutoGov — Filter Lineage Analysis")
    parser.add_argument("--catalog", required=True, help="Path to Gold_Layer_Tables_Columns.xlsx")
    parser.add_argument("--output", default="Filter_Lineage.xlsx", help="Output Excel file path")
    args = parser.parse_args()

    table_df, measure_df = compute_filter_lineage(args.catalog)
    export_filter_lineage(table_df, measure_df, args.output)
