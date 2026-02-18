# -*- coding: utf-8 -*-
"""
Skill 3: detect_security.py
PBI AutoGov — Power BI Data Governance Automation Pipeline

Scans RLS (Row-Level Security) role definitions in the semantic model
to automatically identify security tables. These tables must be protected
from removal by the optimization pipeline.

Input:  Semantic model root path (looks for definition/roles/*.tmdl)
Output: Security_Tables_Detected.xlsx (single column: TableName)
"""

import argparse
import re
from pathlib import Path

import pandas as pd


def detect_security_tables(semantic_model_dir: str) -> set:
    """Scan RLS role definitions to find security tables.

    Args:
        semantic_model_dir: Path to the semantic model root
            (e.g. "Revenue Opportunities.SemanticModel")

    Returns:
        Set of table names that have RLS tablePermission rules.
        Returns empty set if no roles folder exists.
    """
    roles_dir = Path(semantic_model_dir) / "definition" / "roles"
    security_tables = set()

    if not roles_dir.is_dir():
        print("No roles folder found: no RLS security tables detected.")
        return security_tables

    for tmdl_file in roles_dir.glob("*.tmdl"):
        content = tmdl_file.read_text(encoding="utf-8-sig")
        # Regex finds tablePermission 'TableName' = ... patterns
        matches = re.findall(r"tablePermission\s+'?([^'=\n]+?)'?\s*=", content)
        for table_name in matches:
            security_tables.add(table_name.strip())

    print(f"Detected {len(security_tables)} security table(s): {security_tables}")
    return security_tables


def export_security_tables(security_tables: set, output_path: str):
    """Export detected security tables to Excel."""
    df = pd.DataFrame({"TableName": sorted(security_tables)})
    df.to_excel(output_path, index=False)
    print(f"Exported {len(df)} security tables to {output_path}")


# ============================================================
# Standalone execution
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PBI AutoGov — Security Table Detector")
    parser.add_argument("--model-root", required=True,
                        help="Path to semantic model root (e.g. 'Revenue Opportunities.SemanticModel')")
    parser.add_argument("--output", default="Security_Tables_Detected.xlsx",
                        help="Output Excel file path")
    args = parser.parse_args()

    security_tables = detect_security_tables(args.model_root)
    export_security_tables(security_tables, args.output)
