# -*- coding: utf-8 -*-
"""
Skill 4: optimization_pipeline.py
PBI AutoGov — Power BI Data Governance Automation Pipeline

The core optimization engine. Takes metadata from Skill 1, catalog from Skill 2,
security tables from Skill 3, and a manual Views file, then runs a 6-function
pipeline to identify unused columns/tables and generate DROP SQL.

Pipeline:
    F1: report_field_usage — aggregates usage counts per Table$$Column
    F2: relationship_columns_resolver — maps relationship IDs to names
    F3: flag_columns_used_in_pbi — flags Used_in_PBI
    F4: flag_columns_used_in_relationships — flags Used_in_Relationship
    F5: flag_columns_to_remove — merges F3+F4, applies protections
    F6: flag_tables_to_remove — flags whole tables for removal
    + DROP TABLE and DROP COLUMN SQL generation

Input:  pbi_report_metadata.xlsx (Skill 1), Gold_Layer_Tables_Columns.xlsx (Skill 2),
        Security_Tables_Detected.xlsx (Skill 3), Views_Security.xlsx (manual)
Output: Function1-6 intermediate Excel files + DROP_TABLES.sql + DROP_COLUMNS.sql + MODEL_CLEANUP.xlsx
"""

import argparse
import re
from pathlib import Path
from typing import Optional, Tuple, Set

import pandas as pd


# ============================================================
# Helper functions
# ============================================================

def _clean_text(x) -> str:
    """Normalize text: replace newlines, collapse whitespace, strip."""
    if pd.isna(x):
        return ""
    x = str(x).replace("\n", " ").strip()
    x = re.sub(r"\s+", " ", x)
    return x


def normalize_key(s: pd.Series) -> pd.Series:
    """Normalize Key_Column values for matching: lowercase, strip, collapse whitespace."""
    s = (
        s.astype(str)
         .str.replace("\n", " ", regex=False)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
         .str.lower()
    )
    # Remove trailing question marks (edge case from some TMDL files)
    s = s.str.replace(r"\s*\?\s*$", "", regex=True)
    return s


def build_semantic_master_from_gold(gold_model_file: str) -> pd.DataFrame:
    """Build the semantic column universe from the Gold metadata catalog."""
    tables_df = pd.read_excel(gold_model_file, sheet_name="Tables")
    columns_df = pd.read_excel(gold_model_file, sheet_name="Columns")

    tables_lookup = tables_df[["ID", "Name"]].rename(columns={"ID": "TableID", "Name": "TableName"})
    cols_core = columns_df[["ID", "ExplicitName", "SourceColumn", "TableID"]].rename(
        columns={"ID": "ColumnID", "ExplicitName": "ColumnName"},
    )

    semantic_master = cols_core.merge(tables_lookup, on="TableID", how="left")

    if semantic_master["TableName"].isna().any():
        bad = semantic_master[semantic_master["TableName"].isna()][["ColumnID", "ColumnName", "TableID"]].head(25)
        raise ValueError("Some columns could not be mapped to TableName. Examples:\n" + bad.to_string(index=False))

    semantic_master["Key_Column"] = (
        semantic_master["TableName"].astype(str).str.strip()
        + "$$"
        + semantic_master["ColumnName"].astype(str).str.strip()
    )
    semantic_master["Key_Column_Normalized"] = normalize_key(semantic_master["Key_Column"])
    return semantic_master


# ============================================================
# Function 1: Report field usage
# ============================================================

def function1_report_field_usage(
    input_xlsx_path: str,
    output_xlsx_path: Optional[str] = None,
) -> pd.DataFrame:
    """Aggregate usage counts per Table/Column and build Key_Column."""
    df = pd.read_excel(input_xlsx_path)

    df["Table in the Semantic Model"] = df["Table in the Semantic Model"].apply(_clean_text)
    df["Column in the Semantic Model"] = df["Column in the Semantic Model"].apply(_clean_text)
    df["Usage (Visual/Filter/Slicer)"] = df["Usage (Visual/Filter/Slicer)"].apply(_clean_text)

    # Split comma-separated columns into individual rows
    df["__col_list"] = df["Column in the Semantic Model"].str.split(",")
    df = df.explode("__col_list")
    df["Column in the Semantic Model"] = df["__col_list"].apply(_clean_text)
    df = df.drop(columns="__col_list")
    df = df[df["Column in the Semantic Model"] != ""]

    # Duplicate measure rows with UI Field Name as the column name.
    # This ensures the measure NAME itself is tracked as a used key,
    # not just the underlying columns the measure references.
    if "Measure Formula" in df.columns and "UI Field Name" in df.columns:
        measure_rows = df[df["Measure Formula"].notna() & (df["Measure Formula"].astype(str).str.strip() != "")].copy()
        if len(measure_rows) > 0:
            measure_rows["Column in the Semantic Model"] = measure_rows["UI Field Name"].apply(_clean_text)
            df = pd.concat([df, measure_rows], ignore_index=True)
            df = df[df["Column in the Semantic Model"] != ""]

    # Classify usage types
    usage = df["Usage (Visual/Filter/Slicer)"].str.lower()
    df["Slicer"] = usage.str.contains(r"\bslicer\b", regex=True).astype(int)
    df["Visual"] = usage.str.contains(r"\bvisual\b", regex=True).astype(int)
    df["Filter"] = usage.str.contains(r"\bfilter\b", regex=True).astype(int)

    has_measure_tag = usage.str.contains(r"\(measure\)", regex=True)
    has_measure_formula = df.get("Measure Formula", pd.Series([None] * len(df))).notna()
    df["Measure"] = (has_measure_tag | has_measure_formula).astype(int)

    # Aggregate
    out = (
        df.groupby(["Table in the Semantic Model", "Column in the Semantic Model"], as_index=False)[
            ["Slicer", "Visual", "Filter", "Measure"]
        ]
        .sum()
    )

    out["Frequency Used in Power BI"] = out[["Slicer", "Visual", "Filter", "Measure"]].sum(axis=1)
    out["Key_Column"] = out["Table in the Semantic Model"] + "$$" + out["Column in the Semantic Model"]

    out = out.sort_values(
        ["Frequency Used in Power BI", "Table in the Semantic Model", "Column in the Semantic Model"],
        ascending=[False, True, True],
    )

    if output_xlsx_path:
        out.to_excel(output_xlsx_path, index=False)
        print(f"Function 1 completed. Saved {len(out)} rows to: {output_xlsx_path}")

    return out


# ============================================================
# Function 2: Relationship columns resolver
# ============================================================

def function2_relationship_columns_resolver(
    gold_file: str,
    output_xlsx_path: Optional[str] = None,
) -> pd.DataFrame:
    """Resolve relationship endpoints from IDs to Table/Column names."""
    tables_df = pd.read_excel(gold_file, sheet_name="Tables")
    columns_df = pd.read_excel(gold_file, sheet_name="Columns")
    relations_df = pd.read_excel(gold_file, sheet_name="Relations")

    table_name_by_id = dict(zip(tables_df["ID"], tables_df["Name"]))
    column_name_by_id = dict(zip(columns_df["ID"], columns_df["ExplicitName"]))

    output_df = pd.DataFrame({
        "FromTableName": relations_df["FromTableID"].map(table_name_by_id),
        "FromColumnName": relations_df["FromColumnID"].map(column_name_by_id),
        "ToTableName": relations_df["ToTableID"].map(table_name_by_id),
        "ToColumnName": relations_df["ToColumnID"].map(column_name_by_id),
    })

    if output_df.isna().any(axis=1).any():
        raise ValueError("Some relationship IDs could not be resolved (missing TableID/ColumnID mapping).")

    if output_xlsx_path:
        output_df.to_excel(output_xlsx_path, index=False)
        print(f"Function 2 completed. Output saved to: {output_xlsx_path}")

    return output_df


# ============================================================
# Function 3: Flag columns used in PBI
# ============================================================

def function3_flag_columns_used_in_pbi(
    gold_model_file: str,
    function1_out_df: pd.DataFrame,
    output_xlsx_path: Optional[str] = None,
    flag_as_int: bool = True,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Flag semantic columns used in Power BI based on Function 1 output."""
    semantic_master = build_semantic_master_from_gold(gold_model_file)

    if "Key_Column" not in function1_out_df.columns:
        raise ValueError(f"Function 1 output must contain Key_Column. Found: {list(function1_out_df.columns)}")

    used_df = function1_out_df.copy()
    used_df["Key_Column_Normalized"] = normalize_key(used_df["Key_Column"])
    used_keys_norm = set(used_df["Key_Column_Normalized"].dropna().unique())

    semantic_master["Used_in_PBI"] = semantic_master["Key_Column_Normalized"].isin(used_keys_norm)

    # Secondary pass: match measures by name only (PBI measure names are globally unique).
    # Visuals may attribute a measure to a different table than its home table,
    # creating keys like Owners$$Goal when the measure is defined in Opportunities.
    # Name-only matching catches these cross-table references safely for measures.
    if "SourceColumn" in semantic_master.columns:
        is_measure = semantic_master["SourceColumn"].fillna("").astype(str).str.strip().eq("[Measure]")
        not_yet_matched = ~semantic_master["Used_in_PBI"]
        # Build a set of just column/measure names from F1 (the part after $$)
        f1_col_names_norm = set()
        for k in used_keys_norm:
            parts = k.split("$$", 1)
            if len(parts) == 2:
                f1_col_names_norm.add(parts[1].strip())
        measure_name_norm = semantic_master["ColumnName"].astype(str).str.strip().str.lower()
        name_matched = measure_name_norm.isin(f1_col_names_norm)
        semantic_master.loc[is_measure & not_yet_matched & name_matched, "Used_in_PBI"] = True

    if flag_as_int:
        semantic_master["Used_in_PBI"] = semantic_master["Used_in_PBI"].astype(int)
    else:
        semantic_master["Used_in_PBI"] = semantic_master["Used_in_PBI"].map({True: "Yes", False: "No"})

    # Audit: classify unmatched F1 keys into true mismatches vs display aliases.
    # True mismatches: the column name exists in the catalog under a different table.
    # Display aliases: the column name doesn't exist anywhere in the catalog — these
    # are visual-level renamed measures or ad-hoc calculated fields (harmless).
    model_keys_norm = set(semantic_master["Key_Column_Normalized"].unique())

    # Build lookup sets for classification
    measure_names_in_catalog = set()
    all_col_names_in_catalog = set()
    if "SourceColumn" in semantic_master.columns:
        measures = semantic_master[semantic_master["SourceColumn"].fillna("").astype(str).str.strip().eq("[Measure]")]
        measure_names_in_catalog = set(measures["ColumnName"].astype(str).str.strip().str.lower())
    all_col_names_in_catalog = set(semantic_master["ColumnName"].astype(str).str.strip().str.lower())

    true_mismatches = []
    display_aliases = []

    for _, row in used_df[["Key_Column", "Key_Column_Normalized"]].drop_duplicates().iterrows():
        knorm = row["Key_Column_Normalized"]
        kraw = row["Key_Column"]

        # Skip if fully matched by key or by measure name
        if knorm in model_keys_norm:
            continue
        parts = knorm.split("$$", 1)
        if len(parts) == 2 and parts[1].strip() in measure_names_in_catalog:
            continue

        # Unmatched — classify it
        col_name = parts[1].strip() if len(parts) == 2 else knorm
        if col_name in all_col_names_in_catalog:
            # Column name exists under a different table — true mismatch
            true_mismatches.append(kraw)
        else:
            # Column name doesn't exist anywhere — display alias (harmless)
            display_aliases.append(kraw)

    unmatched_used = pd.DataFrame({"Key_Column": true_mismatches}) if true_mismatches else pd.DataFrame(columns=["Key_Column"])

    if output_xlsx_path:
        semantic_master.to_excel(output_xlsx_path, index=False)
        if len(true_mismatches) > 0:
            audit_file = output_xlsx_path.replace(".xlsx", "_UNMATCHED_USED_KEYS.xlsx")
            pd.DataFrame({"Unmatched_Used_Key_Column": true_mismatches}).to_excel(audit_file, index=False)
            print(f"  True unmatched keys: {len(true_mismatches)} (saved to {audit_file})")
        if len(display_aliases) > 0:
            alias_file = output_xlsx_path.replace(".xlsx", "_DISPLAY_ALIASES.xlsx")
            pd.DataFrame({"Display_Alias_Key": display_aliases}).to_excel(alias_file, index=False)
            print(f"  Display aliases (harmless): {len(display_aliases)} (saved to {alias_file})")
        if len(true_mismatches) == 0 and len(display_aliases) == 0:
            print(f"  All F1 keys matched.")
        used_count = int(semantic_master["Used_in_PBI"].sum()) if flag_as_int else int((semantic_master["Used_in_PBI"] == "Yes").sum())
        print(f"Function 3 completed. Used_in_PBI count: {used_count}")

    return semantic_master, (unmatched_used if len(unmatched_used) > 0 else None)


# ============================================================
# Function 4: Flag columns used in relationships
# ============================================================

def function4_flag_columns_used_in_relationships(
    gold_model_file: str,
    function2_out_df: pd.DataFrame,
    output_xlsx_path: Optional[str] = None,
    flag_as_int: bool = True,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Flag semantic columns used in relationships based on Function 2 output."""
    semantic_master = build_semantic_master_from_gold(gold_model_file)

    required_cols = {"FromTableName", "FromColumnName", "ToTableName", "ToColumnName"}
    missing = required_cols - set(function2_out_df.columns)
    if missing:
        raise ValueError(f"Function 2 output missing columns: {missing}")

    rel_df = function2_out_df.copy()

    from_keys = rel_df["FromTableName"].astype(str).str.strip() + "$$" + rel_df["FromColumnName"].astype(str).str.strip()
    to_keys = rel_df["ToTableName"].astype(str).str.strip() + "$$" + rel_df["ToColumnName"].astype(str).str.strip()

    endpoints = pd.concat([from_keys, to_keys], ignore_index=True)
    endpoints_norm = normalize_key(endpoints)
    rel_key_set = set(endpoints_norm.dropna().unique())

    semantic_master["Used_in_Relationship"] = semantic_master["Key_Column_Normalized"].isin(rel_key_set)
    if flag_as_int:
        semantic_master["Used_in_Relationship"] = semantic_master["Used_in_Relationship"].astype(int)
    else:
        semantic_master["Used_in_Relationship"] = semantic_master["Used_in_Relationship"].map({True: "Yes", False: "No"})

    # Audit: find relationship endpoints that don't exist in the semantic model
    model_key_set = set(semantic_master["Key_Column_Normalized"].dropna().unique())
    unmatched_endpoints = endpoints.loc[~endpoints_norm.isin(model_key_set)].drop_duplicates()

    if output_xlsx_path:
        semantic_master.to_excel(output_xlsx_path, index=False)
        audit_file = output_xlsx_path.replace(".xlsx", "_UNMATCHED_REL_ENDPOINTS.xlsx")
        pd.DataFrame({"Unmatched_Relationship_Endpoint_Key_Column": unmatched_endpoints}).to_excel(audit_file, index=False)
        used_count = int(semantic_master["Used_in_Relationship"].sum()) if flag_as_int else int((semantic_master["Used_in_Relationship"] == "Yes").sum())
        print(f"Function 4 completed. Used_in_Relationship count: {used_count}")

    return semantic_master, (unmatched_endpoints if len(unmatched_endpoints) > 0 else None)


# ============================================================
# Function 5: Flag columns to remove
# ============================================================

def function5_flag_columns_to_remove(
    function3_out_df: pd.DataFrame,
    function4_out_df: pd.DataFrame,
    output_xlsx_path: Optional[str] = None,
    protected_view_columns: Optional[Set[str]] = None,
    protected_security_tables: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """Merge F3 + F4 and flag columns for removal.
    Remove = Yes when Used_in_PBI = 0 AND Used_in_Relationship = 0.
    Protected view columns and security tables override removal.
    """
    required_f3 = {"ColumnID", "Used_in_PBI", "TableName", "ColumnName"}
    required_f4 = {"ColumnID", "Used_in_Relationship"}

    if not required_f3.issubset(function3_out_df.columns):
        raise ValueError(f"Function 3 output missing: {required_f3 - set(function3_out_df.columns)}")
    if not required_f4.issubset(function4_out_df.columns):
        raise ValueError(f"Function 4 output missing: {required_f4 - set(function4_out_df.columns)}")

    df = function3_out_df.merge(
        function4_out_df[["ColumnID", "Used_in_Relationship"]],
        on="ColumnID",
        how="left",
    )

    if df["Used_in_Relationship"].isna().any():
        raise ValueError("Merge issue: some columns missing Used_in_Relationship.")

    # Flag for removal: not used in PBI and not in any relationship
    df["Remove_column"] = ((df["Used_in_PBI"] == 0) & (df["Used_in_Relationship"] == 0)).map(
        {True: "Yes", False: "No"},
    )

    # Apply protections
    table_norm = df["TableName"].astype(str).str.strip().str.lower()
    col_norm = df["ColumnName"].astype(str).str.strip().str.lower()
    key_norm = table_norm + "$$" + col_norm

    protected_tables = {
        str(t).strip().lower()
        for t in (protected_security_tables or set())
        if pd.notna(t) and str(t).strip() != ""
    }

    protected_columns = {
        str(k).strip().lower()
        for k in (protected_view_columns or set())
        if pd.notna(k) and str(k).strip() != ""
    }

    df["Protected"] = (
        table_norm.isin(protected_tables) | key_norm.isin(protected_columns)
    ).map({True: "Yes", False: "No"})

    # Protected = Yes overrides Remove = Yes
    df.loc[df["Protected"] == "Yes", "Remove_column"] = "No"

    if output_xlsx_path:
        df.to_excel(output_xlsx_path, index=False)

    return df


# ============================================================
# Function 6: Flag tables to remove
# ============================================================

def function6_flag_tables_to_remove(
    function5_out_df: pd.DataFrame,
    output_xlsx_path: Optional[str] = None,
    protected_security_tables: Optional[Set[str]] = None,
) -> Tuple[pd.DataFrame, Set[str]]:
    """Flag entire tables for removal (only when ALL columns are flagged)."""
    if "TableName" not in function5_out_df.columns or "Remove_column" not in function5_out_df.columns:
        raise ValueError("Function 5 output must contain TableName and Remove_column.")

    tmp = function5_out_df.copy()
    tmp["_remove_col_bool"] = tmp["Remove_column"].astype(str).str.strip().str.lower().eq("yes")

    table_summary = (
        tmp.groupby("TableName")
           .agg(
               Total_Columns=("Remove_column", "count"),
               Columns_To_Remove=("_remove_col_bool", "sum"),
               Remove_table=("_remove_col_bool", "all"),
           )
           .reset_index()
    )

    table_summary["Remove_table"] = table_summary["Remove_table"].map({True: "Yes", False: "No"})

    # Protect security tables
    if protected_security_tables:
        protected_norm = {
            str(t).strip().lower()
            for t in protected_security_tables
            if pd.notna(t) and str(t).strip() != ""
        }
        table_summary["_TableName_norm"] = table_summary["TableName"].astype(str).str.strip().str.lower()
        table_summary.loc[
            table_summary["_TableName_norm"].isin(protected_norm),
            "Remove_table",
        ] = "No"
        table_summary = table_summary.drop(columns="_TableName_norm")

    kept_tables = set(
        table_summary.loc[table_summary["Remove_table"].eq("No"), "TableName"].astype(str),
    )

    if output_xlsx_path:
        table_summary.to_excel(output_xlsx_path, index=False)

    return table_summary, kept_tables


# ============================================================
# SQL generators
# ============================================================

def generate_drop_table_sql(
    function6_df: pd.DataFrame,
    output_sql_file: str = "DROP_TABLES.sql",
    schema: str = "dbo",
    output_xlsx_file: str = "DROP_TABLES.xlsx",
) -> pd.DataFrame:
    """Generate DROP TABLE SQL for tables flagged for removal."""
    df = function6_df.copy()

    if "TableName" not in df.columns or "Remove_table" not in df.columns:
        raise ValueError("Function 6 DF must contain TableName and Remove_table columns.")

    to_drop = df[df["Remove_table"].astype(str).str.strip().str.lower().eq("yes")].copy()
    to_drop["Drop_SQL"] = "DROP TABLE [" + schema + "].[" + to_drop["TableName"].astype(str) + "];"

    with open(output_sql_file, "w", encoding="utf-8") as f:
        for stmt in to_drop["Drop_SQL"]:
            f.write(stmt + "\n")

    to_drop[["TableName", "Drop_SQL"]].to_excel(output_xlsx_file, index=False)

    print("DROP TABLE SQL generated.")
    print(f"  SQL file: {output_sql_file}")
    print(f"  Review file: {output_xlsx_file}")
    print(f"  Tables to drop: {len(to_drop)}")

    return to_drop[["TableName", "Drop_SQL"]]


def generate_drop_column_sql(
    function5_df: pd.DataFrame,
    output_sql_file: str = "DROP_COLUMNS.sql",
    schema: str = "dbo",
    output_xlsx_file: str = "DROP_COLUMNS.xlsx",
    kept_tables: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """Generate DROP COLUMN SQL for columns flagged for removal (in kept tables only)."""
    required = {"TableName", "ColumnName", "Remove_column"}
    if not required.issubset(function5_df.columns):
        raise ValueError(f"Function 5 DF missing columns: {required - set(function5_df.columns)}")

    df = function5_df.copy()

    # Only generate DROP COLUMN for tables that are NOT being dropped entirely
    if kept_tables is not None:
        df["TableName"] = df["TableName"].astype(str).str.strip()
        df = df[df["TableName"].isin(kept_tables)].copy()

    to_drop = df[df["Remove_column"].astype(str).str.strip().str.lower().eq("yes")].copy()

    # Filter to real imported columns only — exclude measures and calculated columns
    # Measures have SourceColumn == "[Measure]", calculated columns have empty SourceColumn
    if "SourceColumn" in to_drop.columns:
        src = to_drop["SourceColumn"].fillna("").astype(str).str.strip()
        to_drop = to_drop[(src != "") & (src != "[Measure]")].copy()

    if to_drop.empty:
        print("No imported columns flagged for removal. No SQL generated.")
        return to_drop[["TableName", "ColumnName", "Remove_column"]]

    # Use SourceColumn as the actual DB column name (PBI may rename columns)
    db_col_name = to_drop["ColumnName"].astype(str)
    if "SourceColumn" in to_drop.columns:
        db_col_name = to_drop["SourceColumn"].astype(str).str.strip()

    to_drop["Drop_SQL"] = (
        "ALTER TABLE [" + schema + "].[" + to_drop["TableName"].astype(str) + "] "
        "DROP COLUMN [" + db_col_name + "];"
    )

    with open(output_sql_file, "w", encoding="utf-8") as f:
        for stmt in to_drop["Drop_SQL"]:
            f.write(stmt + "\n")

    to_drop[["TableName", "ColumnName", "Drop_SQL"]].to_excel(output_xlsx_file, index=False)
    print(f"DROP COLUMN SQL generated. Columns to drop: {len(to_drop)}")

    return to_drop[["TableName", "ColumnName", "Drop_SQL"]]


def generate_model_cleanup_report(
    function5_df: pd.DataFrame,
    output_xlsx_file: str = "MODEL_CLEANUP.xlsx",
    kept_tables: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """Generate a cleanup report for unused measures and calculated columns.

    These items exist only in the Power BI semantic model (TMDL files),
    not in the database, so they cannot be dropped with SQL.

    Measures have SourceColumn == "[Measure]".
    Calculated columns have SourceColumn == "" (empty).
    """
    required = {"TableName", "ColumnName", "Remove_column"}
    if not required.issubset(function5_df.columns):
        raise ValueError(f"Function 5 DF missing columns: {required - set(function5_df.columns)}")

    df = function5_df.copy()

    # Only consider tables that are NOT being dropped entirely
    if kept_tables is not None:
        df["TableName"] = df["TableName"].astype(str).str.strip()
        df = df[df["TableName"].isin(kept_tables)].copy()

    to_clean = df[df["Remove_column"].astype(str).str.strip().str.lower().eq("yes")].copy()

    # Keep only measures and calculated columns (non-imported)
    if "SourceColumn" in to_clean.columns:
        src = to_clean["SourceColumn"].fillna("").astype(str).str.strip()
        to_clean = to_clean[(src == "") | (src == "[Measure]")].copy()
    else:
        # Without SourceColumn info, nothing to report
        to_clean = to_clean.iloc[0:0].copy()

    if to_clean.empty:
        print("No unused measures or calculated columns found.")
        empty_df = pd.DataFrame(columns=["TableName", "ColumnName", "Type", "TMDL_File"])
        empty_df.to_excel(output_xlsx_file, index=False)
        return empty_df

    # Classify type: Measure vs Calculated Column
    src = to_clean["SourceColumn"].fillna("").astype(str).str.strip()
    to_clean["Type"] = src.map(lambda v: "Measure" if v == "[Measure]" else "Calculated Column")
    to_clean["TMDL_File"] = to_clean["TableName"].astype(str).str.strip() + ".tmdl"

    out = to_clean[["TableName", "ColumnName", "Type", "TMDL_File"]].copy()
    out.to_excel(output_xlsx_file, index=False)

    measure_count = (out["Type"] == "Measure").sum()
    calc_count = (out["Type"] == "Calculated Column").sum()
    print(f"Model cleanup report generated.")
    print(f"  Review file: {output_xlsx_file}")
    print(f"  Unused measures: {measure_count}")
    print(f"  Unused calculated columns: {calc_count}")

    return out


# ============================================================
# Views and security protection loader
# ============================================================

def build_views_and_security_protection(
    input_xlsx_path: str,
    views_sheet: str = "Views",
    security_sheet: str = "Security Names",
    views_table_col: str = "TableName",
    views_column_col: str = "ColumnName",
    security_table_col: str = "TableName",
) -> Tuple[Set[str], Set[str]]:
    """Load view columns and security tables from the manual input Excel file."""
    v = pd.read_excel(input_xlsx_path, sheet_name=views_sheet)

    missing_v = {views_table_col, views_column_col} - set(v.columns)
    if missing_v:
        raise ValueError(f"Views sheet missing columns: {missing_v}")

    v["_table_norm"] = v[views_table_col].astype(str).str.strip().str.lower()
    v["_column_norm"] = v[views_column_col].astype(str).str.strip().str.lower()
    v = v[(v["_table_norm"] != "") & (v["_column_norm"] != "")].copy()

    protected_view_columns = set(
        (v["_table_norm"] + "$$" + v["_column_norm"]).tolist(),
    )

    s = pd.read_excel(input_xlsx_path, sheet_name=security_sheet)

    if security_table_col not in s.columns:
        raise ValueError(f"Security sheet missing column: {security_table_col}")

    s["_table_norm"] = s[security_table_col].astype(str).str.strip().str.lower()
    s = s[s["_table_norm"] != ""].copy()

    protected_security_tables = set(s["_table_norm"].tolist())

    return protected_view_columns, protected_security_tables


# ============================================================
# Full pipeline orchestrator
# ============================================================

def run_pipeline(
    pbi_metadata_file: str,
    gold_file: str,
    views_security_file: str,
    output_dir: str = ".",
    schema: str = "dbo",
    views_sheet: str = "Views",
    security_sheet: str = "Security Names",
    security_file: Optional[str] = None,
):
    """Run the full 6-function optimization pipeline end-to-end.

    Args:
        pbi_metadata_file: Path to metadata Excel from Skill 1
        gold_file: Path to catalog Excel from Skill 2
        views_security_file: Path to manual Views/Security Excel
        output_dir: Directory for all output files
        schema: Database schema for DROP SQL (default: dbo)
        views_sheet: Sheet name for views in the views_security_file
        security_sheet: Sheet name for security tables in the views_security_file
        security_file: Path to Security_Tables_Detected.xlsx from Skill 3 (optional).
            If provided, auto-detected RLS tables are merged with the manual security list.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("PBI AutoGov — Optimization Pipeline")
    print("=" * 60)

    # Load protections from manual Views/Security file
    protected_view_columns, protected_security_tables = build_views_and_security_protection(
        input_xlsx_path=views_security_file,
        views_sheet=views_sheet,
        security_sheet=security_sheet,
    )

    # Merge auto-detected security tables from Skill 3 (if provided)
    if security_file and Path(security_file).is_file():
        auto_security_df = pd.read_excel(security_file)
        if "TableName" in auto_security_df.columns:
            auto_tables = {
                str(t).strip().lower()
                for t in auto_security_df["TableName"].dropna().unique()
                if str(t).strip() != ""
            }
            protected_security_tables = protected_security_tables | auto_tables
            print(f"  Merged {len(auto_tables)} auto-detected security table(s) from Skill 3")
    elif security_file:
        print(f"  WARNING: Security file not found: {security_file}")

    print(f"  Total protected security tables: {len(protected_security_tables)}")
    print(f"  Total protected view columns: {len(protected_view_columns)}")

    # F1: Report field usage
    f1 = function1_report_field_usage(pbi_metadata_file, str(out / "Function1_Output.xlsx"))

    # F2: Relationship resolver
    f2 = function2_relationship_columns_resolver(gold_file, str(out / "Function2_Output.xlsx"))

    # F3: Flag used in PBI
    f3, _ = function3_flag_columns_used_in_pbi(gold_file, f1, str(out / "Function3_Output.xlsx"))

    # F4: Flag used in relationships
    f4, _ = function4_flag_columns_used_in_relationships(gold_file, f2, str(out / "Function4_Output.xlsx"))

    # F5: Flag columns to remove
    f5 = function5_flag_columns_to_remove(
        f3, f4,
        str(out / "Function5_Output.xlsx"),
        protected_view_columns=protected_view_columns,
        protected_security_tables=protected_security_tables,
    )

    # F6: Flag tables to remove
    f6, kept_tables = function6_flag_tables_to_remove(
        f5,
        str(out / "Function6_Output.xlsx"),
        protected_security_tables=protected_security_tables,
    )

    # Generate DROP SQL
    drop_tables_df = generate_drop_table_sql(
        f6,
        output_sql_file=str(out / "DROP_TABLES.sql"),
        output_xlsx_file=str(out / "DROP_TABLES.xlsx"),
        schema=schema,
    )

    drop_cols_df = generate_drop_column_sql(
        f5,
        output_sql_file=str(out / "DROP_COLUMNS.sql"),
        output_xlsx_file=str(out / "DROP_COLUMNS.xlsx"),
        schema=schema,
        kept_tables=kept_tables,
    )

    # Model cleanup report: unused measures + calculated columns (TMDL-only items)
    model_cleanup_df = generate_model_cleanup_report(
        f5,
        output_xlsx_file=str(out / "MODEL_CLEANUP.xlsx"),
        kept_tables=kept_tables,
    )

    print(f"\n{'=' * 60}")
    print("Pipeline complete!")
    print(f"  Tables flagged for removal: {len(drop_tables_df)}")
    print(f"  DB columns flagged for DROP SQL: {len(drop_cols_df)}")
    print(f"  Model items flagged for TMDL cleanup: {len(model_cleanup_df)}")
    print(f"  Output directory: {out}")
    print(f"{'=' * 60}")

    return f1, f2, f3, f4, f5, f6, kept_tables, drop_tables_df, drop_cols_df, model_cleanup_df


# ============================================================
# Standalone execution
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PBI AutoGov — Optimization Pipeline")
    parser.add_argument("--metadata", required=True, help="Path to metadata Excel (Skill 1 output)")
    parser.add_argument("--catalog", required=True, help="Path to catalog Excel (Skill 2 output)")
    parser.add_argument("--security", default=None, help="Path to Security_Tables_Detected.xlsx (Skill 3 output)")
    parser.add_argument("--views-security", required=True, help="Path to Views/Security Excel (manual input)")
    parser.add_argument("--output-dir", default="output", help="Output directory for all files")
    parser.add_argument("--schema", default="dbo", help="Database schema for DROP SQL")
    parser.add_argument("--views-sheet", default="Views", help="Sheet name for views in Views/Security file")
    parser.add_argument("--security-sheet", default="Security Names", help="Sheet name for security in Views/Security file")
    args = parser.parse_args()

    run_pipeline(
        pbi_metadata_file=args.metadata,
        gold_file=args.catalog,
        views_security_file=args.views_security,
        output_dir=args.output_dir,
        schema=args.schema,
        views_sheet=args.views_sheet,
        security_sheet=args.security_sheet,
        security_file=args.security,
    )
