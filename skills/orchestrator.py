# -*- coding: utf-8 -*-
"""
Skill 5: orchestrator.py
PBI AutoGov — Power BI Data Governance Automation Pipeline

Chains all skills in sequence to go from raw PBIP files to DROP SQL + model cleanup
in a single run:
    1. extract_metadata.py  → pbi_report_metadata.xlsx
    2. generate_catalog.py  → Gold_Layer_Tables_Columns.xlsx
    3. filter_lineage.py    → Filter_Lineage.xlsx
    4. detect_security.py   → Security_Tables_Detected.xlsx
    5. optimization_pipeline.py → Function1-6 + DROP SQL + MODEL_CLEANUP.xlsx
    6. tmdl_cleanup.py      → Remove unused blocks from TMDL files (optional, user-prompted)

Input:  PBIP report root, semantic model root, Views/Security Excel
Output: Everything in the output directory
"""

import argparse
import sys
from pathlib import Path


def run_full_pipeline(
    report_root: str,
    model_root: str,
    views_security_file: str,
    output_dir: str = "output",
    schema: str = "dbo",
    views_sheet: str = "Views",
    security_sheet: str = "Security Names",
    tmdl_mode: str = "ask",
):
    """Run the complete PBI AutoGov pipeline end-to-end.

    Args:
        report_root: Path to PBIP report definition root (contains pages/, report.json)
        model_root: Path to semantic model definition root (contains tables/, relationships.tmdl)
        views_security_file: Path to manual Views/Security Excel
        output_dir: Directory for all output files
        schema: Database schema for DROP SQL (default: dbo)
        views_sheet: Sheet name for views in the views_security_file
        security_sheet: Sheet name for security tables in the views_security_file
        tmdl_mode: TMDL cleanup mode — "ask" (prompt user), "tmdl_only", "all", or "skip"
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Resolve the semantic model root for Skill 3 (needs parent of definition/)
    # model_root points to .../definition, Skill 3 needs .../SemanticModel
    model_root_path = Path(model_root)
    semantic_model_dir = model_root_path.parent if model_root_path.name == "definition" else model_root_path

    print("=" * 60)
    print("PBI AutoGov — Full Pipeline")
    print("=" * 60)

    # --- Validate inputs ---
    report_root_path = Path(report_root)
    pages_dir = report_root_path / "pages"
    tables_dir = model_root_path / "tables"

    errors = []
    if not report_root_path.is_dir():
        errors.append(f"Report root not found: {report_root_path}")
    elif not pages_dir.is_dir():
        errors.append(f"Pages directory not found: {pages_dir}")
    if not model_root_path.is_dir():
        errors.append(f"Model root not found: {model_root_path}")
    elif not tables_dir.is_dir():
        errors.append(f"Tables directory not found: {tables_dir}")
    if not Path(views_security_file).is_file():
        errors.append(f"Views/Security file not found: {views_security_file}")

    if errors:
        for err in errors:
            print(f"  ERROR: {err}")
        raise FileNotFoundError("Missing required input files. See errors above.")

    # --- Step 1: Extract metadata ---
    print(f"\n{'-' * 60}")
    print("STEP 1/6: Extracting report metadata")
    print(f"{'-' * 60}")

    from extract_metadata import extract_metadata, export_to_excel

    metadata_path = str(out / "pbi_report_metadata.xlsx")
    df_metadata = extract_metadata(report_root, model_root)

    if df_metadata.empty:
        raise ValueError("Metadata extraction returned no data. Check report paths.")

    export_to_excel(df_metadata, metadata_path)

    # --- Step 2: Generate catalog ---
    print(f"\n{'-' * 60}")
    print("STEP 2/6: Generating semantic model catalog")
    print(f"{'-' * 60}")

    from generate_catalog import generate_catalog, export_catalog

    catalog_path = str(out / "Gold_Layer_Tables_Columns.xlsx")
    tables_df, columns_df, relations_df = generate_catalog(model_root)

    if tables_df.empty:
        raise ValueError("Catalog generation returned no tables. Check model paths.")

    export_catalog(tables_df, columns_df, relations_df, catalog_path)

    # --- Step 3: Filter lineage analysis ---
    print(f"\n{'-' * 60}")
    print("STEP 3/6: Analyzing filter lineage")
    print(f"{'-' * 60}")

    from filter_lineage import compute_filter_lineage, export_filter_lineage

    lineage_path = str(out / "Filter_Lineage.xlsx")
    table_lineage_df, measure_lineage_df = compute_filter_lineage(catalog_path)
    export_filter_lineage(table_lineage_df, measure_lineage_df, lineage_path)

    # --- Step 4: Detect security tables ---
    print(f"\n{'-' * 60}")
    print("STEP 4/6: Detecting RLS security tables")
    print(f"{'-' * 60}")

    from detect_security import detect_security_tables, export_security_tables

    security_path = str(out / "Security_Tables_Detected.xlsx")
    security_tables = detect_security_tables(str(semantic_model_dir))
    export_security_tables(security_tables, security_path)

    # --- Step 5: Run optimization pipeline ---
    print(f"\n{'-' * 60}")
    print("STEP 5/6: Running optimization pipeline")
    print(f"{'-' * 60}")

    from optimization_pipeline import run_pipeline

    results = run_pipeline(
        pbi_metadata_file=metadata_path,
        gold_file=catalog_path,
        views_security_file=views_security_file,
        output_dir=output_dir,
        schema=schema,
        views_sheet=views_sheet,
        security_sheet=security_sheet,
        security_file=security_path,
    )

    f1, f2, f3, f4, f5, f6, kept_tables, drop_tables_df, drop_cols_df, model_cleanup_df = results

    # --- Step 6: TMDL Cleanup ---
    print(f"\n{'-' * 60}")
    print("STEP 6/6: TMDL Cleanup")
    print(f"{'-' * 60}")

    tmdl_removed = []
    tmdl_skipped = []

    # Determine cleanup mode
    if tmdl_mode == "ask":
        print("\nClean up TMDL files?")
        print("  1. Measures and calculated columns only (remove TMDL-only items)")
        print("  2. Everything (remove all unused items from TMDL)")
        print("  3. Skip (don't edit TMDL files)")
        choice = input("\nEnter choice (1/2/3): ").strip()
        if choice == "1":
            tmdl_mode = "tmdl_only"
        elif choice == "2":
            tmdl_mode = "all"
        else:
            tmdl_mode = "skip"

    if tmdl_mode in ("tmdl_only", "all"):
        from tmdl_cleanup import run_tmdl_cleanup, export_cleanup_report

        tmdl_removed, tmdl_skipped = run_tmdl_cleanup(
            function5_file=str(out / "Function5_Output.xlsx"),
            tables_dir=str(tables_dir),
            mode=tmdl_mode,
        )
        export_cleanup_report(
            tmdl_removed, tmdl_skipped,
            str(out / "TMDL_CLEANUP_REPORT.xlsx"),
        )
    else:
        print("  Skipping TMDL cleanup.")

    # --- Final summary ---
    print(f"\n{'=' * 60}")
    print("FULL PIPELINE COMPLETE")
    print(f"{'=' * 60}")
    lineage_reachable = table_lineage_df[table_lineage_df["Hops"] > 0]
    measure_reachable = measure_lineage_df[measure_lineage_df["Hops"] > 0]

    print(f"  Metadata rows extracted: {len(df_metadata)}")
    print(f"  Catalog: {len(tables_df)} tables, {len(columns_df)} columns, {len(relations_df)} relationships")
    print(f"  Filter lineage: {len(lineage_reachable)} table pairs, {len(measure_reachable)} measure pairs")
    print(f"  Security tables detected: {len(security_tables)}")
    print(f"  Tables flagged for removal: {len(drop_tables_df)}")
    print(f"  DB columns flagged for DROP SQL: {len(drop_cols_df)}")
    print(f"  Model items flagged for TMDL cleanup: {len(model_cleanup_df)}")
    print(f"  TMDL items removed: {len(tmdl_removed)}")
    print(f"  Output directory: {out}")
    print(f"{'=' * 60}")

    return results, tmdl_removed, tmdl_skipped


# ============================================================
# Standalone execution
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PBI AutoGov — Full Pipeline Orchestrator")
    parser.add_argument("--report-root", required=True,
                        help="Path to PBIP report definition root (contains pages/, report.json)")
    parser.add_argument("--model-root", required=True,
                        help="Path to semantic model definition root (contains tables/)")
    parser.add_argument("--views-security", required=True,
                        help="Path to Views/Security Excel (manual input)")
    parser.add_argument("--output-dir", default="output",
                        help="Output directory for all files")
    parser.add_argument("--schema", default="dbo",
                        help="Database schema for DROP SQL")
    parser.add_argument("--views-sheet", default="Views",
                        help="Sheet name for views in Views/Security file")
    parser.add_argument("--security-sheet", default="Security Names",
                        help="Sheet name for security in Views/Security file")
    parser.add_argument("--tmdl-mode", choices=["ask", "tmdl_only", "all", "skip"], default="ask",
                        help="TMDL cleanup mode: ask (prompt), tmdl_only, all, or skip (default: ask)")
    args = parser.parse_args()

    run_full_pipeline(
        report_root=args.report_root,
        model_root=args.model_root,
        views_security_file=args.views_security,
        output_dir=args.output_dir,
        schema=args.schema,
        views_sheet=args.views_sheet,
        security_sheet=args.security_sheet,
        tmdl_mode=args.tmdl_mode,
    )
