---
name: optimization-pipeline
description: Run the optimization pipeline to find unused columns and tables in a Power BI semantic model. Use when the user asks to optimize, clean up, find unused objects, flag columns for removal, or generate DROP SQL. Also use when the user mentions Functions 1-6 or the pipeline.
---

# Run Optimization Pipeline

## What This Skill Does
Takes the metadata from extract-metadata, the catalog from generate-catalog, and security tables from detect-security, then runs a 6-function pipeline to identify unused columns and tables and generate DROP SQL for safe database cleanup.

## When to Use
- User asks to "run the pipeline" or "optimize the model"
- User wants to find unused columns or tables
- User asks to generate DROP SQL
- User asks about Functions 1-6
- This is Step 4 of the PBI AutoGov pipeline (requires outputs from Steps 1-3)

## How to Run

### As an import:
```python
import sys
sys.path.insert(0, "skills")
from optimization_pipeline import run_pipeline

results = run_pipeline(
    pbi_metadata_file="output/pbi_report_metadata.xlsx",
    gold_file="output/Gold_Layer_Tables_Columns.xlsx",
    views_security_file="data/Views_Security.xlsx",
    output_dir="output",
    schema="dbo",
    views_sheet="Views",
    security_sheet="Security Names",
    security_file="output/Security_Tables_Detected.xlsx",  # optional: auto-detected RLS tables from Skill 3
)
```

### From command line:
```bash
python skills/optimization_pipeline.py \
    --metadata "output/pbi_report_metadata.xlsx" \
    --catalog "output/Gold_Layer_Tables_Columns.xlsx" \
    --security "output/Security_Tables_Detected.xlsx" \
    --views-security "data/Views_Security.xlsx" \
    --output-dir "output" \
    --schema dbo \
    --views-sheet "Views" \
    --security-sheet "Security Names"
```

## Required Inputs
1. **pbi_report_metadata.xlsx** — output from extract-metadata (Skill 1)
2. **Gold_Layer_Tables_Columns.xlsx** — output from generate-catalog (Skill 2)
3. **Security_Tables_Detected.xlsx** — output from detect-security (Skill 3, optional but recommended)
4. **Views_Security.xlsx** — manual input with two sheets:
   - "Views" sheet (columns: TableName, ColumnName)
   - "Security Names" sheet (column: TableName)

## Output
- Function1-6 intermediate Excel files
- `DROP_TABLES.sql` + `DROP_TABLES.xlsx` — tables to remove
- `DROP_COLUMNS.sql` + `DROP_COLUMNS.xlsx` — imported columns to remove from database
- `MODEL_CLEANUP.xlsx` — unused measures and calculated columns to remove from TMDL files

## Pipeline Function Chain
1. **F1** — Aggregates Slicer/Visual/Filter/Measure usage counts per Table$$Column
2. **F2** — Resolves relationship IDs to table/column names
3. **F3** — Flags columns used in PBI (cross-references F1 with catalog)
4. **F4** — Flags columns used in relationships (cross-references F2 with catalog)
5. **F5** — Merges F3+F4, flags Remove=Yes when both flags=0, applies protections
6. **F6** — Flags entire tables for removal when ALL columns are flagged

## Validation
- Check that protected tables show Remove=No
- Check that columns used in visuals show Used_in_PBI=1
- Check that relationship columns show Used_in_Relationship=1
- Verify DROP_COLUMNS.sql only targets real imported columns (measures/calculated columns are in MODEL_CLEANUP.xlsx)
- Check that MODEL_CLEANUP.xlsx lists only unused measures and calculated columns

## Critical Rules
- View columns must ALWAYS be protected (Protected=Yes overrides Remove=Yes)
- Security tables must ALWAYS be protected
- Key_Column normalization must be consistent (case-insensitive, whitespace-trimmed, $$ separator)
- Only generate DROP COLUMN for tables that are NOT being dropped entirely
