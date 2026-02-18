---
name: run-full-pipeline
description: Run the complete PBI AutoGov pipeline end-to-end. Use when the user asks to run everything, process a full report, or do a complete governance analysis. This chains extract-metadata, generate-catalog, detect-security, and optimization-pipeline in sequence.
---

# Run Full PBI AutoGov Pipeline

## What This Skill Does
Chains all 4 skills in sequence to go from raw PBIP files to DROP SQL in one run:
1. Extract metadata from the report
2. Generate the semantic model catalog
3. Detect security tables from RLS roles
4. Run the optimization pipeline to flag unused objects and generate SQL

## When to Use
- User says "run the full pipeline" or "process this report"
- User wants end-to-end governance analysis
- User provides PBIP folder paths and wants everything done

## How to Run

### Using the orchestrator (preferred):
```python
import sys
sys.path.insert(0, "skills")
from orchestrator import run_full_pipeline

results = run_full_pipeline(
    report_root="data/<ReportName>.Report/definition",
    model_root="data/<ModelName>.SemanticModel/definition",
    views_security_file="data/Views_Security.xlsx",
    output_dir="output",
    schema="dbo",
    views_sheet="Views",
    security_sheet="Security Names",
)
```

### From command line:
```bash
python skills/orchestrator.py \
    --report-root "data/<ReportName>.Report/definition" \
    --model-root "data/<ModelName>.SemanticModel/definition" \
    --views-security "data/Views_Security.xlsx" \
    --output-dir "output"
```

### Step-by-step (if you need control over each step):
```python
import sys
sys.path.insert(0, "skills")

# Step 1: Extract metadata
from extract_metadata import extract_metadata, export_to_excel

report_root = "data/<ReportName>.Report/definition"
model_def_root = "data/<ModelName>.SemanticModel/definition"

df_metadata = extract_metadata(report_root, model_def_root)
export_to_excel(df_metadata, "output/pbi_report_metadata.xlsx")

# Step 2: Generate catalog
from generate_catalog import generate_catalog, export_catalog

tables_df, columns_df, relations_df = generate_catalog(model_def_root)
export_catalog(tables_df, columns_df, relations_df, "output/Gold_Layer_Tables_Columns.xlsx")

# Step 3: Detect security tables
from detect_security import detect_security_tables, export_security_tables

model_root = "data/<ModelName>.SemanticModel"
security_tables = detect_security_tables(model_root)
export_security_tables(security_tables, "output/Security_Tables_Detected.xlsx")

# Step 4: Run optimization pipeline (security_file feeds Skill 3 output automatically)
from optimization_pipeline import run_pipeline

results = run_pipeline(
    pbi_metadata_file="output/pbi_report_metadata.xlsx",
    gold_file="output/Gold_Layer_Tables_Columns.xlsx",
    views_security_file="data/Views_Security.xlsx",
    output_dir="output",
    schema="dbo",
    views_sheet="Views",
    security_sheet="Security Names",
    security_file="output/Security_Tables_Detected.xlsx",
)

f1, f2, f3, f4, f5, f6, kept_tables, drop_tables_df, drop_cols_df, model_cleanup_df = results

print(f"\nFinal Summary:")
print(f"  Tables flagged for removal: {len(drop_tables_df)}")
print(f"  DB columns flagged for DROP SQL: {len(drop_cols_df)}")
print(f"  Model items flagged for TMDL cleanup: {len(model_cleanup_df)}")
```

## Required Inputs
1. **PBIP Report folder** — `<ReportName>.Report/definition/` (with pages/ and report.json)
2. **Semantic Model folder** — `<ModelName>.SemanticModel/` (with definition/tables/ and definition/relationships.tmdl)
3. **Views/Security Excel** — manual input file in `data/` folder

## Output (all in output/ folder)
- `pbi_report_metadata.xlsx` — full report metadata
- `Gold_Layer_Tables_Columns.xlsx` — semantic model catalog (3 sheets)
- `Security_Tables_Detected.xlsx` — auto-detected security tables
- `Function1-6_Output.xlsx` — intermediate pipeline results
- `DROP_TABLES.sql` + `DROP_TABLES.xlsx` — tables to remove
- `DROP_COLUMNS.sql` + `DROP_COLUMNS.xlsx` — imported columns to remove from database
- `MODEL_CLEANUP.xlsx` — unused measures and calculated columns to remove from TMDL files

## Pre-Run Checklist
Before running, verify:
- [ ] PBIP report folder exists and contains pages/ and report.json
- [ ] Semantic model folder exists and contains definition/tables/ and definition/relationships.tmdl
- [ ] Views/Security Excel exists with correct sheet names
- [ ] output/ directory exists

## Post-Run Validation
After running, check:
- [ ] Security tables show Remove=No in Function6 output
- [ ] Columns used in visuals show Used_in_PBI=1 in Function3 output
- [ ] DROP SQL file is not empty (unless all objects are in use)
- [ ] Compare results against last known-good output if available
