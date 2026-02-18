# PBI AutoGov

Power BI Data Governance Automation Pipeline. Takes Power BI Project (PBIP) files as input and identifies unused tables, columns, and measures in your semantic model, then generates the SQL and cleanup reports to remove them.

## What It Does

1. **Extract Metadata** — parses every visual, field, filter, and measure from a PBIP report
2. **Generate Catalog** — builds a full inventory of tables, columns, and relationships from TMDL files
3. **Detect Security Tables** — scans RLS role definitions to find protected tables
4. **Optimize** — cross-references report usage against the catalog to flag unused objects
5. **Generate Output** — produces DROP SQL for database cleanup and a model cleanup report for TMDL edits

## Quick Start

```bash
pip install pandas openpyxl
```

```bash
python skills/orchestrator.py \
    --report-root "data/<ReportName>.Report/definition" \
    --model-root "data/<ModelName>.SemanticModel/definition" \
    --views-security "data/Views_Security.xlsx" \
    --output-dir output
```

## Output

| File | Description |
|------|-------------|
| `pbi_report_metadata.xlsx` | Full report metadata (visuals, fields, filters, measures) |
| `Gold_Layer_Tables_Columns.xlsx` | Semantic model catalog (tables, columns, relationships) |
| `Security_Tables_Detected.xlsx` | Auto-detected RLS security tables |
| `Function1-6_Output.xlsx` | Intermediate pipeline results |
| `DROP_TABLES.sql` / `.xlsx` | Tables safe to remove from the database |
| `DROP_COLUMNS.sql` / `.xlsx` | Imported columns safe to remove from the database |
| `MODEL_CLEANUP.xlsx` | Unused measures and calculated columns to remove from TMDL files |

## Required Inputs

- **PBIP Report folder** — exported from Power BI Desktop (contains `pages/` and `report.json`)
- **Semantic Model folder** — exported from Power BI Desktop (contains `tables/` and `relationships.tmdl`)
- **Views/Security Excel** — manual input listing database views and security table names (sheets: `Views`, `Security Names`)
