---
name: extract-metadata
description: Extract metadata from a Power BI PBIP report. Use when the user asks to extract metadata, parse a report, document visuals and fields, or analyze what a PBI report contains. Also use when the user mentions PBIP files, report.json, visual.json, or page.json.
---

# Extract Metadata from PBIP Report

## What This Skill Does
Parses a Power BI Project (PBIP) report to extract every visual, field, filter, and measure. Recursively resolves nested measure dependencies so all underlying column references are captured.

## When to Use
- User asks to "extract metadata" or "parse the report"
- User wants to know what visuals, fields, or measures are in a report
- User provides a PBIP report folder path
- This is Step 1 of the PBI AutoGov pipeline

## How to Run

### As an import (preferred when chaining with other skills):
```python
import sys
sys.path.insert(0, "skills")
from extract_metadata import extract_metadata, export_to_excel

df = extract_metadata(
    report_root="data/<ReportName>.Report/definition",
    model_root="data/<ModelName>.SemanticModel/definition"
)
export_to_excel(df, "output/pbi_report_metadata.xlsx")
```

### From command line:
```bash
python skills/extract_metadata.py \
    --report-root "data/<ReportName>.Report/definition" \
    --model-root "data/<ModelName>.SemanticModel/definition" \
    --output "output/pbi_report_metadata.xlsx"
```

## Required Inputs
1. **Report definition root** — folder containing `pages/` and `report.json`
2. **Semantic model definition root** — folder containing `tables/` (needed for measure DAX lookup)

## Output
- `pbi_report_metadata.xlsx` with columns: Page Name, Visual/Table Name in PBI, Visual Type, UI Field Name, Usage (Visual/Filter/Slicer), Measure Formula, Table in the Semantic Model, Column in the Semantic Model

## Validation
- Check that the number of pages matches the report
- Check that measures used in visuals have their source columns traced (not just the measure name)
- Check that report-level, page-level, and visual-level filters are all captured

## Known Limitations
- Implicit measures (auto-generated Sum, Count from drag-and-drop) are not tracked — only named measures defined in TMDL files appear in output

## Critical Rules
- NEVER modify original measure names
- ALWAYS resolve nested measure dependencies recursively
- Skip auto-generated visual filters that duplicate query state fields
