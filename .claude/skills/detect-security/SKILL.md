---
name: detect-security
description: Detect RLS security tables from a Power BI semantic model. Use when the user asks about security tables, Row-Level Security, RLS roles, or protected tables. Also use when the user wants to know which tables should not be removed.
---

# Detect RLS Security Tables

## What This Skill Does
Scans RLS (Row-Level Security) role definitions in the semantic model to automatically identify security tables. These tables must be protected from removal by the optimization pipeline.

## When to Use
- User asks to "detect security tables" or "find RLS tables"
- User asks which tables are protected
- User provides a semantic model path and wants security analysis
- This is Step 3 of the PBI AutoGov pipeline

## How to Run

### As an import:
```python
import sys
sys.path.insert(0, "skills")
from detect_security import detect_security_tables, export_security_tables

security_tables = detect_security_tables(
    semantic_model_dir="data/<ModelName>.SemanticModel"
)
export_security_tables(security_tables, "output/Security_Tables_Detected.xlsx")
```

### From command line:
```bash
python skills/detect_security.py \
    --model-root "data/<ModelName>.SemanticModel" \
    --output "output/Security_Tables_Detected.xlsx"
```

## Required Inputs
1. **Semantic model root path** — the `.SemanticModel` folder (NOT the definition subfolder). The skill looks for `definition/roles/*.tmdl` inside it.

## Output
- `Security_Tables_Detected.xlsx` with a single column: TableName
- Returns an empty set if no roles folder exists (no RLS = no security tables)

## Integration with Pipeline
- When running via `orchestrator.py`, the output is automatically fed into the optimization pipeline via the `security_file` parameter
- When running standalone, pass the output to `optimization_pipeline.py --security "output/Security_Tables_Detected.xlsx"` to ensure auto-detected tables are protected

## Validation
- Check that detected tables match the RLS roles visible in Power BI Desktop
- Verify these tables are protected (Remove = No) in the pipeline output

## Critical Rules
- Security tables must ALWAYS be protected from removal regardless of usage
- If no roles folder exists, return empty set — do not error
