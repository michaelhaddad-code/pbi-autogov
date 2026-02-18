---
name: generate-catalog
description: Generate a semantic model catalog from TMDL files. Use when the user asks to catalog the semantic model, list tables and columns, parse TMDL files, or create a Gold Layer file. Also use when the user mentions relationships.tmdl or table definitions.
---

# Generate Semantic Model Catalog

## What This Skill Does
Parses TMDL files to build a complete inventory of the semantic model: all tables, columns (including measures and calculated columns), and relationships. Outputs a 3-sheet Excel catalog.

## When to Use
- User asks to "generate the catalog" or "parse the semantic model"
- User wants to see all tables, columns, and relationships
- User provides a semantic model folder path
- This is Step 2 of the PBI AutoGov pipeline

## How to Run

### As an import:
```python
import sys
sys.path.insert(0, "skills")
from generate_catalog import generate_catalog, export_catalog

tables_df, columns_df, relations_df = generate_catalog(
    model_root="data/<ModelName>.SemanticModel/definition"
)
export_catalog(tables_df, columns_df, relations_df, "output/Gold_Layer_Tables_Columns.xlsx")
```

### From command line:
```bash
python skills/generate_catalog.py \
    --model-root "data/<ModelName>.SemanticModel/definition" \
    --output "output/Gold_Layer_Tables_Columns.xlsx"
```

## Required Inputs
1. **Semantic model definition root** — folder containing `tables/` and `relationships.tmdl`

## Output
- `Gold_Layer_Tables_Columns.xlsx` with 3 sheets:
  - **Tables**: ID, Name
  - **Columns**: ID, ExplicitName, SourceColumn, TableID
  - **Relations**: FromTableID, FromColumnID, ToTableID, ToColumnID

## Validation
- Verify table count matches the number of .tmdl files in the tables/ directory
- Verify all relationships resolved (no WARNING messages about unresolved endpoints)
- Check that measures appear in the Columns sheet with SourceColumn = '[Measure]'

## Critical Rules
- Auto-generated IDs are sequential integers (1, 2, 3...) assigned in TMDL file order — must be consistent across all 3 sheets
- Both regular columns and calculated columns must be captured
- Relationships support quoted ('Table'.'Col'), bracketed ([Table].[Col]), and bare (Table.Col) formats
- Log warnings for unresolved relationship endpoints — never silently drop them
