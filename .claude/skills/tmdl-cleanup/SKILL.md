---
name: tmdl-cleanup
description: Remove unused columns and measures directly from TMDL source files. Use when the user asks to clean up TMDL files, remove unused blocks from the semantic model definition, or edit TMDL files based on optimization results. Also use when the user mentions .tmdl.bak backups or TMDL block removal.
---

# TMDL Cleanup

## What This Skill Does
Reads the Function5 output (columns flagged for removal) and directly removes the corresponding column/measure blocks from the TMDL source files. Creates .tmdl.bak backups before any edit.

## When to Use
- User asks to "clean up TMDL files" or "remove unused items from TMDL"
- User wants to edit semantic model definition files based on optimization results
- User asks about removing measures or calculated columns from the model
- This is Step 6 of the PBI AutoGov pipeline (requires Function5_Output.xlsx from Step 5)

## How to Run

### As an import:
```python
import sys
sys.path.insert(0, "skills")
from tmdl_cleanup import run_tmdl_cleanup, export_cleanup_report

removed, skipped = run_tmdl_cleanup(
    function5_file="output/Function5_Output.xlsx",
    tables_dir="data/Revenue Opportunities.SemanticModel/definition/tables",
    mode="tmdl_only",  # or "all"
)
export_cleanup_report(removed, skipped, "output/TMDL_CLEANUP_REPORT.xlsx")
```

### From command line:
```bash
python skills/tmdl_cleanup.py \
    --function5 "output/Function5_Output.xlsx" \
    --tables-dir "data/Revenue Opportunities.SemanticModel/definition/tables" \
    --mode tmdl_only \
    --output "output/TMDL_CLEANUP_REPORT.xlsx"
```

## Required Inputs
1. **Function5_Output.xlsx** — output from optimization pipeline (Skill 5, Function 5)
2. **TMDL tables directory** — path to the semantic model `tables/` folder containing .tmdl files

## Modes
- `tmdl_only` — remove only measures and calculated columns (TMDL-only items that have no database counterpart). User runs DROP SQL separately for imported columns.
- `all` — remove everything flagged for removal (measures + calculated columns + imported columns). Use when you want TMDL to match the cleaned database.

## Output
- Modified `.tmdl` files with unused blocks removed
- `.tmdl.bak` backup files (one per edited TMDL file)
- `TMDL_CLEANUP_REPORT.xlsx` with two sheets:
  - **Removed** — items successfully removed (TableName, ItemName, ItemType, TMDL_File)
  - **Skipped** — items not found in TMDL (TableName, ItemName, ItemType, Reason)

## Safety
- Backups are always created before editing (`.tmdl.bak` alongside the original)
- Items not found in TMDL are logged and skipped (never crashes)
- Partition, hierarchy, and annotation blocks at table level are never touched
- The table declaration line is never modified
- To restore: rename `.tmdl.bak` files back to `.tmdl`

## Critical Rules
- Function5 is the source of truth — don't re-derive removal flags
- Protected items (view columns, security tables) are already excluded in Function5
- Block removal processes bottom-to-top to keep line indices stable
