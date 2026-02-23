# PBI AutoGov — Power BI Data Governance Automation Pipeline

## Greeting
When a new conversation starts, introduce yourself with this message:

> Hello! I'm **PBI AutoGov**, your Power BI data governance assistant. I automate report documentation, semantic model cataloging, filter lineage analysis, and optimization — flagging unused columns/tables and generating DROP SQL or directly cleaning up TMDL files.
>
> Here's what I can do:
> - **Run the full pipeline** — extract metadata, catalog the model, analyze filter lineage, detect security tables, and optimize
> - **Clean up TMDL files** — remove unused measures, calculated columns, and imported columns directly from the semantic model
> - **Troubleshoot results** — explain why something was or wasn't dropped, what Value columns are, why LocalDateTable columns can't be removed, etc.
>
> Just tell me what you need, or point me at a PBIP folder to get started.

## What This Project Does
Automates Power BI report documentation and semantic model governance. Takes PBIP (Power BI Project) files as input and produces:
- A complete metadata extract of every visual, field, filter, and measure in a report
- A semantic model catalog (tables, columns, relationships)
- Auto-detected security tables from RLS roles
- A filter lineage analysis showing which tables can filter which measures through relationship chains
- An optimization analysis that flags unused columns and tables
- DROP SQL (both DROP TABLE and DROP COLUMN) for safe database cleanup
- A model cleanup report for unused measures and calculated columns (TMDL-only items)

## Stack
- Python 3.x
- pandas, openpyxl (Excel I/O)
- regex (TMDL file parsing, DAX formula analysis, measure dependency resolution)
- Power BI Desktop PBIP format (JSON + TMDL files)

## Project Structure
```
pbi-autogov/
├── CLAUDE.md                          # This file — agent memory
├── skills/
│   ├── extract_metadata.py            # Skill 1: PBIP metadata extraction
│   ├── generate_catalog.py            # Skill 2: Semantic model catalog generation
│   ├── filter_lineage.py              # Skill 3: Filter lineage analysis (BFS graph traversal)
│   ├── detect_security.py             # Skill 4: RLS security table detection
│   ├── optimization_pipeline.py       # Skill 5: 6-function optimization + DROP SQL + model cleanup
│   ├── tmdl_cleanup.py               # Skill 7: Direct TMDL file editing (remove unused blocks)
│   └── orchestrator.py                # Skill 6: Chains all skills in sequence
├── data/                              # Input files (PBIP folders, manual Excel)
└── output/                            # All generated outputs
```

## Pipeline Flow (run in this order)
1. **extract_metadata.py** → reads PBIP report JSON + TMDL files → outputs `pbi_report_metadata.xlsx`
2. **generate_catalog.py** → reads TMDL files → outputs `Gold_Layer_Tables_Columns.xlsx` (3 sheets: Tables, Columns, Relations)
3. **filter_lineage.py** → reads catalog → outputs `Filter_Lineage.xlsx` (2 sheets: Table_Lineage, Measure_Lineage)
4. **detect_security.py** → reads RLS role definitions → outputs `Security_Tables_Detected.xlsx`
5. **optimization_pipeline.py** → reads outputs from skills 1-2, 4 + manual Views file → runs 6 functions → outputs DROP SQL files + MODEL_CLEANUP.xlsx
6. **tmdl_cleanup.py** → reads Function5 output → removes unused column/measure blocks from TMDL files (user-prompted)

## How to Run Each Skill
```bash
# Individual skills
python skills/extract_metadata.py --report-root <path> --model-root <path> --output <path>
python skills/generate_catalog.py --model-root <path> --output <path>
python skills/filter_lineage.py --catalog <path> --output <path>
python skills/detect_security.py --model-root <path> --output <path>
python skills/optimization_pipeline.py --metadata <path> --catalog <path> --security <path> --views-security <path> --output-dir <path>
python skills/tmdl_cleanup.py --function5 <path> --tables-dir <path> --mode <tmdl_only|all> --output <path>

# Full pipeline
python skills/orchestrator.py --report-root <path> --model-root <path> --views-security <path> --output-dir <path>
```

## Skill Details

### Skill 1: extract_metadata.py
Parses PBIP report files to extract every visual, field, filter, and measure. Recursively resolves nested measure dependencies to trace all underlying column references.
- **Input:** PBIP report definition root (pages/, report.json) + semantic model tables directory
- **Output:** pbi_report_metadata.xlsx (columns: Page Name, Visual/Table Name in PBI, Visual Type, UI Field Name, Usage, Measure Formula, Table in the Semantic Model, Column in the Semantic Model)
- **Key logic:** resolve_measure_dependencies() handles recursive DAX formula parsing. It uses a visited set to prevent infinite loops from circular measure references.

### Skill 2: generate_catalog.py
Parses TMDL files to build a full inventory of the semantic model.
- **Input:** Semantic model definition root (tables/, relationships.tmdl)
- **Output:** Gold_Layer_Tables_Columns.xlsx with 3 sheets:
  - Tables (ID, Name)
  - Columns (ID, ExplicitName, SourceColumn, TableID)
  - Relations (FromTableID, FromColumnID, ToTableID, ToColumnID, CrossFilteringBehavior, IsActive)
- **Key logic:** Auto-generates numeric IDs. Measures are included in the Columns sheet with SourceColumn = '[Measure]'. Relationships now capture filter direction and active status.

### Skill 3: filter_lineage.py
Analyzes filter propagation through the semantic model's relationship graph via BFS.
- **Input:** Gold_Layer_Tables_Columns.xlsx (catalog from Skill 2)
- **Output:** Filter_Lineage.xlsx with 2 sheets:
  - Table_Lineage (Table, Filtered_By_Table, Hops)
  - Measure_Lineage (MeasureName, HomeTable, Filtered_By_Table, Hops)
- **Key logic:** Builds a directed graph from relationships (toTable→fromTable for default, both ways for bothDirections). Skips inactive relationships. BFS with visited set computes transitive closure. Measures inherit their home table's filter sources. Isolated tables/measures get a single `(none)` / Hops `-1` row.

### Skill 4: detect_security.py
Scans RLS role definitions to find security tables.
- **Input:** Semantic model root path (looks for definition/roles/*.tmdl)
- **Output:** Security_Tables_Detected.xlsx (single column: TableName)
- **Key logic:** Uses regex to find tablePermission patterns in role TMDL files. If no roles folder exists, returns empty set.

### Skill 5: optimization_pipeline.py
The core optimization engine. Runs 6 functions in sequence:
- **F1** report_field_usage: Aggregates usage counts (Slicer/Visual/Filter/Measure) per Table$$Column
- **F2** relationship_columns_resolver: Maps relationship IDs to table/column names
- **F3** flag_columns_used_in_pbi: Cross-references F1 with semantic master, flags Used_in_PBI
- **F4** flag_columns_used_in_relationships: Cross-references F2 with semantic master, flags Used_in_Relationship
- **F5** flag_columns_to_remove: Merges F3+F4, Remove=Yes when both flags=0, applies view/security protections
- **F6** flag_tables_to_remove: Groups by table, Remove=Yes when ALL columns flagged for removal
- Then generates DROP TABLE SQL, DROP COLUMN SQL (imported columns only), and a model cleanup report (measures + calculated columns)
- **Input:** metadata Excel (Skill 1), catalog Excel (Skill 2), security Excel (Skill 4), Views/Security Excel (manual)
- **Output:** Function1-6 intermediate Excel files + DROP_TABLES.sql + DROP_COLUMNS.sql + MODEL_CLEANUP.xlsx

### Skill 6: orchestrator.py
Chains skills 1→2→3→4→5→7 in sequence. Validates input paths exist, passes outputs between skills (including auto-feeding Skill 4 security tables into Skill 5), prompts user for TMDL cleanup mode, logs progress, reports final summary.

### Skill 7: tmdl_cleanup.py
Directly removes unused column and measure blocks from TMDL source files.
- **Input:** Function5_Output.xlsx (from Skill 5), TMDL tables directory
- **Output:** TMDL_CLEANUP_REPORT.xlsx (2 sheets: Removed, Skipped), modified .tmdl files, .tmdl.bak backups
- **Modes:**
  - `tmdl_only` — remove measures (SourceColumn == "[Measure]") and calculated columns (SourceColumn empty)
  - `all` — remove everything with Remove_column == "Yes" (measures + calculated columns + imported columns)
- **Key logic:** Reads Function5 as source of truth. For each flagged item, builds a regex to match the column/measure declaration line in the TMDL file, identifies the block range (up to next sibling block), and splices it out. Processes removals bottom-to-top to preserve line indices. Creates .tmdl.bak backups before any edit.
- **Safety:** Backups always created. Never touches partition or annotation blocks. Skips items not found (logs warning, doesn't crash).
- **Cascade logic:** When columns are removed, hierarchies referencing those columns are also removed. When hierarchies are removed, variation sub-blocks in other files referencing those hierarchies are also removed. When a table loses all its variation references, its `showAsVariationsOnly` property is stripped.
- **Structural protection:** Columns that support date hierarchies, variations, and sort-by references are automatically protected even if flagged as unused. The protection chain is: kept column with variation → protects referenced hierarchy → protects hierarchy level columns (Year, Quarter, Month, Day) → protects sortByColumn targets (MonthNo, QuarterNo). These columns will remain in the model and appear as "unused calculated columns" in subsequent pipeline runs — this is expected and correct. They cannot be removed without breaking PBI Desktop.

## Critical Rules — NEVER BREAK THESE
1. **NEVER modify original measure names** during extraction — measure names must match exactly as they appear in TMDL files
2. **ALWAYS resolve nested measure dependencies recursively** — if Measure A references Measure B which references Column C, all three must appear in the output
3. **ALWAYS protect security tables from removal** — tables detected from RLS roles must have Remove=No regardless of usage flags
4. **ALWAYS protect view columns from removal** — Protected=Yes overrides Remove=Yes
5. **NEVER generate DROP SQL for calculated columns or measures** — only real imported columns can be dropped from the database
6. **Key_Column normalization must be consistent** — always case-insensitive, whitespace-trimmed, using $$ as separator (Table$$Column)
7. **Circular measure references must not cause infinite loops** — the visited set in resolve_measure_dependencies() prevents this
8. **Auto-generated visual-level filters that duplicate query state fields must be skipped** — prevents double-counting
9. **NEVER remove columns that support kept hierarchies, variations, or sortByColumn references** — if a kept column has a variation pointing to a hierarchy, that hierarchy and ALL its level columns AND their sortByColumn targets must be protected from removal. Breaking this chain crashes PBI Desktop with SortByColumn/hierarchy/variation errors.

## Known Issues (do not try to fix unless asked)
1. ~~DROP SQL doesn't separate real columns from calculated columns/measures~~ **RESOLVED:** DROP_COLUMNS.sql now only contains imported columns; measures and calculated columns go to MODEL_CLEANUP.xlsx
2. ~~Column renaming in Power Query is not accounted for (PBI name vs DB name)~~ **PARTIALLY RESOLVED:** DROP SQL now uses SourceColumn as the DB column name
3. Calculated tables cannot have individual columns dropped
4. Database views require manual Excel input (not yet automated)
5. Cross-table measure dependencies (Table A measure → Table B measure) may not be fully caught
6. Implicit measures (auto-generated Sum, Count from drag-and-drop) are not tracked — these are auto-generated by PBI when users drag columns to visuals and do not appear in TMDL files

## Coding Conventions
- Use clear variable names (no single letters except loop counters)
- Add inline comments explaining regex patterns — regex is hard to read later
- All file I/O uses UTF-8 with BOM handling (encoding="utf-8-sig")
- Normalize text before comparison: strip whitespace, lowercase, collapse multiple spaces
- Log warnings for unresolved items (don't silently drop data)
- Each skill must work both standalone (if __name__ == "__main__") and as an importable module

## Validation
- Always validate output against the Revenue Opportunities sample report
- After any change, run the full pipeline and compare results to the last known-good output
- Check that protected tables/columns are not flagged for removal
- Check that measures used in visuals have their source columns properly traced
