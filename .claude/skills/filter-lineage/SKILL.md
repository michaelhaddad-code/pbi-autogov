---
name: filter-lineage
description: Analyze filter propagation through the Power BI semantic model. Use when the user asks about filter flow, filter lineage, which tables filter which measures, relationship direction, or filter topology. Also use when the user wants to understand how slicers or filters propagate through the model.
---

# Filter Lineage Analysis

## What This Skill Does
Builds a directed graph from the semantic model's relationships and computes transitive filter propagation via BFS. Shows which tables can filter which other tables and measures, with hop counts.

## When to Use
- User asks about "filter lineage", "filter flow", or "filter propagation"
- User wants to know which tables can filter a specific measure
- User asks about relationship direction or cross-filtering behavior
- User wants to understand filter topology before cleanup
- This is Step 3 of the PBI AutoGov pipeline

## How to Run

### As an import:
```python
import sys
sys.path.insert(0, "skills")
from filter_lineage import compute_filter_lineage, export_filter_lineage

table_df, measure_df = compute_filter_lineage(
    catalog_file="output/Gold_Layer_Tables_Columns.xlsx"
)
export_filter_lineage(table_df, measure_df, "output/Filter_Lineage.xlsx")
```

### From command line:
```bash
python skills/filter_lineage.py \
    --catalog "output/Gold_Layer_Tables_Columns.xlsx" \
    --output "output/Filter_Lineage.xlsx"
```

## Required Inputs
1. **Catalog Excel** — Gold_Layer_Tables_Columns.xlsx from Skill 2 (must contain Tables, Columns, Relations sheets)

## Output
- `Filter_Lineage.xlsx` with 2 sheets:
  - **Table_Lineage**: Table, Filtered_By_Table, Hops
  - **Measure_Lineage**: MeasureName, HomeTable, Filtered_By_Table, Hops

## Validation
- Fact tables in a star schema should show dimension tables as filter sources (Hops=1)
- Dimension tables with no upstream should show `(none)` / Hops=-1
- Bidirectional relationships should create edges in both directions
- Inactive relationships should be excluded
- Measure lineage pairs = number of measures x number of filter sources per home table

## Critical Rules
- Self-filtering is excluded (trivial noise)
- Inactive relationships are excluded by default (require USERELATIONSHIP in DAX)
- BFS with visited set prevents infinite loops from bidirectional edges
- Backward-compatible with old catalogs missing CrossFilteringBehavior/IsActive columns
