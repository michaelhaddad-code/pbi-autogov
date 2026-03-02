# -*- coding: utf-8 -*-
"""
Skill 1: extract_metadata.py
PBI AutoGov — Power BI Data Governance Automation Pipeline

Parses PBIP report files (JSON + TMDL) to extract every visual, field, filter,
and measure used in a report. Recursively resolves nested measure dependencies
to trace all underlying column references.

Input:  PBIP report definition root (pages/, report.json)
        Semantic model tables directory (for measure DAX lookup)
Output: pbi_report_metadata.xlsx
"""

import argparse
import json
import re
import sys
import io
from pathlib import Path
from collections import Counter

# Ensure stdout can handle Unicode on Windows (cp1252 can't encode many chars)
if sys.stdout.encoding and sys.stdout.encoding.lower().replace("-", "") != "utf8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd


# ============================================================
# Visual type display names
# ============================================================

VISUAL_TYPE_DISPLAY = {
    "barChart": "Bar Chart",
    "clusteredBarChart": "Clustered Bar Chart",
    "clusteredColumnChart": "Clustered Column Chart",
    "stackedBarChart": "Stacked Bar Chart",
    "stackedColumnChart": "Stacked Column Chart",
    "hundredPercentStackedBarChart": "100% Stacked Bar Chart",
    "hundredPercentStackedColumnChart": "100% Stacked Column Chart",
    "lineChart": "Line Chart",
    "areaChart": "Area Chart",
    "stackedAreaChart": "Stacked Area Chart",
    "lineStackedColumnComboChart": "Line & Stacked Column Chart",
    "lineClusteredColumnComboChart": "Line & Clustered Column Chart",
    "ribbonChart": "Ribbon Chart",
    "waterfallChart": "Waterfall Chart",
    "funnelChart": "Funnel Chart",
    "pieChart": "Pie Chart",
    "donutChart": "Donut Chart",
    "treemap": "Treemap",
    "map": "Map",
    "filledMap": "Filled Map",
    "shapeMap": "Shape Map",
    "azureMap": "Azure Map",
    "tableEx": "Table",
    "pivotTable": "Matrix",
    "card": "Card",
    "multiRowCard": "Multi-Row Card",
    "kpi": "KPI",
    "gauge": "Gauge",
    "slicer": "Slicer",
    "scatterChart": "Scatter Chart",
    "decompositionTreeVisual": "Decomposition Tree",
    "keyDriversVisual": "Key Influencers",
    "qnaVisual": "Q&A",
    "scriptVisual": "R Script Visual",
    "pythonVisual": "Python Visual",
    "aiNarratives": "Smart Narrative",
    "paginator": "Paginated Report Visual",
    "cardVisual": "New Card",
    "advancedSlicerVisual": "New Slicer",
    "referenceLabel": "Reference Label",
}

# Visual types to skip (no data fields)
SKIP_VISUAL_TYPES = {
    "actionButton", "image", "textbox", "shape", "bookmarkNavigator",
    "pageNavigator", "groupShape",
}


# ============================================================
# Role → usage label mapping
# ============================================================

ROLE_USAGE_MAP = {
    # Slicer
    ("slicer", "Values"): "Slicer",
    ("advancedSlicerVisual", "Values"): "Slicer",
    # Table / Matrix
    ("tableEx", "Values"): "Visual Column",
    ("tableEx", "Rows"): "Visual Column",
    ("pivotTable", "Values"): "Visual Value",
    ("pivotTable", "Rows"): "Visual Row",
    ("pivotTable", "Columns"): "Visual Column",
    # Cards
    ("card", "Values"): "Visual Value",
    ("cardVisual", "Values"): "Visual Value",
    ("multiRowCard", "Values"): "Visual Value",
    ("kpi", "Value"): "Visual Value",
    ("kpi", "Goal"): "Visual Goal",
    ("kpi", "Trend"): "Visual Trend",
    # Gauge
    ("gauge", "Value"): "Visual Value",
    ("gauge", "MinValue"): "Visual Min",
    ("gauge", "MaxValue"): "Visual Max",
    ("gauge", "TargetValue"): "Visual Target",
}

DEFAULT_ROLE_MAP = {
    "Category": "Visual Column",
    "Y": "Visual Value",
    "Series": "Visual Column",
    "Values": "Visual Value",
    "Rows": "Visual Row",
    "Columns": "Visual Column",
    "Fields": "Visual Column",
    "Analyze": "Visual Value",
    "ExplainBy": "Visual Column",
    "Target": "Visual Column",
    "Location": "Visual Column",
    "Latitude": "Visual Column",
    "Longitude": "Visual Column",
    "Size": "Visual Value",
    "Color": "Visual Column",
    "Tooltips": "Visual Tooltip",
    "Value": "Visual Value",
    "Goal": "Visual Goal",
    "Trend": "Visual Trend",
}


def get_usage_label(vis_type: str, role: str, is_measure: bool) -> str:
    """Determine the usage label for a field based on visual type, role, and measure status."""
    base = ROLE_USAGE_MAP.get((vis_type, role))
    if not base:
        base = DEFAULT_ROLE_MAP.get(role, f"Visual {role}")
    if is_measure:
        return f"{base}, Filter (Measure)"
    return base


# ============================================================
# TMDL measure parser
# ============================================================

def parse_tmdl_files(tables_dir: Path) -> dict:
    """Parse all TMDL files to extract measures and their DAX formulas.
    Returns dict of (table_name, measure_name) -> dax_formula.
    """
    measures = {}
    if not tables_dir.is_dir():
        print(f"WARNING: Tables directory not found: {tables_dir}")
        return measures
    for tmdl_file in sorted(tables_dir.glob("**/*.tmdl")):
        _parse_single_tmdl(tmdl_file, measures)
    return measures


def build_column_catalog(tables_dir: Path) -> dict:
    """Build a quick lookup of all column and measure names per table from TMDL files.
    Returns dict of {table_name_lower: set(column_name_lower, ...)}.
    Also returns a reverse map: {column_name_lower: [table_name, ...]}.
    """
    table_columns = {}  # table_lower -> set of column_lower
    column_to_tables = {}  # column_lower -> list of table_name (original case)

    if not tables_dir.is_dir():
        return table_columns, column_to_tables

    for tmdl_file in sorted(tables_dir.glob("**/*.tmdl")):
        content = tmdl_file.read_text(encoding="utf-8-sig")
        table_match = re.match(r"^table\s+(.+?)$", content, re.MULTILINE)
        if not table_match:
            continue
        table_name = table_match.group(1).strip().strip("'")
        table_lower = table_name.lower()
        cols = set()

        # Match column declarations: regular "\tcolumn Name" and calculated "\tcolumn Name = expr"
        for cm in re.finditer(r"^\tcolumn\s+'?([^'=\n]+?)'?\s*(?:=.*)?$", content, re.MULTILINE):
            col_name = cm.group(1).strip()
            col_lower = col_name.lower()
            cols.add(col_lower)
            column_to_tables.setdefault(col_lower, []).append(table_name)

        # Match measure declarations: "\tmeasure Name =" or "\tmeasure 'Name' ="
        for mm in re.finditer(r"^\tmeasure\s+'?([^'=\n]+?)'?\s*=", content, re.MULTILINE):
            meas_name = mm.group(1).strip()
            meas_lower = meas_name.lower()
            cols.add(meas_lower)
            column_to_tables.setdefault(meas_lower, []).append(table_name)

        table_columns[table_lower] = cols

    return table_columns, column_to_tables


def _parse_single_tmdl(filepath: Path, measures: dict):
    """Extract measure definitions from a single TMDL file."""
    content = filepath.read_text(encoding="utf-8-sig")

    # Extract table name from first line
    table_match = re.match(r"^table\s+(.+?)$", content, re.MULTILINE)
    if not table_match:
        return
    table_name = table_match.group(1).strip().strip("'")

    # Regex to capture measure name and DAX formula
    # Group 1: measure name, Group 2: DAX formula body
    measure_pattern = re.compile(
        r"^\tmeasure\s+'?([^'=\n]+?)'?\s*=\s*(.*?)(?=^\t(?:measure|column|hierarchy|partition|annotation)\s|\Z)",
        re.MULTILINE | re.DOTALL,
    )

    for m in measure_pattern.finditer(content):
        measure_name = m.group(1).strip().strip("'")
        raw_formula = m.group(2).strip()

        # Clean up: stop at TMDL metadata keywords (may appear with leading whitespace)
        # Pattern: keyword followed by optional whitespace and colon (or space for annotations)
        formula_lines = []
        for line in raw_formula.split("\n"):
            stripped = line.strip()
            if re.match(r"^(formatString|lineageTag|annotation|extendedProperty|displayFolder|dataCategory)\s*[: ]", stripped):
                break
            formula_lines.append(line)

        formula = "\n".join(formula_lines).strip()
        # Remove fenced code block wrappers
        formula = re.sub(r"^```\s*\n?", "", formula)
        formula = re.sub(r"\n?\s*```\s*$", "", formula)
        # Clean up indentation
        formula = re.sub(r"\t{2,}", "    ", formula)
        formula = re.sub(r"\t", "    ", formula)
        # Collapse multiple blank lines
        formula = re.sub(r"\n\s*\n", "\n", formula)
        formula = formula.strip()

        measures[(table_name, measure_name)] = formula if formula else ""


# ============================================================
# Field extraction from visual/filter JSON
# ============================================================

def extract_field_info(field: dict) -> list[dict]:
    """Extract table, column/measure, and type from a field definition.
    Used at visual, page, and report level.
    """
    results = []

    if "Column" in field:
        col = field["Column"]
        entity = _get_entity(col)
        prop = col.get("Property", "")
        results.append({"entity": entity, "property": prop, "field_type": "Column"})

    elif "Measure" in field:
        meas = field["Measure"]
        entity = _get_entity(meas)
        prop = meas.get("Property", "")
        results.append({"entity": entity, "property": prop, "field_type": "Measure"})

    elif "Aggregation" in field:
        agg = field["Aggregation"]
        expr = agg.get("Expression", {})
        if "Column" in expr:
            col = expr["Column"]
            entity = _get_entity(col)
            prop = col.get("Property", "")
            agg_func = _get_agg_name(agg.get("Function", 0))
            results.append({
                "entity": entity, "property": prop,
                "field_type": f"Aggregation ({agg_func})",
            })

    elif "HierarchyLevel" in field:
        hl = field["HierarchyLevel"]
        expr = hl.get("Expression", {})
        if "Hierarchy" in expr:
            hier = expr["Hierarchy"]
            entity = _get_entity(hier)
            hierarchy_name = hier.get("Hierarchy", "")
            level_name = hl.get("Level", "")
            prop = level_name or hierarchy_name

            # Fallback: resolve from PropertyVariationSource (auto-generated date hierarchies)
            if not entity:
                inner_expr = hier.get("Expression", {})
                pvs = inner_expr.get("PropertyVariationSource", {})
                if pvs:
                    entity = _get_entity(pvs)
                    prop = pvs.get("Property", prop)

            results.append({"entity": entity, "property": prop, "field_type": "HierarchyLevel"})

    return results


def _get_entity(node: dict) -> str:
    """Extract table name from a field node. Tries Expression > SourceRef > Entity."""
    try:
        return node["Expression"]["SourceRef"]["Entity"]
    except (KeyError, TypeError):
        return ""


def _get_agg_name(func_id: int) -> str:
    """Map Power BI aggregation function ID to readable name."""
    agg_map = {0: "Sum", 1: "Avg", 2: "Count", 3: "Min", 4: "Max",
               5: "CountNonNull", 6: "Median"}
    return agg_map.get(func_id, f"Func{func_id}")


# ============================================================
# Measure dependency resolution (recursive)
# ============================================================

def _strip_dax_comments(formula: str) -> str:
    """Remove DAX comments from a formula before parsing column references.
    Handles single-line comments (-- ...) and block comments (/* ... */).
    Respects string literals so -- inside "..." is not treated as a comment.
    """
    # First, remove block comments /* ... */ (may span multiple lines)
    formula = re.sub(r"/\*.*?\*/", "", formula, flags=re.DOTALL)

    # Then, remove single-line comments (-- to end of line),
    # but only when the -- is not inside a string literal.
    cleaned_lines = []
    for line in formula.split("\n"):
        in_string = False
        i = 0
        while i < len(line):
            if line[i] == '"':
                in_string = not in_string
            elif line[i : i + 2] == "--" and not in_string:
                line = line[:i]
                break
            i += 1
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def resolve_measure_dependencies(formula: str, measures_lookup: dict,
                                 visited: set = None) -> list[dict]:
    """Parse a DAX formula and identify all tables/columns it uses,
    including those from nested measures. Uses a visited set to prevent
    infinite loops from circular dependencies.
    """
    if visited is None:
        visited = set()

    dependencies = []

    # Strip DAX comments before parsing to avoid picking up
    # commented-out column references (e.g. -- Opportunities[PipelineStep])
    clean_formula = _strip_dax_comments(formula)

    # Find direct Table[Column] references
    # Pattern: 'TableName'[ColumnName] or TableName[ColumnName]
    direct_refs = re.findall(
        r"(?:'([^']+)'|([A-Za-z_][\w\s]*?))\[([^\]]+)\]",
        clean_formula,
    )
    for quoted_table, unquoted_table, column in direct_refs:
        table = (quoted_table or unquoted_table).strip()
        col = column.strip()
        if table and col:
            dep = {"table": table, "column": col}
            if dep not in dependencies:
                dependencies.append(dep)

    # Find standalone [MeasureName] references (nested measures)
    nested_refs = re.findall(r"(?<!['\w\]])\[([^\]]+)\]", clean_formula)

    for ref_name in nested_refs:
        ref_name = ref_name.strip()
        # Skip if already captured as a direct column reference
        if any(d["column"] == ref_name for d in dependencies):
            continue
        # Skip if already visited (prevents circular dependency loops)
        if ref_name in visited:
            continue
        visited.add(ref_name)

        # Look up this measure in the measures_lookup to find its DAX
        for (tbl, mname), sub_formula in measures_lookup.items():
            if mname == ref_name:
                # Include the nested measure itself as a dependency
                nested_dep = {"table": tbl, "column": mname}
                if nested_dep not in dependencies:
                    dependencies.append(nested_dep)

                # Recursively resolve the nested measure's dependencies
                sub_deps = resolve_measure_dependencies(sub_formula, measures_lookup, visited)
                for dep in sub_deps:
                    if dep not in dependencies:
                        dependencies.append(dep)
                break

    return dependencies


def get_measure_source_tables(entity: str, prop: str, measures_lookup: dict) -> list[dict]:
    """Get all source tables/columns for a measure, including nested dependencies.
    Groups columns by table for cleaner output.
    """
    formula = measures_lookup.get((entity, prop), "")
    if not formula:
        return [{"table": entity, "column": prop}]
    deps = resolve_measure_dependencies(formula, measures_lookup)
    if not deps:
        return [{"table": entity, "column": prop}]

    measure_dep = {"table": entity, "column": prop}
    if measure_dep not in deps:
        deps.insert(0, measure_dep)

    table_cols = {}
    for dep in deps:
        t, c = dep["table"], dep["column"]
        if t not in table_cols:
            table_cols[t] = []
        if c not in table_cols[t]:
            table_cols[t].append(c)
    return [{"table": t, "column": ", ".join(cols)} for t, cols in table_cols.items()]


# ============================================================
# Visual parser
# ============================================================

def get_visual_display_name(vis_type: str) -> str:
    """Convert camelCase visual type to human-readable name."""
    if vis_type in VISUAL_TYPE_DISPLAY:
        return VISUAL_TYPE_DISPLAY[vis_type]
    name = re.sub(r"([A-Z])", r" \1", vis_type).strip()
    return name.title()


def _get_visual_title(vis: dict) -> str:
    """Extract explicit title from visual JSON, if set."""
    try:
        titles = vis.get("visualContainerObjects", {}).get("title", [])
        for t in titles:
            text_expr = t.get("properties", {}).get("text", {})
            if "expr" in text_expr:
                val = text_expr["expr"].get("Literal", {}).get("Value", "")
                cleaned = val.strip("'")
                if cleaned:
                    return cleaned
    except (KeyError, TypeError, AttributeError):
        pass
    return ""


def _process_measure_field(page_name, vis_label, vis_type, display_name, usage, formula,
                           entity, prop, measures_lookup):
    """Helper: resolve a measure field into output rows (handles nested dependencies)."""
    rows = []
    if formula:
        source_tables = get_measure_source_tables(entity, prop, measures_lookup)
        for st in source_tables:
            rows.append({
                "Page Name": page_name,
                "Visual/Table Name in PBI": vis_label,
                "Visual Type": vis_type,
                "UI Field Name": display_name,
                "Usage (Visual/Filter/Slicer)": usage,
                "Measure Formula": formula,
                "Table in the Semantic Model": st["table"],
                "Column in the Semantic Model": st["column"],
            })
    return rows


def parse_visual(visual_json: dict, page_name: str, measures_lookup: dict,
                 vis_type_counter: Counter) -> list[dict]:
    """Parse a single visual.json and return rows for the output."""
    rows = []
    vis = visual_json.get("visual", {})
    vis_type = vis.get("visualType", "unknown")

    if vis_type in SKIP_VISUAL_TYPES:
        return rows

    # Determine visual label (title or auto-generated name)
    vis_title = _get_visual_title(vis)
    if vis_title:
        vis_label = vis_title
    else:
        vis_type_counter[vis_type] += 1
        count = vis_type_counter[vis_type]
        display_type = get_visual_display_name(vis_type)
        vis_label = display_type if count == 1 else f"{display_type} ({count})"

    # --- Query state fields (visual data roles) ---
    query_state = vis.get("query", {}).get("queryState", {})
    for role, role_data in query_state.items():
        projections = role_data.get("projections", [])
        for proj in projections:
            field = proj.get("field", {})
            display_name = proj.get("displayName", "")
            field_infos = extract_field_info(field)

            for fi in field_infos:
                if not display_name:
                    display_name = fi["property"]

                is_measure = fi["field_type"] == "Measure"
                formula = ""
                if is_measure:
                    formula = measures_lookup.get((fi["entity"], fi["property"]), "")

                usage = get_usage_label(vis_type, role, is_measure)

                if is_measure and formula:
                    rows.extend(_process_measure_field(
                        page_name, vis_label, vis_type, display_name, usage, formula,
                        fi["entity"], fi["property"], measures_lookup,
                    ))
                else:
                    rows.append({
                        "Page Name": page_name,
                        "Visual/Table Name in PBI": vis_label,
                        "Visual Type": vis_type,
                        "UI Field Name": display_name,
                        "Usage (Visual/Filter/Slicer)": usage,
                        "Measure Formula": formula,
                        "Table in the Semantic Model": fi["entity"],
                        "Column in the Semantic Model": fi["property"],
                    })

    # --- Collect fields already captured (to skip duplicate auto-generated filters) ---
    query_fields = set()
    for row in rows:
        query_fields.add((row["Table in the Semantic Model"], row["Column in the Semantic Model"]))

    # --- Visual-level filters ---
    vis_filters = visual_json.get("filterConfig", {}).get("filters", [])
    for flt in vis_filters:
        flt_field = flt.get("field", {})
        field_infos = extract_field_info(flt_field)
        for fi in field_infos:
            # Skip auto-generated filters that duplicate query state fields
            if (fi["entity"], fi["property"]) in query_fields:
                continue

            is_measure = fi["field_type"] == "Measure"
            formula = ""
            if is_measure:
                formula = measures_lookup.get((fi["entity"], fi["property"]), "")
                usage_str = "Filter (Measure)"
                if formula:
                    rows.extend(_process_measure_field(
                        page_name, vis_label, vis_type, fi["property"], usage_str, formula,
                        fi["entity"], fi["property"], measures_lookup,
                    ))
                    continue
            else:
                usage_str = "Filter"

            rows.append({
                "Page Name": page_name,
                "Visual/Table Name in PBI": vis_label,
                "Visual Type": vis_type,
                "UI Field Name": fi["property"],
                "Usage (Visual/Filter/Slicer)": usage_str,
                "Measure Formula": formula,
                "Table in the Semantic Model": fi["entity"],
                "Column in the Semantic Model": fi["property"],
            })

    return rows


# ============================================================
# Page filter parser
# ============================================================

def parse_page_filters(page_json: dict, page_name: str, measures_lookup: dict) -> list[dict]:
    """Extract page-level filters."""
    rows = []
    filters = page_json.get("filterConfig", {}).get("filters", [])
    for flt in filters:
        flt_field = flt.get("field", {})
        field_infos = extract_field_info(flt_field)
        for fi in field_infos:
            is_measure = fi["field_type"] == "Measure"
            formula = ""
            if is_measure:
                formula = measures_lookup.get((fi["entity"], fi["property"]), "")
                usage_str = "Page Filter (Measure)"
                if formula:
                    rows.extend(_process_measure_field(
                        page_name, "Page Filters", "pageFilter", fi["property"],
                        usage_str, formula, fi["entity"], fi["property"], measures_lookup,
                    ))
                    continue
            else:
                usage_str = "Page Filter"

            rows.append({
                "Page Name": page_name,
                "Visual/Table Name in PBI": "Page Filters",
                "Visual Type": "pageFilter",
                "UI Field Name": fi["property"],
                "Usage (Visual/Filter/Slicer)": usage_str,
                "Measure Formula": formula,
                "Table in the Semantic Model": fi["entity"],
                "Column in the Semantic Model": fi["property"],
            })
    return rows


# ============================================================
# Main extraction function
# ============================================================

def extract_metadata(report_root: str, model_root: str) -> pd.DataFrame:
    """Main entry point: extract all metadata from a PBIP report.

    Args:
        report_root: Path to PBIP report definition root (contains pages/, report.json)
        model_root: Path to semantic model definition root (contains tables/)

    Returns:
        DataFrame with all extracted metadata rows.
    """
    tables_dir = Path(model_root) / "tables"
    pages_dir = Path(report_root) / "pages"

    print("=" * 60)
    print("PBI AutoGov — Metadata Extractor")
    print("=" * 60)

    # [1] Parse measures from semantic model
    print(f"\n[1] Parsing semantic model: {tables_dir}")
    measures_lookup = parse_tmdl_files(tables_dir)
    print(f"    Found {len(measures_lookup)} measures")
    if measures_lookup:
        print("    Sample measures:")
        for i, ((tbl, mname), _) in enumerate(list(measures_lookup.items())[:3]):
            print(f"      - {tbl}.{mname}")

    # [2] Parse report-level filters
    print(f"\n[2] Checking for report-level filters")
    report_json_path = Path(report_root) / "report.json"
    all_rows = []

    if report_json_path.is_file():
        report_json = json.loads(report_json_path.read_text(encoding="utf-8-sig"))
        report_filters = report_json.get("filterConfig", {}).get("filters", [])
        if report_filters:
            for flt in report_filters:
                flt_field = flt.get("field", {})
                field_infos = extract_field_info(flt_field)
                for fi in field_infos:
                    is_measure = fi["field_type"] == "Measure"
                    formula = ""
                    if is_measure:
                        formula = measures_lookup.get((fi["entity"], fi["property"]), "")
                        usage_str = "Report Filter (Measure)"
                        if formula:
                            all_rows.extend(_process_measure_field(
                                "(All Pages)", "Report Filters", "reportFilter",
                                fi["property"], usage_str, formula,
                                fi["entity"], fi["property"], measures_lookup,
                            ))
                            continue
                    else:
                        usage_str = "Report Filter"

                    all_rows.append({
                        "Page Name": "(All Pages)",
                        "Visual/Table Name in PBI": "Report Filters",
                        "Visual Type": "reportFilter",
                        "UI Field Name": fi["property"],
                        "Usage (Visual/Filter/Slicer)": usage_str,
                        "Measure Formula": formula,
                        "Table in the Semantic Model": fi["entity"],
                        "Column in the Semantic Model": fi["property"],
                    })
            print(f"    Found {len(all_rows)} report-level filters")
        else:
            print("    No report-level filters found")
    else:
        print("    report.json not found, skipping")

    # [3] Parse report pages
    print(f"\n[3] Parsing report pages: {pages_dir}")
    if not pages_dir.is_dir():
        print(f"ERROR: Pages directory not found: {pages_dir}")
        return pd.DataFrame()

    for page_folder in sorted(pages_dir.iterdir()):
        if not page_folder.is_dir():
            continue
        page_json_path = page_folder / "page.json"
        if not page_json_path.is_file():
            continue

        page_json = json.loads(page_json_path.read_text(encoding="utf-8-sig"))
        page_name = page_json.get("displayName", page_folder.name)
        print(f"\n    Page: {page_name}")

        # Page filters
        pf_rows = parse_page_filters(page_json, page_name, measures_lookup)
        all_rows.extend(pf_rows)
        print(f"      Page filters: {len(pf_rows)}")

        # Visuals
        visuals_dir = page_folder / "visuals"
        if not visuals_dir.is_dir():
            print("      No visuals directory found")
            continue

        vis_count = 0
        vis_type_counter = Counter()

        for vis_folder in sorted(visuals_dir.iterdir()):
            if not vis_folder.is_dir():
                continue
            vis_json_path = vis_folder / "visual.json"
            if not vis_json_path.is_file():
                continue

            vis_json = json.loads(vis_json_path.read_text(encoding="utf-8-sig"))
            vis_rows = parse_visual(vis_json, page_name, measures_lookup, vis_type_counter)
            all_rows.extend(vis_rows)
            if vis_rows:
                vis_count += 1

        print(f"      Visuals with data: {vis_count}")

    # [4] Validate and remap field references against semantic model catalog
    print(f"\n[4] Validating field references against semantic model catalog")
    table_columns, column_to_tables = build_column_catalog(tables_dir)
    validated_rows = []
    remapped_count = 0
    dropped_count = 0

    for row in all_rows:
        tbl = str(row.get("Table in the Semantic Model", "")).strip()
        col = str(row.get("Column in the Semantic Model", "")).strip()
        if not tbl or not col:
            validated_rows.append(row)
            continue

        tbl_lower = tbl.lower()

        # Measure rows may have comma-separated column names (e.g. "Revenue Won, Value, Status").
        # Check if ALL individual parts exist in the catalog for this table.
        col_parts = [c.strip().lower() for c in col.split(",")]
        all_parts_valid = (
            tbl_lower in table_columns
            and all(p in table_columns[tbl_lower] for p in col_parts)
        )
        if all_parts_valid:
            validated_rows.append(row)
            continue

        # For single-column references, try to find the column in other tables
        if len(col_parts) == 1:
            col_lower = col_parts[0]
            candidate_tables = column_to_tables.get(col_lower, [])
            if candidate_tables:
                correct_table = candidate_tables[0]
                row["Table in the Semantic Model"] = correct_table
                validated_rows.append(row)
                remapped_count += 1
                print(f"    Remapped: {tbl}.{col} -> {correct_table}.{col}")
                continue

            # Column not found anywhere — stale/orphaned reference, drop it
            dropped_count += 1
            print(f"    Dropped stale reference: {tbl}.{col}")
            continue

        # Multi-column (measure) reference where some parts are invalid:
        # keep only the valid parts, remap parts found in other tables
        valid_parts = []
        row_ok = False
        for part in col_parts:
            if tbl_lower in table_columns and part in table_columns[tbl_lower]:
                valid_parts.append(part)
                row_ok = True
            elif part in column_to_tables:
                valid_parts.append(part)
                row_ok = True
        if row_ok and valid_parts:
            # Reconstruct with only valid column names
            original_parts = [c.strip() for c in col.split(",")]
            kept = [op for op in original_parts if op.strip().lower() in valid_parts]
            row["Column in the Semantic Model"] = ", ".join(kept)
            validated_rows.append(row)
        else:
            dropped_count += 1
            print(f"    Dropped stale reference: {tbl}.{col}")

    all_rows = validated_rows
    if remapped_count or dropped_count:
        print(f"    Remapped: {remapped_count}, Dropped stale: {dropped_count}")
    else:
        print(f"    All field references valid")

    # Build output DataFrame
    df = pd.DataFrame(all_rows, columns=[
        "Page Name",
        "Visual/Table Name in PBI",
        "Visual Type",
        "UI Field Name",
        "Usage (Visual/Filter/Slicer)",
        "Measure Formula",
        "Table in the Semantic Model",
        "Column in the Semantic Model",
    ])

    print(f"\n{'=' * 60}")
    print(f"Total rows extracted: {len(df)}")
    print(f"Pages: {df['Page Name'].nunique()}")
    print(f"Visuals: {df['Visual/Table Name in PBI'].nunique()}")
    print(f"{'=' * 60}")

    return df


def export_to_excel(df: pd.DataFrame, output_path: str):
    """Save metadata DataFrame to Excel with auto-sized columns."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Report Metadata", index=False)
        ws = writer.sheets["Report Metadata"]
        for col_idx, col_name in enumerate(df.columns, 1):
            max_len = max(
                len(str(col_name)),
                df[col_name].astype(str).str.len().max() if len(df) > 0 else 0,
            )
            ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max_len + 2, 60)
    print(f"\nExcel file saved to: {output_path}")


# ============================================================
# Standalone execution
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PBI AutoGov — Metadata Extractor")
    parser.add_argument("--report-root", required=True, help="Path to PBIP report definition root")
    parser.add_argument("--model-root", required=True, help="Path to semantic model definition root")
    parser.add_argument("--output", default="pbi_report_metadata.xlsx", help="Output Excel file path")
    args = parser.parse_args()

    df = extract_metadata(args.report_root, args.model_root)
    if not df.empty:
        export_to_excel(df, args.output)
    else:
        print("No data extracted. Check paths.")
