import os
import time
from typing import List, Dict, Optional

import boto3
import botocore

from strands import Agent, tool
from strands_tools import calculator, current_time

# Define a custom tool as a Python function using the @tool decorator
@tool
def letter_counter(word: str, letter: str) -> int:
    """
    Count occurrences of a specific letter in a word.

    Args:
        word (str): The input word to search in
        letter (str): The specific letter to count

    Returns:
        int: The number of occurrences of the letter in the word
    """
    if not isinstance(word, str) or not isinstance(letter, str):
        return 0

    if len(letter) != 1:
        raise ValueError("The 'letter' parameter must be a single character")

    return word.lower().count(letter.lower())

def _create_agent_instance(tools):
    """Create a strands Agent, honoring STRANDS_MODEL if supported by this version.
    Falls back to default constructor if 'model' isn't accepted.
    """
    selected_model = os.getenv("STRANDS_MODEL")
    if selected_model:
        try:
            return Agent(tools=tools, model=selected_model)
        except TypeError:
            # Older versions may not support 'model' kwarg
            pass
    return Agent(tools=tools)

"""
Amazon Athena tools
These tools let the agent discover schema and run SQL on Athena.
Set environment variables for defaults when not passed explicitly:
 - ATHENA_DATABASE (required unless provided in calls)
 - ATHENA_WORKGROUP (optional; falls back to workgroup default)
 - ATHENA_CATALOG (optional; default 'AwsDataCatalog')
 - ATHENA_OUTPUT (optional; S3 path, used if workgroup lacks one)
 - AWS_REGION / AWS_DEFAULT_REGION (required by boto3 unless configured otherwise)
"""


_CLIENT_CACHE: dict = {}


def _athena_clients(region_name: Optional[str] = None):
    region = region_name or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if not region:
        raise RuntimeError("AWS region not configured. Set AWS_REGION or AWS_DEFAULT_REGION.")
    # Reuse clients per-region to avoid setup overhead on each tool call
    cache_key = ("athena", "glue", region)
    clients = _CLIENT_CACHE.get(cache_key)
    if clients is None:
        clients = (
            boto3.client("athena", region_name=region),
            boto3.client("glue", region_name=region),
        )
        _CLIENT_CACHE[cache_key] = clients
    return clients


def _wait_for_query(athena, query_execution_id: str, timeout_s: int = 120, poll_s: float = 1.5):
    start = time.time()
    while True:
        resp = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state, resp
        if time.time() - start > timeout_s:
            raise TimeoutError(f"Athena query timed out after {timeout_s}s: {query_execution_id}")
        time.sleep(poll_s)


@tool
def list_athena_tables(
    database: Optional[str] = None,
    catalog: Optional[str] = None,
    region_name: Optional[str] = None,
) -> List[str]:
    """
    List tables in an Athena database (via Glue Data Catalog).

    Args:
        database: Glue/Athena database name. Defaults to env ATHENA_DATABASE.
        catalog: Data catalog name. Defaults to env ATHENA_CATALOG or 'AwsDataCatalog'.
        region_name: AWS region. Defaults to AWS_REGION/AWS_DEFAULT_REGION.

    Returns:
        List of table names.
    """
    db = database or os.getenv("ATHENA_DATABASE")
    if not db:
        raise ValueError("database is required (or set ATHENA_DATABASE)")
    cat = catalog or os.getenv("ATHENA_CATALOG") or "AwsDataCatalog"
    _, glue = _athena_clients(region_name)
    tables: List[str] = []
    paginator = glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=db, CatalogId=None):
        for t in page.get("TableList", []):
            tables.append(t.get("Name"))
    return tables


@tool
def get_athena_table_schema(
    table: str,
    database: Optional[str] = None,
    catalog: Optional[str] = None,
    region_name: Optional[str] = None,
) -> Dict[str, str]:
    """
    Get column names and types for a table from the Glue Data Catalog.

    Args:
        table: Table name within the database.
        database: Database name. Defaults to env ATHENA_DATABASE.
        catalog: Data catalog. Defaults to env ATHENA_CATALOG or 'AwsDataCatalog'.
        region_name: AWS region.

    Returns:
        Mapping of column_name -> data_type.
    """
    db = database or os.getenv("ATHENA_DATABASE")
    if not db:
        raise ValueError("database is required (or set ATHENA_DATABASE)")
    cat = catalog or os.getenv("ATHENA_CATALOG") or "AwsDataCatalog"
    _, glue = _athena_clients(region_name)
    try:
        res = glue.get_table(DatabaseName=db, Name=table)
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"Glue get_table failed: {e}")
    cols = {}
    for c in res.get("Table", {}).get("StorageDescriptor", {}).get("Columns", []):
        cols[c.get("Name")] = c.get("Type")
    for c in res.get("Table", {}).get("PartitionKeys", []):
        cols[c.get("Name")] = c.get("Type")
    return cols


@tool
def run_athena_query(
    sql: str,
    database: Optional[str] = None,
    workgroup: Optional[str] = None,
    output_location: Optional[str] = None,
    region_name: Optional[str] = None,
    max_rows: int = 100,
) -> Dict:
    """
    Execute a SQL statement in Amazon Athena and return rows.

    Notes for the model:
      - Use Presto/Trino-compatible SQL; qualify with the database if needed.
      - Use LIMIT to keep results concise unless user requests otherwise.
      - Call list_athena_tables/get_athena_table_schema to discover schema before writing SQL.

    Args:
        sql: The SQL query to execute.
        database: Athena/Glue database. Defaults to env ATHENA_DATABASE.
        workgroup: Athena workgroup. Defaults to env ATHENA_WORKGROUP.
        output_location: S3 path for results if workgroup lacks output.
        region_name: AWS region.
        max_rows: Max rows to return (server may return more; we truncate client-side).

    Returns:
        Dict with keys: columns (List[str]), rows (List[List[str]]), query_execution_id (str)
    """
    if not sql or not isinstance(sql, str):
        raise ValueError("sql is required")

    db = database or os.getenv("ATHENA_DATABASE")
    if not db:
        raise ValueError("database is required (or set ATHENA_DATABASE)")
    wg = workgroup or os.getenv("ATHENA_WORKGROUP")
    out = output_location or os.getenv("ATHENA_OUTPUT")

    athena, _ = _athena_clients(region_name)
    kwargs = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": db},
    }
    if wg:
        kwargs["WorkGroup"] = wg
    if out:
        kwargs["ResultConfiguration"] = {"OutputLocation": out}

    try:
        start_resp = athena.start_query_execution(**kwargs)
        qid = start_resp["QueryExecutionId"]
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"Failed to start query: {e}")

    state, exec_resp = _wait_for_query(athena, qid)
    if state != "SUCCEEDED":
        reason = exec_resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        raise RuntimeError(f"Athena query {state}: {reason}")

    # Fetch results
    cols: List[str] = []
    rows: List[List[str]] = []
    paginator = athena.get_paginator("get_query_results")
    count = 0
    first_page = True
    for page in paginator.paginate(QueryExecutionId=qid):
        result_set = page.get("ResultSet", {})
        rs_cols = result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])
        if first_page:
            cols = [c.get("Name") for c in rs_cols]
            first_page = False
        for row in result_set.get("Rows", []):
            data = [c.get("VarCharValue", "") for c in row.get("Data", [])]
            # Skip header row (Athena includes headers as first row)
            if data == cols:
                continue
            rows.append(data)
            count += 1
            if count >= max_rows:
                break
        if count >= max_rows:
            break

    return {"columns": cols, "rows": rows, "query_execution_id": qid}


@tool
def plot_vegalite_from_query(
    sql: str,
    x: str,
    y: str,
    mark: str = "bar",
    color: Optional[str] = None,
    title: Optional[str] = None,
    database: Optional[str] = None,
    workgroup: Optional[str] = None,
    output_location: Optional[str] = None,
    region_name: Optional[str] = None,
    max_rows: int = 500,
) -> str:
    """
    Run a SQL query in Athena and return a Vega-Lite chart spec as a fenced code block.

    The returned string is formatted as:
    ```vega-lite
    { ...json spec... }
    ```

    Args:
        sql: SQL to execute. Use lowercase SQL keywords, but preserve string literal casing. Include LIMIT when appropriate.
        x: Column to use for the x-axis.
        y: Column to use for the y-axis.
        mark: One of bar, line, area, point.
        color: Optional column to color/segment by.
        title: Optional chart title.
        database, workgroup, output_location, region_name: Athena settings.
        max_rows: Maximum rows to embed in the chart (smaller is faster).

    Returns:
        A string containing a vega-lite fenced code block suitable for rendering by the UI.
    """
    res = run_athena_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        output_location=output_location,
        region_name=region_name,
        max_rows=max_rows,
    )
    cols = res.get("columns", [])
    rows = res.get("rows", [])
    # Build compact records with only x, y and optional color to reduce JSON size
    def _idx(col_name: Optional[str]) -> Optional[int]:
        if not col_name:
            return None
        try:
            return cols.index(col_name)
        except ValueError:
            lower_map = {c.lower(): i for i, c in enumerate(cols)}
            return lower_map.get(col_name.lower())

    ix = _idx(x) or 0
    iy = _idx(y) or 0
    ic = _idx(color)

    records = []
    for r in rows:
        rec = {}
        # x as-is (categorical/nominal by default)
        rec[x] = r[ix] if ix < len(r) else None
        # y -> attempt numeric conversion only for y
        val_y = r[iy] if iy < len(r) else None
        if isinstance(val_y, str):
            try:
                rec[y] = float(val_y) if "." in val_y else int(val_y)
            except Exception:
                rec[y] = val_y
        else:
            rec[y] = val_y
        # optional color field (kept as nominal)
        if ic is not None:
            rec[color] = r[ic] if ic < len(r) else None
        records.append(rec)

    enc_y = {"field": y, "type": "quantitative"}
    # If y looks like a non-numeric string, render as nominal
    try:
        _ = float(records[0][y]) if records and y in records[0] else None
    except Exception:
        enc_y["type"] = "nominal"

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": records},
        "mark": mark,
        "encoding": {
            "x": {"field": x, "type": "nominal", "sort": "-y"},
            "y": enc_y,
        },
    }
    if color:
        spec["encoding"]["color"] = {"field": color, "type": "nominal"}
    if title:
        spec["title"] = title

    import json
    return "```vega-lite\n" + json.dumps(spec, ensure_ascii=False) + "\n```"


def get_agent():
    """Return a configured strands Agent instance with Athena tools.
    Reads STRANDS_MODEL to select a smaller/faster model when supported.
    """
    tools = [
        calculator,
        current_time,
        letter_counter,
        list_athena_tables,
        get_athena_table_schema,
        run_athena_query,
        plot_vegalite_from_query,
    ]
    return _create_agent_instance(tools)


# Backwards-compatibility: expose a default agent instance
agent = get_agent()
