"""Amazon Athena helpers and Strand tools used by the GenAI agent."""

from __future__ import annotations

import os
import time
from typing import Dict, List, Optional, Tuple

import boto3
import botocore

_CLIENT_CACHE: dict[Tuple[str, str, str], Tuple] = {}


def _athena_clients(region_name: Optional[str] = None):
    region = region_name or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if not region:
        raise RuntimeError("AWS region not configured. Set AWS_REGION or AWS_DEFAULT_REGION.")
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


def list_athena_tables(
    database: Optional[str] = None,
    catalog: Optional[str] = None,
    region_name: Optional[str] = None,
) -> List[str]:
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


def get_athena_table_schema(
    table: str,
    database: Optional[str] = None,
    catalog: Optional[str] = None,
    region_name: Optional[str] = None,
) -> Dict[str, str]:
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


def run_athena_query(
    sql: str,
    database: Optional[str] = None,
    workgroup: Optional[str] = None,
    output_location: Optional[str] = None,
    region_name: Optional[str] = None,
    max_rows: int = 100,
) -> Dict:
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
            if data == cols:
                continue
            rows.append(data)
            count += 1
            if count >= max_rows:
                break
        if count >= max_rows:
            break

    return {"columns": cols, "rows": rows, "query_execution_id": qid}


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
        rec[x] = r[ix] if ix < len(r) else None
        val_y = r[iy] if iy < len(r) else None
        if isinstance(val_y, str):
            try:
                rec[y] = float(val_y) if "." in val_y else int(val_y)
            except Exception:
                rec[y] = val_y
        else:
            rec[y] = val_y
        if ic is not None:
            rec[color] = r[ic] if ic < len(r) else None
        records.append(rec)

    enc_y = {"field": y, "type": "quantitative"}
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


__all__ = [
    "list_athena_tables",
    "get_athena_table_schema",
    "run_athena_query",
    "plot_vegalite_from_query",
]

