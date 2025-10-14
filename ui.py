import os
import streamlit as st
import threading
import time

# Import the NL→SQL Athena-enabled agent module (we may reload it after model changes)
import agent as agent_module


def _as_text(obj) -> str:
    """Best-effort to coerce agent results (AgentResult, dicts, lists) into a clean string."""
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj

    # If it's a dict-like structure with common fields
    if isinstance(obj, dict):
        # LLM-style chat payloads: {'role': 'assistant', 'content': [... or str ...]}
        if "content" in obj and "role" in obj:
            return _as_text(obj["content"])
        for key in ("text", "content", "message", "output", "response"):
            if key in obj:
                return _as_text(obj[key])
        # Fallback to JSON-ish string
        try:
            import json
            return json.dumps(obj, ensure_ascii=False)
        except Exception:
            return str(obj)

    # If it's a list/tuple, flatten recursively
    if isinstance(obj, (list, tuple)):
        parts = [t for t in (_as_text(x) for x in obj) if isinstance(t, str) and t]
        if not parts:
            return ""
        # Heuristic: if lots of very short pieces, likely token/char stream → join densely
        avg_len = sum(len(p) for p in parts) / max(1, len(parts))
        sep = "" if avg_len < 3 and len(parts) > 20 else "\n\n"
        return sep.join(parts)

    # Try common attributes on objects
    for attr in ("text", "content", "message", "output", "response"):
        try:
            val = getattr(obj, attr)
        except Exception:
            val = None
        if isinstance(val, str):
            return val
        if val is not None:
            return _as_text(val)

    # Fallback
    try:
        return str(obj)
    except Exception:
        return ""


def _estimate_tokens(text: str | None) -> int:
    """Very rough token estimate: ~4 chars per token.
    Returns at least 1 for non-empty strings.
    """
    if not text:
        return 0
    try:
        n = max(1, int(len(text) / 4))
    except Exception:
        n = 0
    return n


st.set_page_config(page_title="Athena Q&A – Strands Agent", page_icon="🧠", layout="wide")
st.title("Ask Athena")


def set_env_var(name: str, value: str | None):
    if value is None:
        return
    # Keep env in sync for the current Streamlit process
    if value:
        os.environ[name] = value
    elif name in os.environ:
        del os.environ[name]



# -----------------------------
# Sidebar: AWS + Athena settings
# -----------------------------
st.sidebar.header("Settings")

default_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
default_db = os.getenv("ATHENA_DATABASE", "")
default_wg = os.getenv("ATHENA_WORKGROUP", "")
default_catalog = os.getenv("ATHENA_CATALOG", "AwsDataCatalog")
default_output = os.getenv("ATHENA_OUTPUT", "")

region = st.sidebar.text_input("AWS Region", value=default_region)
database = st.sidebar.text_input("Athena Database (required)", value=default_db)
workgroup = st.sidebar.text_input("Athena Workgroup (optional)", value=default_wg)
catalog = st.sidebar.text_input("Data Catalog (optional)", value=default_catalog)
output = st.sidebar.text_input("S3 Output (optional)", value=default_output, help="e.g., s3://my-bucket/athena-results/")

set_env_var("AWS_REGION", region)
set_env_var("AWS_DEFAULT_REGION", region)
set_env_var("ATHENA_DATABASE", database)
set_env_var("ATHENA_WORKGROUP", workgroup)
set_env_var("ATHENA_CATALOG", catalog)
set_env_var("ATHENA_OUTPUT", output)

max_rows = st.sidebar.number_input("Max rows", min_value=1, max_value=10000, value=100, step=50)
fast_mode = st.sidebar.checkbox(
    "Fast mode: use provided schema only (skip discovery tools)", value=True, key="fast_mode"
)
explain_mode = st.sidebar.checkbox(
    "Add brief explanation after results", value=False, key="explain_mode"
)
display_cost = st.sidebar.checkbox(
    "Display LLM cost estimate", value=False, key="display_cost"
)

# Sidebar: Schema & Examples (fixed in sidebar like settings)

if st.sidebar.button("Reset conversation"):
    st.session_state.messages = []
    st.sidebar.success("Conversation reset.")


# -----------------------------
# Schema reference (README-style, no XML)
# -----------------------------
default_schema = (
    "#### Dataset: sample.super_store_data\n\n"
    "A retail dataset of orders, customers, products, and sales metrics.\n\n"
    "| column        | type   | description                          |\n"
    "|---------------|--------|--------------------------------------|\n"
    "| row id        | bigint | unique row identifier                 |\n"
    "| order id      | string | unique order identifier               |\n"
    "| order date    | string | order date (YYYY-MM-DD)               |\n"
    "| ship date     | string | shipment date (YYYY-MM-DD)            |\n"
    "| ship mode     | string | shipment method                       |\n"
    "| customer id   | string | unique customer identifier            |\n"
    "| customer name | string | customer full name                    |\n"
    "| segment       | string | customer segment                      |\n"
    "| country       | string | country                               |\n"
    "| city          | string | city                                  |\n"
    "| state         | string | state/province                        |\n"
    "| postal code   | bigint | postal/zip code                       |\n"
    "| region        | string | sales region                          |\n"
    "| product id    | string | unique product identifier             |\n"
    "| category      | string | product category                      |\n"
    "| sub-category  | string | product sub-category                  |\n"
    "| product name  | string | product display name                  |\n"
    "| sales         | double | sales amount                          |\n"
    "| quantity      | bigint | quantity sold                         |\n"
    "| discount      | double | discount applied (0-1)                |\n"
    "| profit        | double | profit amount                         |\n"
)

default_examples = (
    "Example queries (optional):\n\n"
    "- select count(distinct \"order id\") as \"total orders\" from \"sample\".\"super_store_data\";\n"
    "- select city, state, count(distinct \"order id\") as \"order count\"\n"
    "  from \"sample\".\"super_store_data\"\n"
    "  group by 1, 2\n"
    "  order by 3 desc;\n"
)

# Render schema in sidebar expander
with st.sidebar:
    with st.expander("Schema & Examples (preview)", expanded=False):
        st.markdown(default_schema)
        st.markdown(default_examples)

# Variables used in prompt composition
schema_text = default_schema
examples_text = default_examples

# Chat history area
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    role = message.get("role", "assistant") if isinstance(message, dict) else "assistant"
    content = message.get("content", "") if isinstance(message, dict) else str(message)
    with st.chat_message(role):
        # Re-render structured artifacts when available
        if isinstance(message, dict):
            # Prefer showing results table first for readability
            try:
                records = None
                if message.get("table_records") and isinstance(message.get("table_records"), list):
                    records = message.get("table_records")
                elif message.get("table") and isinstance(message.get("table"), dict):
                    cols = message["table"].get("columns", [])
                    rows = message["table"].get("rows", [])
                    tmp_records = []
                    for r in rows or []:
                        rec = {}
                        for i, c in enumerate(cols or []):
                            rec[c] = r[i] if i < len(r) else None
                        tmp_records.append(rec)
                    records = tmp_records
                if records:
                    st.dataframe(records, use_container_width=True)
            except Exception:
                # Fallback: show raw rows if transformation fails
                try:
                    raw_table = message.get("table")
                    if isinstance(raw_table, dict):
                        st.text(str({"columns": raw_table.get("columns"), "rows": raw_table.get("rows")[:5]}))
                except Exception:
                    pass

            # Charts next
            try:
                if message.get("vega_spec"):
                    st.vega_lite_chart(message["vega_spec"], use_container_width=True)
                if message.get("vega_specs") and isinstance(message.get("vega_specs"), list):
                    for spec in message.get("vega_specs"):
                        st.vega_lite_chart(spec, use_container_width=True)
            except Exception:
                pass

            # SQL query last
            try:
                if message.get("sql"):
                    st.code(message["sql"], language="sql")
            except Exception:
                pass

            # Cost estimate (if present)
            try:
                cost = message.get("llm_cost")
                if isinstance(cost, dict):
                    inp = int(cost.get("input_tokens", 0))
                    outp = int(cost.get("output_tokens", 0))
                    cin = (inp / 1000.0) * 0.003
                    cout = (outp / 1000.0) * 0.015
                    st.caption(
                        f"Estimated LLM cost: input {inp} tok (USD {'%0.4f' % cin}), output {outp} tok (USD {'%0.4f' % cout}), total USD {'%0.4f' % (cin+cout)}"
                    )
            except Exception:
                pass

        if content:
            st.markdown(content)

# Always place chat input at root level so Streamlit pins it to the bottom
prompt = st.chat_input("Ask a question about your Athena data…")

if prompt:
    if not database:
        st.error("Please set ATHENA_DATABASE in the sidebar before asking a question.")
    else:
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            resp_text = ""
            resp_message = None
            try:
                # Cache the agent to avoid re-instantiation overhead each turn
                if "nl_sql_agent" not in st.session_state:
                    st.session_state.nl_sql_agent = agent_module.get_agent()

                # Placeholders inside assistant bubble
                want_chart = False
                try:
                    lp = (prompt or "").lower()
                    want_chart = any(
                        w in lp
                        for w in [
                            "chart",
                            "plot",
                            "graph",
                            "visualize",
                            "visualisation",
                            "visualization",
                            "bar",
                            "line",
                            "scatter",
                            "histogram",
                        ]
                    )
                except Exception:
                    want_chart = False

                # Order: SQL -> Query -> (optional) Chart -> Formatting
                steps_list = ["Understanding request", "Generating SQL", "Running Athena query"]
                if want_chart:
                    steps_list.append("Generating chart")
                steps_list.append("Formatting answer")
                progress_placeholder = st.empty()
                sql_placeholder = st.empty()
                results_placeholder = st.empty()
                final_placeholder = st.empty()

                def render_progress(active_idx: int, done_flags=None):
                    done_flags = done_flags or [False] * len(steps_list)
                    lines = ["### Working…"]
                    for i, sname in enumerate(steps_list):
                        if done_flags[i]:
                            prefix = "✅"
                        elif i == active_idx:
                            prefix = "⏳"
                        else:
                            prefix = "•"
                        lines.append(f"{prefix} {sname}")
                    progress_placeholder.markdown("\n".join(lines))

                # --- Stage 1: Generate SQL only ---
                agent_ref = st.session_state.nl_sql_agent
                sql_holder = {"done": False, "result": None, "error": None}

                discovery_rule_sql = (
                    "Do NOT use schema discovery tools (list_athena_tables, get_athena_table_schema); use only the provided schema."
                    if fast_mode
                    else "Prefer the provided schema; use discovery tools only if necessary."
                )
                instruction_sql = (
                    "You are a precise data assistant for Amazon Athena. "
                    f"{discovery_rule_sql}\n\n"
                    "Task: Produce ONLY the final SQL to answer the user's question. "
                    "Use lowercase SQL keywords, but preserve the original casing of string literals (e.g., names like 'Sean Miller'). "
                    "When filtering on user-provided names, use case-insensitive comparison (e.g., ILIKE or lower(column)=lower('value')). Do not execute tools. "
                    "Output strictly as a fenced code block:\n\n```sql\n...\n```\n\n"
                    f"Provided schema and examples (for reference):\n{schema_text}\n{examples_text}\n\n"
                    f"Max rows to return: {max_rows}. If not specified by the user, include an appropriate LIMIT.\n"
                    "Date handling: do not cast raw strings directly to DATE. Use date(coalesce(try(date_parse(column, '%Y-%m-%d')), try(date_parse(column, '%m/%d/%Y')))) when converting string columns to dates. "
                    "When comparing against a specific date, compare to date literals (e.g., date '2016-11-08') or parse the strings first.\n"
                    f"User question: {prompt}"
                )

                # Track token estimates
                in_texts: list[str] = []
                out_texts: list[str] = []
                in_texts.append(instruction_sql)

                def _worker_sql():
                    try:
                        res = agent_ref(instruction_sql)
                        sql_holder["result"] = res
                    except Exception as _e:
                        sql_holder["error"] = str(_e)
                    finally:
                        sql_holder["done"] = True

                threading.Thread(target=_worker_sql, daemon=True).start()
                while not sql_holder["done"]:
                    render_progress(active_idx=1)
                    time.sleep(0.2)
                done_flags = [False] * len(steps_list)
                done_flags[0] = True  # Understanding request
                done_flags[1] = True  # Generating SQL
                render_progress(active_idx=1, done_flags=done_flags)
                if sql_holder["error"]:
                    raise RuntimeError(sql_holder["error"])
                sql_text_full = _as_text(sql_holder["result"]) or ""
                out_texts.append(sql_text_full)

                # Extract SQL code block or fallback to heuristic
                import re

                m = re.search(r"```sql\s*([\s\S]*?)\s*```", sql_text_full, flags=re.IGNORECASE)
                if m:
                    sql_to_run = m.group(1).strip()
                else:
                    sel = re.search(r"\bselect\b[\s\S]+", sql_text_full, flags=re.IGNORECASE)
                    sql_to_run = sel.group(0).strip() if sel else ""
                if not sql_to_run:
                    raise RuntimeError("Failed to extract SQL from the model output.")
                sql_placeholder.code(sql_to_run, language="sql")

                # --- Stage 2: Run Athena query ---
                query_holder = {"done": False, "result": None, "error": None}

                def _worker_query():
                    try:
                        res = agent_module.run_athena_query(
                            sql=sql_to_run,
                            database=database or None,
                            workgroup=workgroup or None,
                            output_location=output or None,
                            region_name=region or None,
                            max_rows=max_rows,
                        )
                        query_holder["result"] = res
                    except Exception as _e:
                        query_holder["error"] = str(_e)
                    finally:
                        query_holder["done"] = True

                threading.Thread(target=_worker_query, daemon=True).start()
                while not query_holder["done"]:
                    idx = steps_list.index("Running Athena query")
                    render_progress(active_idx=idx, done_flags=done_flags)
                    time.sleep(0.25)
                if query_holder["error"]:
                    raise RuntimeError(query_holder["error"])

                # Show results table
                res = query_holder["result"] or {}
                cols = res.get("columns", [])
                rows = res.get("rows", [])
                records = []
                for r in rows:
                    rec = {}
                    for i, c in enumerate(cols):
                        rec[c] = r[i] if i < len(r) else None
                    records.append(rec)
                if records:
                    results_placeholder.dataframe(records, use_container_width=True)
                else:
                    results_placeholder.info("No rows returned.")
                done_flags[steps_list.index("Running Athena query")] = True

                # --- Optional Stage: Generate chart locally from results ---
                chart_spec = None
                if want_chart and records:
                    try:
                        sample = records[0]
                        numeric_candidates = []
                        for k, v in sample.items():
                            try:
                                float(v)
                                numeric_candidates.append(k)
                            except Exception:
                                pass
                        preferred = [
                            "sales",
                            "amount",
                            "revenue",
                            "profit",
                            "count",
                            "order count",
                            "quantity",
                            "total",
                        ]
                        y_field = None
                        for p in preferred:
                            for k in records[0].keys():
                                if k.lower() == p:
                                    y_field = k
                                    break
                            if y_field:
                                break
                        if not y_field and numeric_candidates:
                            y_field = numeric_candidates[0]
                        x_field = None
                        for k in records[0].keys():
                            if k != y_field:
                                x_field = k
                                break
                        if x_field and y_field:
                            chart_spec = {
                                "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                                "data": {"values": records},
                                "mark": "bar",
                                "encoding": {
                                    "x": {"field": x_field, "type": "nominal", "sort": "-y"},
                                    "y": {"field": y_field, "type": "quantitative"},
                                },
                            }
                            st.vega_lite_chart(chart_spec, use_container_width=True)
                            if "Generating chart" in steps_list:
                                done_flags[steps_list.index("Generating chart")] = True
                    except Exception:
                        pass

                render_progress(active_idx=steps_list.index("Formatting answer"), done_flags=done_flags)

                # --- Stage 3: Optional concise explanation ---
                final_text = ""
                if explain_mode:
                    explain_holder = {"done": False, "result": None, "error": None}
                    instruction_explain = (
                        "Provide a concise 2-4 bullet explanation of the results based on the SQL and returned data. "
                        "Avoid repeating the full table. Include the executed SQL in a code block at the end.\n\n"
                        f"SQL:\n```sql\n{sql_to_run}\n```\n"
                    )

                    def _worker_explain():
                        try:
                            res = agent_ref(instruction_explain)
                            explain_holder["result"] = res
                        except Exception as _e:
                            explain_holder["error"] = str(_e)
                        finally:
                            explain_holder["done"] = True

                    threading.Thread(target=_worker_explain, daemon=True).start()
                    while not explain_holder["done"]:
                        render_progress(active_idx=steps_list.index("Formatting answer"), done_flags=done_flags)
                        time.sleep(0.25)
                    if explain_holder["error"]:
                        raise RuntimeError(explain_holder["error"])
                    final_text = _as_text(explain_holder["result"]) or ""
                    in_texts.append(instruction_explain)
                    out_texts.append(final_text)
                    final_placeholder.markdown(final_text)
                done_flags[steps_list.index("Formatting answer")] = True

                progress_placeholder.markdown("\n".join(["### Completed"] + [f"✅ {s}" for s in steps_list]))

                llm_cost = None
                if display_cost:
                    in_tokens = sum(_estimate_tokens(t) for t in in_texts)
                    out_tokens = sum(_estimate_tokens(t) for t in out_texts)
                    cost_in = (in_tokens / 1000.0) * 0.003
                    cost_out = (out_tokens / 1000.0) * 0.015
                    total_cost = cost_in + cost_out
                    cost_line = (
                        f"Estimated LLM cost: input {in_tokens} tok (USD {'%0.4f' % cost_in}), "
                        f"output {out_tokens} tok (USD {'%0.4f' % cost_out}), total USD {'%0.4f' % total_cost}"
                    )
                    st.caption(cost_line)
                    llm_cost = {
                        "input_tokens": in_tokens,
                        "output_tokens": out_tokens,
                        "input_cost": float(f"{cost_in:.6f}"),
                        "output_cost": float(f"{cost_out:.6f}"),
                    }

                combined = f"Results shown above.\n\n```sql\n{sql_to_run}\n```\n\n" + (final_text or "")
                resp_text = combined
                table_records = []
                for r in rows:
                    rec = {}
                    for i, c in enumerate(cols):
                        rec[c] = r[i] if i < len(r) else None
                    table_records.append(rec)
                resp_message = {
                    "role": "assistant",
                    "content": resp_text,
                    "sql": sql_to_run,
                    "table": {"columns": cols, "rows": rows},
                    "table_records": table_records,
                }
                if chart_spec is not None:
                    resp_message["vega_spec"] = chart_spec
                if display_cost and llm_cost is not None:
                    resp_message["llm_cost"] = llm_cost
            except Exception as e:
                resp_text = f"Error: {e}"
                st.error(resp_text)
                resp_message = {"role": "assistant", "content": resp_text}

        st.session_state.messages.append(resp_message or {"role": "assistant", "content": resp_text})
