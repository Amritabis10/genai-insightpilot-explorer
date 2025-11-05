"""Main Streamlit application entrypoint with modularized components."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional

import streamlit as st
from PIL import Image

from .constants import DATASET_URL, DEFAULT_EXAMPLES, DEFAULT_SCHEMA
from .services import agent as agent_services
from .settings import load_athena_settings
from .ui.helpers import as_text, estimate_tokens, records_from_table


PAGE_TITLE = "GenAI InsightPilot Explorer"
PAGE_ICON_PATH = Path("assets/genai.png")
MAX_ROWS = 100
FAST_MODE = True
DISPLAY_COST = True

def run_app() -> None:
    """Streamlit entry point."""
    settings = load_athena_settings()
    _configure_page()
    _render_sidebar()
    _ensure_session_defaults()
    _render_history()

    prompt = st.chat_input("Ask a question about Superstore Dataset…")
    if not prompt:
        return
    if not settings.database:
        st.error("Please set ATHENA_DATABASE in the sidebar before asking a question.")
        return

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            response = _handle_prompt(prompt=prompt, settings=settings)
        except Exception as exc:
            message = f"Error: {exc}"
            st.error(message)
            response = {"role": "assistant", "content": message}
    st.session_state.messages.append(response)


def _configure_page() -> None:
    icon = Image.open(PAGE_ICON_PATH)
    st.set_page_config(page_title=PAGE_TITLE, page_icon=icon, layout="wide")
    st.markdown(f"### Enquire about [Superstore Dataset]({DATASET_URL}) in Natural Language")


def _render_sidebar() -> None:
    with st.sidebar:
        st.markdown("### Schema & Examples")
        st.markdown(f"[Dataset source]({DATASET_URL})")
        st.markdown(DEFAULT_EXAMPLES)
        st.markdown(DEFAULT_SCHEMA)


def _ensure_session_defaults() -> None:
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "nl_sql_agent" not in st.session_state:
        st.session_state.nl_sql_agent = agent_services.get_agent()


def _render_history() -> None:
    for message in st.session_state.messages:
        role = message.get("role", "assistant")
        content = message.get("content", "")
        with st.chat_message(role):
            if role == "assistant":
                _render_assistant_payload(message)
            if content:
                st.markdown(content)


def _render_assistant_payload(message: Dict) -> None:
    records = message.get("table_records")
    if records:
        st.dataframe(records, use_container_width=True)

    vega_spec = message.get("vega_spec")
    if vega_spec:
        st.vega_lite_chart(vega_spec, use_container_width=True)

    if message.get("sql"):
        st.code(message["sql"], language="sql")

    cost = message.get("llm_cost")
    if isinstance(cost, dict):
        inp = int(cost.get("input_tokens", 0))
        outp = int(cost.get("output_tokens", 0))
        cin = (inp / 1000.0) * 0.003
        cout = (outp / 1000.0) * 0.015
        st.caption(
            f"Estimated LLM cost: input {inp} tok (USD {'%0.4f' % cin}), "
            f"output {outp} tok (USD {'%0.4f' % cout}), total USD {'%0.4f' % (cin + cout)}"
        )


def _handle_prompt(prompt: str, settings) -> Dict:
    agent = st.session_state.nl_sql_agent
    chart_requested = _wants_chart(prompt)

    instructions: List[str] = []
    outputs: List[str] = []

    sql_instruction = _build_sql_instruction(prompt)
    instructions.append(sql_instruction)

    with st.spinner("Generating SQL..."):
        sql_raw = agent(sql_instruction)
    sql_text_full = as_text(sql_raw)
    outputs.append(sql_text_full)

    sql_to_run = _extract_sql(sql_text_full)
    if not sql_to_run:
        raise RuntimeError("Failed to extract SQL from the model output.")
    st.code(sql_to_run, language="sql")

    with st.spinner("Running Athena query..."):
        query_result = agent_services.run_athena_query(
            sql=sql_to_run,
            database=settings.database,
            workgroup=settings.workgroup,
            output_location=settings.output,
            region_name=settings.region,
            max_rows=MAX_ROWS,
        )

    columns = query_result.get("columns", [])
    rows = query_result.get("rows", [])
    records = records_from_table(columns, rows)
    if records:
        st.dataframe(records, use_container_width=True)
    else:
        st.info("No rows returned.")

    chart_spec = _build_chart(records) if chart_requested and records else None
    if chart_spec:
        st.vega_lite_chart(chart_spec, use_container_width=True)

    llm_cost = None
    if DISPLAY_COST:
        in_tokens = sum(estimate_tokens(text) for text in instructions)
        out_tokens = sum(estimate_tokens(text) for text in outputs)
        llm_cost = {
            "input_tokens": in_tokens,
            "output_tokens": out_tokens,
            "input_cost": float(f"{(in_tokens / 1000.0) * 0.003:.6f}"),
            "output_cost": float(f"{(out_tokens / 1000.0) * 0.015:.6f}"),
        }
        st.caption(
            f"Estimated LLM cost: input {in_tokens} tok, "
            f"output {out_tokens} tok, total USD {(float(llm_cost['input_cost']) + float(llm_cost['output_cost'])):.4f}"
        )

    response_text = f"Results shown above.\n\n```sql\n{sql_to_run}\n```"

    response = {
        "role": "assistant",
        "content": response_text,
        "sql": sql_to_run,
        "table": {"columns": columns, "rows": rows},
        "table_records": records,
    }
    if chart_spec:
        response["vega_spec"] = chart_spec
    if llm_cost:
        response["llm_cost"] = llm_cost
    return response


def _build_sql_instruction(prompt: str) -> str:
    discovery_rule_sql = (
        "Do NOT use schema discovery tools (list_athena_tables, get_athena_table_schema); use only the provided schema."
        if FAST_MODE
        else "Prefer the provided schema; use discovery tools only if necessary."
    )
    return (
        "You are a precise data assistant for Amazon Athena. "
        f"{discovery_rule_sql}\n\n"
        "Task: Produce ONLY the final SQL to answer the user's question. "
        "Use lowercase SQL keywords, but preserve the original casing of string literals (e.g., names like 'Sean Miller'). "
        "When filtering on user-provided names, use case-insensitive comparison (e.g., ILIKE or lower(column)=lower('value')). "
        "Do not execute tools. Output strictly as a fenced code block:\n\n```sql\n...\n```\n\n"
        f"Provided schema and examples (for reference):\n{DEFAULT_SCHEMA}\n{DEFAULT_EXAMPLES}\n\n"
        f"Max rows to return: {MAX_ROWS}. If not specified by the user, include an appropriate LIMIT.\n"
        "Date handling: do not cast raw strings directly to DATE. Use date(coalesce(try(date_parse(column, '%Y-%m-%d')), "
        "try(date_parse(column, '%m/%d/%Y')))) when converting string columns to dates. When comparing against a specific date, "
        "compare to date literals (e.g., date '2016-11-08') or parse the strings first.\n"
        f"User question: {prompt}"
    )


def _extract_sql(raw_text: str) -> str:
    match = re.search(r"```sql\s*([\s\S]*?)\s*```", raw_text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    fallback = re.search(r"\bselect\b[\s\S]+", raw_text, flags=re.IGNORECASE)
    return fallback.group(0).strip() if fallback else ""


def _wants_chart(prompt: str) -> bool:
    lowered = (prompt or "").lower()
    keywords = [
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
    return any(word in lowered for word in keywords)


def _build_chart(records: List[Dict]) -> Optional[Dict]:
    if not records:
        return None
    sample = records[0]
    numeric_candidates = []
    for key, value in sample.items():
        try:
            float(value)
            numeric_candidates.append(key)
        except Exception:
            continue

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
    for candidate in preferred:
        for key in sample.keys():
            if key.lower() == candidate:
                y_field = key
                break
        if y_field:
            break
    if not y_field and numeric_candidates:
        y_field = numeric_candidates[0]

    x_field = None
    for key in sample.keys():
        if key != y_field:
            x_field = key
            break

    if not x_field or not y_field:
        return None

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": records},
        "mark": "bar",
        "encoding": {
            "x": {"field": x_field, "type": "nominal", "sort": "-y"},
            "y": {"field": y_field, "type": "quantitative"},
        },
    }
    return spec


__all__ = ["run_app"]
