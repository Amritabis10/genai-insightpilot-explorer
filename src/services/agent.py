"""Agent factory and tool definitions for the Streamlit application."""

from __future__ import annotations

import os
from typing import Optional

from strands import Agent, tool
from strands_tools import calculator, current_time

from . import athena


@tool
def letter_counter(word: str, letter: str) -> int:
    """Count occurrences of a specific letter in a word."""
    if not isinstance(word, str) or not isinstance(letter, str):
        return 0
    if len(letter) != 1:
        raise ValueError("The 'letter' parameter must be a single character")
    return word.lower().count(letter.lower())


@tool
def list_athena_tables(
    database: Optional[str] = None,
    catalog: Optional[str] = None,
    region_name: Optional[str] = None,
):
    return athena.list_athena_tables(
        database=database,
        catalog=catalog,
        region_name=region_name,
    )


@tool
def get_athena_table_schema(
    table: str,
    database: Optional[str] = None,
    catalog: Optional[str] = None,
    region_name: Optional[str] = None,
):
    return athena.get_athena_table_schema(
        table=table,
        database=database,
        catalog=catalog,
        region_name=region_name,
    )


@tool
def run_athena_query(
    sql: str,
    database: Optional[str] = None,
    workgroup: Optional[str] = None,
    output_location: Optional[str] = None,
    region_name: Optional[str] = None,
    max_rows: int = 100,
):
    return athena.run_athena_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        output_location=output_location,
        region_name=region_name,
        max_rows=max_rows,
    )


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
):
    return athena.plot_vegalite_from_query(
        sql=sql,
        x=x,
        y=y,
        mark=mark,
        color=color,
        title=title,
        database=database,
        workgroup=workgroup,
        output_location=output_location,
        region_name=region_name,
        max_rows=max_rows,
    )


def _create_agent_instance(tools):
    """Create a strands Agent, honoring STRANDS_MODEL if supported."""
    selected_model = os.getenv("STRANDS_MODEL")
    if selected_model:
        try:
            return Agent(tools=tools, model=selected_model)
        except TypeError:
            pass
    return Agent(tools=tools)


def get_agent():
    """Return a configured strands Agent instance with Athena tools."""
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


__all__ = [
    "get_agent",
    "letter_counter",
    "list_athena_tables",
    "get_athena_table_schema",
    "run_athena_query",
    "plot_vegalite_from_query",
]

