"""Backwards-compatible shim exposing the agent services package."""

from __future__ import annotations

from src.services.agent import (
    get_agent,
    get_athena_table_schema,
    letter_counter,
    list_athena_tables,
    plot_vegalite_from_query,
    run_athena_query,
)

# Eagerly create a shared agent instance to mimic previous behaviour.
agent = get_agent()

__all__ = [
    "agent",
    "get_agent",
    "letter_counter",
    "list_athena_tables",
    "get_athena_table_schema",
    "run_athena_query",
    "plot_vegalite_from_query",
]

