"""Utility helpers shared across Streamlit UI components."""

from __future__ import annotations

import json
from typing import Any, Iterable


def as_text(obj: Any) -> str:
    """Best-effort coercion of agent responses to displayable text."""
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        if "content" in obj and "role" in obj:
            return as_text(obj["content"])
        for key in ("text", "content", "message", "output", "response"):
            if key in obj:
                return as_text(obj[key])
        try:
            return json.dumps(obj, ensure_ascii=False)
        except Exception:
            return str(obj)
    if isinstance(obj, (list, tuple)):
        flattened = [as_text(item) for item in obj]
        parts = [item for item in flattened if isinstance(item, str) and item]
        if not parts:
            return ""
        avg_len = sum(len(part) for part in parts) / max(1, len(parts))
        separator = "" if avg_len < 3 and len(parts) > 20 else "\n\n"
        return separator.join(parts)

    for attr in ("text", "content", "message", "output", "response"):
        try:
            value = getattr(obj, attr)
        except Exception:
            value = None
        if isinstance(value, str):
            return value
        if value is not None:
            return as_text(value)
    return str(obj)


def estimate_tokens(text: str | None) -> int:
    """Rough token estimate assuming ~4 chars per token."""
    if not text:
        return 0
    try:
        return max(1, int(len(text) / 4))
    except Exception:
        return 0


def records_from_table(columns: Iterable[str], rows: Iterable[Iterable[str]]):
    """Convert Athena column/row structures into Streamlit-friendly records."""
    records = []
    column_list = list(columns)
    for row in rows:
        record = {}
        row_list = list(row)
        for idx, column in enumerate(column_list):
            record[column] = row_list[idx] if idx < len(row_list) else None
        records.append(record)
    return records


__all__ = ["as_text", "estimate_tokens", "records_from_table"]

