"""Streamlit entrypoint delegating to the modularized application."""

from __future__ import annotations

from src.app import run_app


def main() -> None:
    run_app()


if __name__ == "__main__":
    main()

