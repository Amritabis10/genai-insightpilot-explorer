"""Configuration helpers for environment-backed Athena settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AthenaSettings:
    """Normalized collection of Athena-related environment values."""

    region: str
    database: str
    workgroup: Optional[str]
    catalog: str
    output: Optional[str]

    def apply(self) -> None:
        """Sync the settings back to environment variables used by the app."""
        set_env_var("AWS_REGION", self.region)
        set_env_var("AWS_DEFAULT_REGION", self.region)
        set_env_var("ATHENA_DATABASE", self.database)
        set_env_var("ATHENA_WORKGROUP", self.workgroup)
        set_env_var("ATHENA_CATALOG", self.catalog)
        set_env_var("ATHENA_OUTPUT", self.output)


def set_env_var(name: str, value: Optional[str]) -> None:
    """Update or delete an environment variable in the current process."""
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def load_athena_settings() -> AthenaSettings:
    """Load Athena configuration from the environment with sensible defaults."""
    region = (
        os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    database = os.getenv("ATHENA_DATABASE") or "super_store_data"
    workgroup = os.getenv("ATHENA_WORKGROUP") or None
    catalog = os.getenv("ATHENA_CATALOG") or "AwsDataCatalog"
    output = os.getenv("ATHENA_OUTPUT") or None
    settings = AthenaSettings(
        region=region,
        database=database,
        workgroup=workgroup,
        catalog=catalog,
        output=output,
    )
    settings.apply()
    return settings


__all__ = ["AthenaSettings", "load_athena_settings", "set_env_var"]

