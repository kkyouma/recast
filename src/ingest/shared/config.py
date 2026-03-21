"""Configuration helpers for environment-driven jobs."""

import os


def require_env(name: str, default: str | None = None) -> str:
    """Read a required environment variable or raise with a clear message.

    Args:
        name: Environment variable name
        default: Optional fallback value
    """
    value = os.getenv(name, default)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value
