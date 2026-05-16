"""Configuration helpers for environment-driven jobs."""

import os


def require_env(name: str) -> str:
    """Read a required environment variable or raise with a clear message.

    Args:
        name: Environment variable name

    Raises:
        ValueError: If the environment variable is not set
    """
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_env(name: str, default: str) -> str:
    """Read an optional environment variable with a default fallback.

    Args:
        name: Environment variable name
        default: Fallback value if the variable is not set
    """
    return os.getenv(name, default)
