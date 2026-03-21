"""Secret resolution — env var first, GCP Secret Manager fallback."""

import logging
import os

logger = logging.getLogger(__name__)


def get_secret(name: str, project_id: str | None = None) -> str:
    """Resolve a secret value.

    Resolution order:
      1. Environment variable with the same *name*.
      2. GCP Secret Manager (requires ``google-cloud-secret-manager``).

    Args:
        name: Secret / env-var name
        project_id: GCP project. Falls back to ``GCP_PROJECT_ID`` env var

    Raises:
        ValueError: If the secret cannot be resolved from any source
    """
    value = os.getenv(name)
    if value:
        logger.debug("Secret '%s' resolved from environment variable", name)
        return value

    project_id = project_id or os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError(
            f"Secret '{name}' not found in env vars and GCP_PROJECT_ID is not set"
        )

    try:
        from google.cloud import secretmanager  # noqa: PLC0415

        client = secretmanager.SecretManagerServiceClient()
        resource = f"projects/{project_id}/secrets/{name}/versions/latest"
        response = client.access_secret_version(request={"name": resource})
        logger.info("Secret '%s' resolved from GCP Secret Manager", name)
        return response.payload.data.decode("UTF-8")
    except Exception:
        logger.exception("Failed to resolve secret '%s'", name)
        raise
