# ─────────────────────────────────────────────────────────────────────────────
# justfile — Energy Project command runner
# Usage: just <recipe>  |  just --list
# ─────────────────────────────────────────────────────────────────────────────

set dotenv-load  # Carga .env → expone CEN_AUTH_TOKEN

# ── Infra config (no son secretos, sí son específicos de este entorno) ────────
GCP_PROJECT_ID := "project-cb026a3b-da35-4742-a36"
GCS_BUCKET     := "recast-landing-project-cb026a3b-da35-4742-a36"

# ── Colector CEN ──────────────────────────────────────────────────────────────
GCS_PREFIX_CEN := "cen_api/generacion-real"

# ── Docker ────────────────────────────────────────────────────────────────────
IMAGE_NAME := "energy-ingest"
IMAGE_TAG  := "latest"

# ── Fechas de prueba (rango corto para iteración rápida) ─────────────────────
TEST_START := "2026-02-01"
TEST_END   := "2026-02-03"

# ─────────────────────────────────────────────────────────────────────────────
# Default: muestra todos los comandos disponibles
# ─────────────────────────────────────────────────────────────────────────────
_default:
    @just --list --unsorted


# ─────────────────────────────────────────────────────────────────────────────
# DEV LOCAL — SINK=local, sin Docker
# ─────────────────────────────────────────────────────────────────────────────

# Ejecuta el colector CEN y guarda los datos en data/raw/ (local)
run-local start=TEST_START end=TEST_END:
    SINK=local \
    START_DATE={{start}} \
    END_DATE={{end}} \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# ─────────────────────────────────────────────────────────────────────────────
# GCS DIRECTO — SINK=gcs, sin Docker (usa ADC del host)
# ─────────────────────────────────────────────────────────────────────────────

# Ejecuta el colector CEN y escribe en GCS usando tus credenciales locales
run-gcs start=TEST_START end=TEST_END:
    SINK=gcs \
    GCP_PROJECT_ID={{GCP_PROJECT_ID}} \
    GCS_BUCKET={{GCS_BUCKET}} \
    GCS_PREFIX={{GCS_PREFIX_CEN}} \
    START_DATE={{start}} \
    END_DATE={{end}} \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# ─────────────────────────────────────────────────────────────────────────────
# DOCKER — Ciclo completo de construcción y ejecución
# ─────────────────────────────────────────────────────────────────────────────

# Construye la imagen Docker
docker-build:
    docker build -t {{IMAGE_NAME}}:{{IMAGE_TAG}} .

# Corre el contenedor con SINK=local — monta ./data para ver los archivos
docker-run-local start=TEST_START end=TEST_END:
    docker run --rm \
      -v "$(pwd)/data:/app/data" \
      -e SINK=local \
      -e START_DATE={{start}} \
      -e END_DATE={{end}} \
      -e CEN_AUTH_TOKEN="${CEN_AUTH_TOKEN}" \
      -e LOG_LEVEL=DEBUG \
      {{IMAGE_NAME}}:{{IMAGE_TAG}}

# Corre el contenedor con SINK=gcs — monta ADC del host para autenticación GCP
#
# Requiere haber corrido previamente:
#   gcloud auth application-default login
docker-run-gcs start=TEST_START end=TEST_END:
    docker run --rm \
      -v "${HOME}/.config/gcloud:/root/.config/gcloud:ro" \
      -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
      -e GCP_PROJECT_ID={{GCP_PROJECT_ID}} \
      -e GCS_BUCKET={{GCS_BUCKET}} \
      -e GCS_PREFIX={{GCS_PREFIX_CEN}} \
      -e CEN_AUTH_TOKEN="${CEN_AUTH_TOKEN}" \
      -e SINK=gcs \
      -e START_DATE={{start}} \
      -e END_DATE={{end}} \
      -e LOG_LEVEL=DEBUG \
      {{IMAGE_NAME}}:{{IMAGE_TAG}}

# Abre una shell bash dentro del contenedor para inspección y debug
docker-shell:
    docker run --rm -it \
      -v "${HOME}/.config/gcloud:/root/.config/gcloud:ro" \
      -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
      -e GCP_PROJECT_ID={{GCP_PROJECT_ID}} \
      -e GCS_BUCKET={{GCS_BUCKET}} \
      -e CEN_AUTH_TOKEN="${CEN_AUTH_TOKEN}" \
      --entrypoint bash \
      {{IMAGE_NAME}}:{{IMAGE_TAG}}

# ─────────────────────────────────────────────────────────────────────────────
# CALIDAD DE CÓDIGO
# ─────────────────────────────────────────────────────────────────────────────

# Ejecuta ruff: lint con auto-fix + format
lint:
    .venv/bin/ruff check src/ --fix
    .venv/bin/ruff format src/

# Ejecuta el type checker (ty / typlite)
check:
    .venv/bin/ty check src/
