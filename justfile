# ─────────────────────────────────────────────────────────────────────────────
# justfile — Energy Project command runner
# Usage: just <recipe>  |  just --list
# ─────────────────────────────────────────────────────────────────────────────

set dotenv-load  # Carga .env → expone CEN_AUTH_TOKEN

# ── Infra config ──────────────────────────────────────────────────────────────
GCP_PROJECT_ID := "project-cb026a3b-da35-4742-a36"
GCS_BUCKET     := "recast-landing-project-cb026a3b-da35-4742-a36"
GCS_PREFIX_CEN := ""

# ── Docker ────────────────────────────────────────────────────────────────────
IMAGE_NAME := "energy-ingest"
IMAGE_TAG  := "latest"

# ── Fechas de prueba por defecto ─────────────────────────────────────────────
TEST_START := "2026-02-01"
TEST_END   := "2026-02-03"

# ─────────────────────────────────────────────────────────────────────────────
# Default: muestra todos los comandos disponibles
# ─────────────────────────────────────────────────────────────────────────────
_default:
    @just --list --unsorted

# ─────────────────────────────────────────────────────────────────────────────
# 1. LOCAL DEVELOPMENT (SINK=local)
# Comandos para probar la ingesta y guardar los resultados en ./data/raw/
# ─────────────────────────────────────────────────────────────────────────────

# Ejecuta el colector CEN para un rango de fechas y guarda en local
run-local start=TEST_START end=TEST_END:
    SINK=local \
    START_DATE={{start}} \
    END_DATE={{end}} \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# Descarga el historial completo de una central específica (cuida rate limit)
fetch-central-generation id_central start="2020-01-01" end="2026-12-31":
    SINK=local \
    START_DATE={{start}} \
    END_DATE={{end}} \
    EXTRA_PARAMS='{"idCentral": "{{id_central}}"}' \
    SLEEP=2.0 \
    PAGE_SIZE=10000 \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# Descarga la información y metadata de todas las centrales
fetch-centrals-info:
    SINK=local \
    SLEEP=2.0 \
    LIMIT_PARAM=limit \
    ENDPOINT='/centrales/v4/findByDate' \
    PAGE_SIZE=10000 \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# ─────────────────────────────────────────────────────────────────────────────
# 2. DATA PIPELINE (GCS & BIGQUERY)
# Comandos esenciales para el despliegue en Cloud Run y carga a BigQuery
# ─────────────────────────────────────────────────────────────────────────────

# Ejecuta el colector CEN y guarda directamente en Google Cloud Storage (GCS)
run-gcs start=TEST_START end=TEST_END:
    SINK=gcs \
    GCP_PROJECT_ID={{GCP_PROJECT_ID}} \
    GCS_BUCKET={{GCS_BUCKET}} \
    GCS_PREFIX={{GCS_PREFIX_CEN}} \
    START_DATE={{start}} \
    END_DATE={{end}} \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# Carga los datos generados (archivos JSONL) desde GCS hacia BigQuery
load-bq date="2026-05-17" source="cen" table="generacion_real":
  bq load \
  --source_format=NEWLINE_DELIMITED_JSON \
  --noreplace \
  {{GCP_PROJECT_ID}}:recast_staging.{{table}} \
  gs://recast-landing-{{GCP_PROJECT_ID}}/{{source}}/{{date}}/*.jsonl

# ─────────────────────────────────────────────────────────────────────────────
# 3. DOCKER OPERATIONS
# Construcción y pruebas de los contenedores que correrán en Cloud Run Job
# ─────────────────────────────────────────────────────────────────────────────

# Construye la imagen Docker de ingesta
docker-build:
    docker build -t {{IMAGE_NAME}}:{{IMAGE_TAG}} .

# Corre el contenedor localmente y monta el volumen ./data para ver los archivos
docker-run-local start=TEST_START end=TEST_END:
    docker run --rm \
      -v "$(pwd)/data:/app/data" \
      -e SINK=local \
      -e START_DATE={{start}} \
      -e END_DATE={{end}} \
      -e CEN_AUTH_TOKEN="${CEN_AUTH_TOKEN}" \
      -e LOG_LEVEL=DEBUG \
      {{IMAGE_NAME}}:{{IMAGE_TAG}}

# Corre el contenedor simulando Cloud Run (escribe en GCS usando credenciales locales)
# Nota: Requiere haber ejecutado 'gcloud auth application-default login'
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

# Abre una shell interactiva en el contenedor para debugear
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
# 4. CODE QUALITY
# ─────────────────────────────────────────────────────────────────────────────

# Ejecuta linter y formateador (ruff)
lint:
    .venv/bin/ruff check src/ --fix
    .venv/bin/ruff format src/

# Ejecuta el chequeo estático de tipos (typlite)
check:
    .venv/bin/ty check src/
