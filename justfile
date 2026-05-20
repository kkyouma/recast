# ─────────────────────────────────────────────────────────────────────────────
# justfile — Energy Project command runner
# Usage: just <recipe>  |  just --list
# ─────────────────────────────────────────────────────────────────────────────

set dotenv-load  # Carga .env → expone CEN_AUTH_TOKEN

# ── Infra ─────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID := "project-cb026a3b-da35-4742-a36"
GCS_BUCKET     := "recast-landing-project-cb026a3b-da35-4742-a36"
BQ_DATASET     := "recast_staging"

# ── Fechas de prueba por defecto ─────────────────────────────────────────────
TEST_START := "2026-02-01"
TEST_END   := "2026-02-03"

# ─────────────────────────────────────────────────────────────────────────────
# Default: muestra todos los comandos disponibles
# ─────────────────────────────────────────────────────────────────────────────
_default:
    @just --list --unsorted

# ─────────────────────────────────────────────────────────────────────────────
# 1. INGESTA LOCAL (SINK=local) — testeo rápido sin GCS
# ─────────────────────────────────────────────────────────────────────────────

# Ingesta de generación real (endpoint por defecto) → guarda en ./data/
run-local start=TEST_START end=TEST_END:
    SINK=local \
    START_DATE={{start}} \
    END_DATE={{end}} \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# Ingesta de info/metadata de centrales → guarda en ./data/
run-local-centrales:
    SINK=local \
    SLEEP=2.0 \
    LIMIT_PARAM=limit \
    ENDPOINT='/centrales/v4/findByDate' \
    PAGE_SIZE=10000 \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# ─────────────────────────────────────────────────────────────────────────────
# 2. INGESTA A GCS (SINK=gcs) — producción / Cloud Run
# ─────────────────────────────────────────────────────────────────────────────

# Ingesta de generación real → escribe en GCS
run-gcs start=TEST_START end=TEST_END:
    SINK=gcs \
    GCP_PROJECT_ID={{GCP_PROJECT_ID}} \
    GCS_BUCKET={{GCS_BUCKET}} \
    START_DATE={{start}} \
    END_DATE={{end}} \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# Ingesta de info/metadata de centrales → escribe en GCS
run-gcs-centrales:
    SINK=gcs \
    GCP_PROJECT_ID={{GCP_PROJECT_ID}} \
    GCS_BUCKET={{GCS_BUCKET}} \
    SLEEP=2.0 \
    LIMIT_PARAM=limit \
    ENDPOINT='/centrales/v4/findByDate' \
    PAGE_SIZE=10000 \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.cen

# ─────────────────────────────────────────────────────────────────────────────
# 3. CARGA A BIGQUERY
# ─────────────────────────────────────────────────────────────────────────────

# Carga generación real desde GCS → BigQuery
# Uso: just load-bq-generacion 2026-05-19
load-bq-generacion date="2026-05-19":
    bq load \
    --source_format=NEWLINE_DELIMITED_JSON \
    --noreplace \
    {{GCP_PROJECT_ID}}:{{BQ_DATASET}}.generacion_real \
    "gs://{{GCS_BUCKET}}/cen/{{date}}/generacion_real*.jsonl"

# Carga info de centrales desde GCS → BigQuery
# Uso: just load-bq-centrales 2026-05-19
load-bq-centrales date="2026-05-19":
    bq load \
    --source_format=NEWLINE_DELIMITED_JSON \
    --noreplace \
    {{GCP_PROJECT_ID}}:{{BQ_DATASET}}.centrales_info \
    "gs://{{GCS_BUCKET}}/cen/{{date}}/centrales_v4*.jsonl"

# ─────────────────────────────────────────────────────────────────────────────
# 4. VERIFICACIÓN
# ─────────────────────────────────────────────────────────────────────────────

# Verifica archivos en GCS para una fecha específica
# Uso: just check-gcs 2026-05-19
check-gcs date="2026-05-19" source="cen":
    @echo "── Archivos en GCS para {{date}} ──"
    @gcloud storage ls -l "gs://{{GCS_BUCKET}}/{{source}}/{{date}}/*.jsonl" 2>/dev/null \
      && echo "✓ Archivos encontrados" \
      || echo "✗ No hay archivos para esta fecha"

# Verifica registros en BigQuery para una tabla y fecha
# Uso: just check-bq generacion_real 2026-05-19
check-bq table="generacion_real" date="2026-05-19":
    @echo "── Registros en {{BQ_DATASET}}.{{table}} para {{date}} ──"
    bq query --use_legacy_sql=false --format=pretty \
      'SELECT COUNT(*) AS total_rows FROM `{{GCP_PROJECT_ID}}.{{BQ_DATASET}}.{{table}} WHERE date_trunc(fecha_hora, day) = {{date}}` LIMIT 1'

# ─────────────────────────────────────────────────────────────────────────────
# 5. CODE QUALITY
# ─────────────────────────────────────────────────────────────────────────────

# Ejecuta linter y formateador (ruff)
lint:
    .venv/bin/ruff check src/ --fix
    .venv/bin/ruff format src/

# Ejecuta el chequeo estático de tipos (typlite)
check:
    .venv/bin/ty check src/
