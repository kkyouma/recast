# ─────────────────────────────────────────────────────────────────────────────
# justfile — Energy Project command runner
# Usage: just <recipe>  |  just --list
# ─────────────────────────────────────────────────────────────────────────────

set dotenv-load

# ── Infra ─────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID := env_var_or_default('GCP_PROJECT_ID', 'recast-landing-project')
GCS_BUCKET     := env_var_or_default('GCS_BUCKET', 'recast-landing-bucket')
BQ_DATASET     := env_var_or_default('BQ_DATASET', 'energy_project')

# ── Fechas de prueba por defecto ─────────────────────────────────────────────
TEST_START := "2020-01-01"
TEST_END   := "2026-06-10"

# ─────────────────────────────────────────────────────────────────────────────
# Default: muestra todos los comandos disponibles
# ─────────────────────────────────────────────────────────────────────────────
_default:
    @just --list --unsorted

# ─────────────────────────────────────────────────────────────────────────────
# 1. INGESTA LOCAL (SINK=local) — testeo rápido sin GCS
# ─────────────────────────────────────────────────────────────────────────────

# Ingesta de generación real (endpoint por defecto) → guarda en ./data/
run-local id_central start=TEST_START end=TEST_END:
    SINK=local \
    EXTRA_PARAMS='{"idCentral": "{{id_central}}"}' \
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

# Ingesta de datos climáticos ERA5 → guarda en ./data/
run-local-era5 lat="0.0" lon="0.0" start=TEST_START end=TEST_END:
    SINK=local \
    LATITUDE={{lat}} \
    LONGITUDE={{lon}} \
    START_DATE={{start}} \
    END_DATE={{end}} \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.era5

# ─────────────────────────────────────────────────────────────────────────────
# 2. INGESTA A GCS (SINK=gcs) — producción / Cloud Run
# ─────────────────────────────────────────────────────────────────────────────

# Ingesta de generación real → escribe en GCS
run-gcs id_central start=TEST_START end=TEST_END:
    SINK=gcs \
    GCP_PROJECT_ID={{GCP_PROJECT_ID}} \
    GCS_BUCKET={{GCS_BUCKET}} \
    EXTRA_PARAMS='{"idCentral": "{{id_central}}"}' \
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

# Ingesta de datos climáticos ERA5 → escribe en GCS
run-gcs-era5 lat="0.0" lon="0.0" start=TEST_START end=TEST_END:
    SINK=gcs \
    GCP_PROJECT_ID={{GCP_PROJECT_ID}} \
    GCS_BUCKET={{GCS_BUCKET}} \
    LATITUDE={{lat}} \
    LONGITUDE={{lon}} \
    START_DATE={{start}} \
    END_DATE={{end}} \
    LOG_LEVEL=DEBUG \
    uv run -m ingest.era5

# ─────────────────────────────────────────────────────────────────────────────
# 3. CARGA A BIGQUERY
# ─────────────────────────────────────────────────────────────────────────────

# Carga generación real desde GCS → BigQuery
# Uso: just load-bq-generacion 2026-05-19
bq-load-generacion date="2026-05-19":
    bq load \
    --source_format=NEWLINE_DELIMITED_JSON \
    --replace=false \
    {{GCP_PROJECT_ID}}:{{BQ_DATASET}}.generacion_real \
    "gs://{{GCS_BUCKET}}/cen/{{date}}/generacion-real*.jsonl"

# Carga info de centrales desde GCS → BigQuery
# Uso: just load-bq-centrales 2026-05-19
bq-load-centrales date="2026-05-19":
    bq load \
    --source_format=NEWLINE_DELIMITED_JSON \
    --replace=true \
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

# ─────────────────────────────────────────────────────────────────────────────
# 6. DBT
# ─────────────────────────────────────────────────────────────────────────────

# Ejecuta dbt debug para verificar la conexión
dbt-debug:
    DBT_PROFILES_DIR=src/transform \
    DBT_GCP_PROJECT={{GCP_PROJECT_ID}} \
    DBT_BQ_DATASET={{BQ_DATASET}} \
    dbt debug --project-dir src/transform

# Ejecuta todos los modelos dbt
dbt-run:
    DBT_PROFILES_DIR=src/transform \
    DBT_GCP_PROJECT={{GCP_PROJECT_ID}} \
    DBT_BQ_DATASET={{BQ_DATASET}} \
    dbt run --project-dir src/transform

# Ejecuta las pruebas de dbt
dbt-test:
    DBT_PROFILES_DIR=src/transform \
    DBT_GCP_PROJECT={{GCP_PROJECT_ID}} \
    DBT_BQ_DATASET={{BQ_DATASET}} \
    dbt test --project-dir src/transform

# Compila los modelos dbt
dbt-compile:
    DBT_PROFILES_DIR=src/transform \
    DBT_GCP_PROJECT={{GCP_PROJECT_ID}} \
    DBT_BQ_DATASET={{BQ_DATASET}} \
    dbt compile --project-dir src/transform

# Limpia los artefactos compilados de dbt
dbt-clean:
    DBT_PROFILES_DIR=src/transform \
    DBT_GCP_PROJECT={{GCP_PROJECT_ID}} \
    DBT_BQ_DATASET={{BQ_DATASET}} \
    dbt clean --project-dir src/transform
