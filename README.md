# ⚡ Recast

Pipeline de datos para predecir generación energética usando APIs públicas, GCP y BigQuery ML.

## Stack

- **Python 3.13+** — Ingestión de datos
- **Google Cloud Platform** — Cloud infrastructure
- **GCS** — Data lake
- **BigQuery + BigQuery ML** — Procesamiento y modelos
- **Terraform** — Infra as code

## Roadmap

```
Fase 1: Ingestión
  └─ [✓] Cliente Python para CEN API
  └─ [ ] Integración API climática (ERA5)

Fase 2: Almacenamiento
  └─ [✓] GCS + BigQuery (Terraform)
  └─ [ ] Pipeline GCS → BigQuery

Fase 3: ML
  └─ [ ] Modelado de datos en BigQuery
  └─ [ ] Entrenamiento modelos BQML

Fase 4: Producto
  └─ [ ] API de predicciones
  └─ [ ] Dashboard
```

## Quick Start

```bash
uv sync
cp src/ingest/.env.example src/ingest/.env
# Editar .env con tus credenciales
uv run python dev_main.py
```

## Licencia

MIT
