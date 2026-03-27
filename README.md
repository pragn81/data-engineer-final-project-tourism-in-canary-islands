# Data Engineering Zoomcamp Project: Tourism in Canary Islands

## Problem description

The Canary Islands receive millions of tourists every year and the regional statistical
institute (ISTAC) publishes quarterly datasets on tourist volumes, accommodation choices,
demographics (age & sex), and economic revenue.

This project builds an end-to-end batch data pipeline that:

1. **Ingests** the three ISTAC open-data CSVs (accommodations, age/sex, revenue) on a
   quarterly schedule.
2. **Transforms** the raw data with PySpark — normalises column names, casts types, and
   produces Parquet files partitioned by year.
3. **Stores** raw CSVs and processed Parquet in Google Cloud Storage (data lake).
4. **Loads** the Parquet into BigQuery (`tourism_raw.*`), with MONTH partitioning on
   `period_date` and clustering on the most-queried dimensions.
5. **Models** the data with dbt: staging views clean and rename fields; mart tables
   aggregate by accommodation type and by demographic group.
6. **Visualises** two key questions in a Looker Studio dashboard:
   - How did tourist arrivals evolve over time by accommodation type?
   - What is the demographic breakdown (age / sex) of tourists?

The full pipeline is orchestrated by Kestra and scheduled to run on the first day of each
quarter.

---

## Architecture

```
ISTAC API
   │
   ▼
ingestion/download.py  ──►  data/raw/*.csv
                                │
                                ▼
                     ingestion/upload_gcs.py  ──►  GCS: tourism-raw/  (CSV)
                                │
                                ▼
                  processing/spark_transform.py
                     (local PySpark, local[*])
                                │
                                ▼
                          data/processed/*
                                │
                                ▼
                     ingestion/upload_gcs.py  ──►  GCS: tourism-processed/  (Parquet)
                                │
                                ▼
                     ingestion/load_bigquery.py
                                │
                                ▼
                    BigQuery: tourism_raw.*
                     (partitioned + clustered)
                                │
                                ▼
                           dbt run
                      tourism_staging.*  (views)
                      tourism_mart.*     (tables)
                                │
                                ▼
                       Looker Studio dashboard
```

**Cloud:** Google Cloud Platform (GCS + BigQuery)  
**IaC:** Terraform  
**Orchestration:** Kestra (Docker Compose)  
**Transformation:** PySpark 3.5 (batch) + dbt 1.7 (SQL)  

---

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python 3.11 + uv |
| IaC | Terraform ~5.0 |
| Data lake | Google Cloud Storage |
| Data warehouse | BigQuery |
| Batch processing | PySpark 3.5 |
| SQL modelling | dbt-bigquery 1.7 |
| Orchestration | Kestra (Docker Compose) |
| Dashboard | Looker Studio |

---

## Reproducing the project

### Prerequisites

- Python 3.11 and [uv](https://docs.astral.sh/uv/) installed
- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.6
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- A GCP project with billing enabled
- A GCP service account (Owner or Storage+BQ admin) — save the JSON key as
  `credentials/service_account.json`
- Java 11+ on `PATH` (required by PySpark)

### 1 — Clone & install dependencies

```bash
git clone <repo-url>
cd data-engineer-final-project-tourism-in-canary-islands
uv sync
```

### 2 — Configure environment

Copy the example and fill in your values:

```bash
cp .env.example .env   # then edit .env
```

Minimum required variables in `.env`:

```dotenv
GCP_PROJECT_ID=<your-gcp-project-id>
GCS_RAW_BUCKET=<project-id>-tourism-raw
GCS_PROCESSED_BUCKET=<project-id>-tourism-processed
BQ_DATASET_RAW=tourism_raw
BQ_DATASET_STAGING=tourism_staging
BQ_DATASET_MART=tourism_mart
GCP_REGION=europe-west1
GOOGLE_APPLICATION_CREDENTIALS=credentials/pipeline_sa.json
```

### 3 — Provision cloud resources with Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in project_id
terraform init
terraform apply
cd ..
```

This creates:
- GCS buckets: `tourism-raw` and `tourism-processed`
- BigQuery datasets: `tourism_raw`, `tourism_staging`, `tourism_mart`
- Service account `tourism-pipeline` with Storage + BigQuery admin roles
- Pipeline SA key written to `credentials/pipeline_sa.json`

### 4 — Run the pipeline manually (step by step)

```bash
# Download raw CSVs from ISTAC API
uv run main.py ingest

# PySpark transformation → data/processed/
uv run main.py transform

# Upload to GCS (raw CSVs + processed Parquet)
uv run main.py upload

# Load Parquet from GCS into BigQuery (tourism_raw.*)
uv run main.py load
```

### 5 — Run dbt models

```bash
# Add the BigQuery profile to ~/.dbt/profiles.yml (see dbt/README section below)
cd dbt
dbt deps          # install packages (if any)
dbt run           # creates staging views + mart tables
dbt test          # optional: run schema tests
cd ..
```

#### dbt profile (`~/.dbt/profiles.yml`)

```yaml
canary_islands_tourism:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: <your-gcp-project-id>
      dataset: tourism_staging
      keyfile: /absolute/path/to/credentials/pipeline_sa.json
      location: europe-west1
      threads: 4
```

### 6 — Run the full pipeline via Kestra

```bash
docker compose up -d
```

Then open **http://localhost:8080**.

First time: Kestra shows a setup wizard — create your admin user (e.g.
`admin@kestra.io` / `Admin1234`).

1. Go to **Flows** and confirm `canary-islands-tourism-pipeline` (namespace `tourism`)
   is listed. If it is not, click **+** → paste the contents of `kestra/pipeline.yml`
   → **Save**.
2. To trigger a manual run: open the flow → **Execute** → **Execute now**.
3. The quarterly schedule triggers automatically on Jan 1, Apr 1, Jul 1, Oct 1 at 06:00 UTC.

---

## Dashboard

The Looker Studio dashboard connects to the `tourism_mart` BigQuery dataset.

**Access:** [Looker Studio dashboard](https://lookerstudio.google.com) ← add your link here

### Tile 1 — Tourist arrivals by accommodation type over time

- Data source: `tourism_mart.mart_tourists_by_accommodation`
- Chart type: Line chart
- Dimension: `period_date`
- Breakdown: `accommodation_type`
- Metric: `SUM(tourist_count)`

### Tile 2 — Tourist demographics (age group & sex)

- Data source: `tourism_mart.mart_tourists_by_demographics`
- Chart type: Bar chart (grouped)
- Dimension: `age_group`
- Breakdown: `sex`
- Metric: `SUM(tourist_count)`

---

## Project structure

```
.
├── credentials/          # GCP service account keys (gitignored)
├── data/
│   ├── raw/              # Downloaded CSVs (gitignored)
│   └── processed/        # PySpark Parquet output (gitignored)
├── dbt/
│   ├── models/
│   │   ├── staging/      # Views: stg_tourist_accommodations, stg_tourist_age_sex, stg_tourist_revenue
│   │   └── mart/         # Tables: mart_tourists_by_accommodation, mart_tourists_by_demographics
│   └── dbt_project.yml
├── ingestion/
│   ├── download.py       # Download CSVs from ISTAC API
│   ├── upload_gcs.py     # Upload to GCS
│   └── load_bigquery.py  # Load Parquet → BigQuery
├── kestra/
│   └── pipeline.yml      # Kestra flow definition (5-step DAG)
├── processing/
│   └── spark_transform.py  # PySpark batch transformation
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── docker-compose.yml    # Kestra + PostgreSQL backend
├── Dockerfile.kestra     # Kestra image + Python 3.11 + uv
├── main.py               # CLI entrypoint
└── pyproject.toml        # Python dependencies (uv)
```
