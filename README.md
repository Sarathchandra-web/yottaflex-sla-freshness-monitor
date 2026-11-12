# YottaFlex SLA & Freshness Monitor

A lightweight data engineering pipeline for tracking batch dataset arrival SLAs, checking data freshness, isolating malformed load events, and managing operational alerts. Built with Python, Pydantic, and DuckDB.

## Why I Built This
In batch data pipelines, silent delays or schema drift often go unnoticed until downstream dashboards break. This service ingests load lifecycle events (`STARTED`, `COMPLETED`, `FAILED`), validates incoming records against strict contracts, evaluates arrival deadlines against per-dataset SLA windows, and maintains an idempotent alert table in DuckDB.

## How It Works
1. **Ingestion & Quarantine (`src/ingestion.py`)**: Reads `.jsonl` or `.csv` event logs, validates schemas with Pydantic, catches duplicate `event_id`s or unknown `dataset_id` foreign keys, and routes bad rows to a quarantine table instead of crashing the batch.
2. **Warehouse & Bulk Loading (`src/repository.py`)**: Stores raw events, quarantined records, SLA status snapshots, and alerts in DuckDB using bulk JSON loading (`read_json`) so 100k+ records process in a few seconds.
3. **SLA & Freshness Evaluation (`src/transformations.py`)**: Compares actual completion times against expected UTC arrival windows to classify each `(dataset_id, batch_date)` as `ON_TIME`, `LATE`, `FAILED`, `MISSING`, or `IN_PROGRESS`. If a delayed or failed batch recovers on a rerun, open alerts auto-resolve.
4. **Reconciliation (`src/reconciliation.py`)**: Verifies `read == valid + rejected` counts and checks referential integrity at the end of every run, outputting a structured JSON summary.

## Repository Layout
- `config/pipeline_config.yaml` — Default runtime configuration
- `data/sample/` — Sample SLA config (`dataset_sla_config.json`) and test events (`sample_events.jsonl`)
- `docs/` — Data dictionary, design trade-offs, and prompt history
- `src/` — Core pipeline modules (`main.py`, `models.py`, `ingestion.py`, `repository.py`, `transformations.py`, `reconciliation.py`, `generator.py`)
- `tests/` — Unit, idempotency, edge-case, and 100k-row scale tests

## Quickstart

Create a virtual environment and install dependencies:
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt