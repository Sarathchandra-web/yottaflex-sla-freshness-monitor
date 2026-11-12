from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

import yaml

from src.generator import get_default_dataset_configs
from src.ingestion import ingest_events_file
from src.models import DatasetSLAConfig
from src.reconciliation import generate_reconciliation_report
from src.repository import SLARepository
from src.transformations import evaluate_slas_and_freshness

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("sla_pipeline")


def load_config(config_path: str | Path = "config/pipeline_config.yaml") -> Dict[str, Any]:
    path = Path(config_path)
    if path.exists():
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        "storage": {
            "database_path": "data/sla_monitor.duckdb",
            "reports_dir": "data/reports",
        },
        "sla_rules": {
            "default_evaluation_anchor_utc": "2026-11-12T12:00:00Z",
        },
    }


def run_pipeline(
    input_files: List[str | Path],
    db_path: str | Path = "data/sla_monitor.duckdb",
    dataset_config_path: str | Path | None = None,
    evaluation_anchor_utc: str = "2026-11-12T12:00:00Z",
    reports_dir: str | Path | None = "data/reports",
    run_id: str | None = None,
) -> Dict[str, Any]:
    """
    Execute the end-to-end SLA & Freshness pipeline with full observability
    and partial-failure tolerance across multiple input files.
    """
    started_at = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    active_run_id = run_id or f"run_{started_at.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"

    logger.info("Starting pipeline run_id=%s", active_run_id)
    repo = SLARepository(db_path=db_path)

    total_read = 0
    total_valid = 0
    total_rejected = 0
    file_errors: List[str] = []

    try:
        # 1. Load dataset SLA dimension configs
        if dataset_config_path and Path(dataset_config_path).exists():
            raw_cfgs = json.loads(Path(dataset_config_path).read_text(encoding="utf-8"))
            configs = [DatasetSLAConfig(**item) for item in raw_cfgs]
        else:
            configs = get_default_dataset_configs()

        repo.upsert_dataset_configs(configs)

        # 2. Ingest input files (isolated per file so one missing/corrupt file triggers PARTIAL_FAILURE)
        for f_path in input_files:
            try:
                summary = ingest_events_file(repo=repo, file_path=f_path, run_id=active_run_id)
                total_read += summary.records_read
                total_valid += summary.records_valid
                total_rejected += summary.records_rejected
                logger.info(
                    "Ingested %s -> read=%d, valid=%d, rejected=%d",
                    f_path, summary.records_read, summary.records_valid, summary.records_rejected,
                )
            except Exception as exc:
                err_msg = f"{f_path}: {exc}"
                logger.error("File ingestion failure: %s", err_msg)
                file_errors.append(err_msg)

        # 3. Evaluate SLAs, Freshness, and Alerts
        eval_stats = evaluate_slas_and_freshness(
            repo=repo,
            run_id=active_run_id,
            evaluation_anchor_utc=evaluation_anchor_utc,
        )

        # 4. Determine run status
        if file_errors and total_valid == 0 and len(input_files) > 0:
            status = "FAILED"
        elif file_errors:
            status = "PARTIAL_FAILURE"
        else:
            status = "SUCCESS"

        ended_at = datetime.now(timezone.utc)
        duration = round(time.perf_counter() - t0, 4)

        repo.record_pipeline_run({
            "run_id": active_run_id,
            "started_at_utc": started_at,
            "ended_at_utc": ended_at,
            "duration_seconds": duration,
            "status": status,
            "records_read": total_read,
            "records_valid": total_valid,
            "records_rejected": total_rejected,
            "alerts_active": eval_stats["alerts_active"],
            "error_summary": "; ".join(file_errors) if file_errors else None,
        })

        report = generate_reconciliation_report(
            repo=repo, run_id=active_run_id, report_dir=reports_dir
        )
        logger.info(
            "Completed run_id=%s | status=%s | duration=%.4fs | active_alerts=%d",
            active_run_id, status, duration, eval_stats["alerts_active"],
        )
        return report

    finally:
        repo.close()


if __name__ == "__main__":
    cfg = load_config()
    parser = argparse.ArgumentParser(description="Run YottaFlex SLA & Freshness Monitoring Pipeline")
    parser.add_argument(
        "--inputs",
        nargs="+",
        default=["data/sample/sample_events.jsonl"],
        help="One or more input .jsonl or .csv files",
    )
    parser.add_argument(
        "--dataset-config",
        default="data/sample/dataset_sla_config.json",
        help="Path to dataset SLA JSON config",
    )
    parser.add_argument(
        "--db-path",
        default=cfg["storage"]["database_path"],
        help="DuckDB file path",
    )
    parser.add_argument(
        "--anchor-utc",
        default=cfg["sla_rules"]["default_evaluation_anchor_utc"],
        help="Evaluation timestamp (ISO-8601 UTC)",
    )
    args = parser.parse_args()

    result = run_pipeline(
        input_files=args.inputs,
        db_path=args.db_path,
        dataset_config_path=args.dataset_config,
        evaluation_anchor_utc=args.anchor_utc,
        reports_dir=cfg["storage"]["reports_dir"],
    )
    print(json.dumps(result, indent=2))