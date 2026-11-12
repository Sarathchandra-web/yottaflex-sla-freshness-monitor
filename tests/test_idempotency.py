import json
from pathlib import Path
from src.main import run_pipeline
from src.repository import SLARepository


def test_rerun_idempotency_and_late_arrival_auto_resolution(tmp_path: Path):
    db_path = tmp_path / "idem.duckdb"
    batch_file = tmp_path / "batch1.jsonl"

    batch_file.write_text("", encoding="utf-8")
    run_pipeline(
        input_files=[batch_file],
        db_path=db_path,
        evaluation_anchor_utc="2026-11-12T12:00:00Z",
        reports_dir=None,
    )

    repo = SLARepository(db_path)
    missing_alerts_r1 = repo.conn.execute(
        "SELECT COUNT(*) FROM fact_sla_alerts WHERE dataset_id = 'ds_orders_core' AND alert_type = 'MISSING_LOAD' AND is_resolved = FALSE"
    ).fetchone()[0]
    repo.close()
    assert missing_alerts_r1 == 1

    batch_file.write_text(
        json.dumps({
            "event_id": "evt_late_01",
            "dataset_id": "ds_orders_core",
            "batch_date": "2026-11-12",
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T08:00:00Z",
            "record_count": 10000,
        }) + "\n",
        encoding="utf-8",
    )
    run_pipeline(
        input_files=[batch_file],
        db_path=db_path,
        evaluation_anchor_utc="2026-11-12T12:00:00Z",
        reports_dir=None,
    )
    run_pipeline(
        input_files=[batch_file],
        db_path=db_path,
        evaluation_anchor_utc="2026-11-12T12:00:00Z",
        reports_dir=None,
    )

    repo = SLARepository(db_path)
    try:
        raw_count = repo.conn.execute("SELECT COUNT(*) FROM raw_load_events").fetchone()[0]
        resolved_missing = repo.conn.execute(
            "SELECT is_resolved FROM fact_sla_alerts WHERE dataset_id = 'ds_orders_core' AND alert_type = 'MISSING_LOAD'"
        ).fetchone()[0]
        breach_count = repo.conn.execute(
            "SELECT COUNT(*) FROM fact_sla_alerts WHERE dataset_id = 'ds_orders_core' AND alert_type = 'SLA_BREACH'"
        ).fetchone()[0]
        assert raw_count == 1
        assert resolved_missing is True
        assert breach_count == 1
    finally:
        repo.close()


def test_partial_failure_recovery(tmp_path: Path):
    db_path = tmp_path / "partial.duckdb"
    valid_file = tmp_path / "valid.jsonl"
    missing_file = tmp_path / "does_not_exist.jsonl"

    valid_file.write_text(
        json.dumps({
            "event_id": "evt_partial_ok",
            "dataset_id": "ds_orders_core",
            "batch_date": "2026-11-12",
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:10:00Z",
            "record_count": 2500,
        }) + "\n",
        encoding="utf-8",
    )

    report = run_pipeline(
        input_files=[valid_file, missing_file],
        db_path=db_path,
        evaluation_anchor_utc="2026-11-12T12:00:00Z",
        reports_dir=None,
    )
    assert report["run_status"] == "PARTIAL_FAILURE"
    assert report["counts"]["run_records_valid"] == 1