import json
from pathlib import Path
from src.main import run_pipeline

SAMPLE_CONFIG = Path("data/sample/dataset_sla_config.json")


def test_empty_batch_file(tmp_path: Path):
    empty_file = tmp_path / "empty.jsonl"
    empty_file.write_text("", encoding="utf-8")
    db_path = tmp_path / "empty.duckdb"

    report = run_pipeline(
        input_files=[empty_file],
        db_path=db_path,
        dataset_config_path=SAMPLE_CONFIG,
        evaluation_anchor_utc="2026-11-12T12:00:00Z",
        reports_dir=None,
    )
    assert report["run_status"] == "SUCCESS"
    assert report["counts"]["run_records_read"] == 0
    assert report["counts"]["run_records_valid"] == 0
    assert report["counts"]["run_records_rejected"] == 0


def test_malformed_schema_drift_nulls_and_bad_fk(tmp_path: Path):
    bad_file = tmp_path / "bad_events.jsonl"
    lines = [
        "{corrupt_json_line",
        json.dumps({"event_id": "e1", "dataset_id": "ds_orders_core"}),
        json.dumps({
            "event_id": "e2",
            "dataset_id": "ds_orders_core",
            "batch_date": "2026-11-12",
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:10:00Z",
            "extra_drift_col": 123,
        }),
        json.dumps({
            "event_id": "e3",
            "dataset_id": "ds_nonexistent_fk",
            "batch_date": "2026-11-12",
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:10:00Z",
        }),
        json.dumps({
            "event_id": "e4",
            "dataset_id": "ds_orders_core",
            "batch_date": "2026-11-12",
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:10:00Z",
            "record_count": 500,
        }),
        json.dumps({
            "event_id": "e4",
            "dataset_id": "ds_orders_core",
            "batch_date": "2026-11-12",
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:10:00Z",
            "record_count": 500,
        }),
    ]
    bad_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    db_path = tmp_path / "edge.duckdb"

    report = run_pipeline(
        input_files=[bad_file],
        db_path=db_path,
        dataset_config_path=SAMPLE_CONFIG,
        evaluation_anchor_utc="2026-11-12T12:00:00Z",
        reports_dir=None,
    )
    assert report["counts"]["run_records_read"] == 6
    assert report["counts"]["run_records_valid"] == 1
    assert report["counts"]["run_records_rejected"] == 5
    assert report["checks"]["referential_integrity_passed"] is True