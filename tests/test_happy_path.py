from pathlib import Path
from src.generator import generate_starter_sample
from src.main import run_pipeline
from src.repository import SLARepository


def test_starter_sample_end_to_end(tmp_path: Path):
    sample_dir = tmp_path / "sample"
    db_path = tmp_path / "test.duckdb"
    reports_dir = tmp_path / "reports"

    expected = generate_starter_sample(sample_dir, seed=42)

    report = run_pipeline(
        input_files=[sample_dir / "sample_events.jsonl"],
        db_path=db_path,
        dataset_config_path=sample_dir / "dataset_sla_config.json",
        evaluation_anchor_utc=expected["evaluation_anchor_utc"],
        reports_dir=reports_dir,
    )

    assert report["run_status"] == "SUCCESS"
    assert report["checks"]["source_to_target_count_reconciled"] is True
    assert report["checks"]["referential_integrity_passed"] is True
    assert report["counts"]["run_records_read"] == expected["reconciliation"]["records_read"]
    assert report["counts"]["run_records_valid"] == expected["reconciliation"]["records_valid"]
    assert report["counts"]["run_records_rejected"] == expected["reconciliation"]["records_rejected"]

    repo = SLARepository(db_path)
    try:
        rows = repo.conn.execute(
            "SELECT dataset_id, sla_status FROM fact_dataset_sla_status WHERE batch_date = '2026-11-12'"
        ).fetchall()
        assert dict(rows) == expected["expected_dataset_statuses"]
    finally:
        repo.close()