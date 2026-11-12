from pathlib import Path
from src.generator import generate_scale_dataset
from src.main import run_pipeline


def test_100k_scale_benchmark(tmp_path: Path):
    scale_file = generate_scale_dataset(tmp_path, num_records=100_000, seed=42)
    db_path = tmp_path / "scale.duckdb"

    report = run_pipeline(
        input_files=[scale_file],
        db_path=db_path,
        evaluation_anchor_utc="2026-11-12T12:00:00Z",
        reports_dir=None,
    )
    assert report["run_status"] == "SUCCESS"
    assert report["counts"]["run_records_read"] == 100_000
    assert report["checks"]["source_to_target_count_reconciled"] is True
    assert report["duration_seconds"] < 30.0