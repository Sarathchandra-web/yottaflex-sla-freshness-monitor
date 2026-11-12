from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from src.repository import SLARepository


def generate_reconciliation_report(
    repo: SLARepository,
    run_id: str,
    report_dir: str | Path | None = None,
) -> Dict[str, Any]:
    """
    Verify source-to-target row counts, referential integrity, and SLA status distribution.
    Optionally writes JSON and Markdown audit reports to disk.
    """
    run_row = repo.conn.execute("""
        SELECT records_read, records_valid, records_rejected, status, duration_seconds
        FROM ops_pipeline_runs
        WHERE run_id = ?
    """, [run_id]).fetchone()

    if not run_row:
        raise ValueError(f"Run ID '{run_id}' not found in ops_pipeline_runs.")

    records_read, records_valid, records_rejected, run_status, duration_sec = run_row

    total_raw_in_db = repo.conn.execute("SELECT COUNT(*) FROM raw_load_events").fetchone()[0]
    total_rejects_in_db = repo.conn.execute("SELECT COUNT(*) FROM quarantine_rejected_records").fetchone()[0]

    orphan_fks = repo.conn.execute("""
        SELECT COUNT(*)
        FROM raw_load_events r
        LEFT JOIN dim_dataset_sla d ON r.dataset_id = d.dataset_id
        WHERE d.dataset_id IS NULL
    """).fetchone()[0]

    status_breakdown = dict(
        repo.conn.execute("""
            SELECT sla_status, COUNT(*)
            FROM fact_dataset_sla_status
            GROUP BY sla_status
            ORDER BY sla_status
        """).fetchall()
    )

    reject_reasons = [
        {"reason": r[0], "count": r[1]}
        for r in repo.conn.execute("""
            SELECT reject_reason, COUNT(*)
            FROM quarantine_rejected_records
            WHERE run_id = ?
            GROUP BY reject_reason
            ORDER BY COUNT(*) DESC
        """, [run_id]).fetchall()
    ]

    active_alerts = [
        {
            "alert_id": r[0],
            "dataset_id": r[1],
            "batch_date": str(r[2]),
            "alert_type": r[3],
            "severity": r[4],
            "message": r[5],
        }
        for r in repo.conn.execute("""
            SELECT alert_id, dataset_id, batch_date, alert_type, severity, message
            FROM fact_sla_alerts
            WHERE is_resolved = FALSE
            ORDER BY severity, dataset_id, batch_date
        """).fetchall()
    ]

    count_reconciled = (records_read == records_valid + records_rejected)
    integrity_passed = (orphan_fks == 0)

    report: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "run_status": run_status,
        "duration_seconds": duration_sec,
        "checks": {
            "source_to_target_count_reconciled": count_reconciled,
            "referential_integrity_passed": integrity_passed,
            "orphan_foreign_keys": orphan_fks,
        },
        "counts": {
            "run_records_read": records_read,
            "run_records_valid": records_valid,
            "run_records_rejected": records_rejected,
            "cumulative_raw_events_in_db": total_raw_in_db,
            "cumulative_quarantined_in_db": total_rejects_in_db,
        },
        "sla_status_distribution": status_breakdown,
        "run_reject_reasons": reject_reasons,
        "active_alerts": active_alerts,
    }

    if report_dir:
        out_path = Path(report_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        (out_path / f"reconciliation_{run_id}.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )

    return report