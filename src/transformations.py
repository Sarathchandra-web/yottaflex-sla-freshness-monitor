from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple, Dict, Any

from src.models import AlertType, SLAStatus, make_deterministic_id, parse_iso_utc
from src.repository import SLARepository


def evaluate_slas_and_freshness(
    repo: SLARepository,
    run_id: str,
    evaluation_anchor_utc: str | datetime,
    target_batch_dates: List[str] | None = None,
) -> Dict[str, int]:
    """
    Evaluate arrival SLAs and freshness across all active datasets for target batch dates.
    Idempotently upserts fact_dataset_sla_status and fact_sla_alerts using bulk DuckDB loads.
    """
    anchor_dt = parse_iso_utc(evaluation_anchor_utc)

    # Determine batch dates to evaluate (from ingested events + anchor date)
    if target_batch_dates:
        batch_dates = sorted(set(target_batch_dates))
    else:
        db_dates = [
            str(r[0])
            for r in repo.conn.execute("SELECT DISTINCT batch_date FROM raw_load_events").fetchall()
        ]
        anchor_date_str = anchor_dt.date().isoformat()
        batch_dates = sorted(set(db_dates + [anchor_date_str]))

    configs = repo.conn.execute("""
        SELECT
            dataset_id, dataset_name, owner_team,
            expected_arrival_hour_utc, expected_arrival_minute_utc,
            sla_window_minutes, max_freshness_minutes, criticality
        FROM dim_dataset_sla
        WHERE is_active = TRUE
    """).fetchall()

    # Aggregate raw events per (dataset_id, batch_date) handling out-of-order events cleanly
    event_aggs = repo.conn.execute("""
        SELECT
            dataset_id,
            CAST(batch_date AS VARCHAR) AS batch_date,
            MIN(CASE WHEN event_type = 'STARTED' THEN event_timestamp_utc END) AS first_start_utc,
            MAX(CASE WHEN event_type = 'COMPLETED' THEN event_timestamp_utc END) AS last_comp_utc,
            MAX(CASE WHEN event_type = 'FAILED' THEN event_timestamp_utc END) AS last_fail_utc,
            MAX(CASE WHEN event_type = 'COMPLETED' THEN record_count END) AS max_records
        FROM raw_load_events
        GROUP BY dataset_id, batch_date
    """).fetchall()

    agg_lookup: Dict[Tuple[str, str], Tuple[Any, ...]] = {
        (row[0], row[1]): row[2:] for row in event_aggs
    }

    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()
    anchor_date_iso = anchor_dt.date().isoformat()
    dumps = json.dumps

    status_lines: List[str] = []
    alert_upsert_lines: List[str] = []
    alert_resolution_lines: List[str] = []

    for ds_id, ds_name, owner, exp_hr, exp_min, sla_win, max_fresh, crit in configs:
        for b_date_str in batch_dates:
            b_year, b_month, b_day = map(int, b_date_str.split("-"))
            expected_dt = datetime(b_year, b_month, b_day, exp_hr, exp_min, tzinfo=timezone.utc)
            sla_deadline_dt = expected_dt + timedelta(minutes=sla_win)

            start_dt, comp_dt, fail_dt, rec_count = agg_lookup.get(
                (ds_id, b_date_str), (None, None, None, None)
            )

            # Normalize DuckDB timestamps to UTC-aware datetime
            if start_dt is not None:
                start_dt = parse_iso_utc(start_dt)
            if comp_dt is not None:
                comp_dt = parse_iso_utc(comp_dt)
            if fail_dt is not None:
                fail_dt = parse_iso_utc(fail_dt)

            arrival_delay_min: float | None = None
            freshness_min: float | None = None
            sla_status: SLAStatus

            # Case 1: Completed load (even if COMPLETED arrived before STARTED out-of-order)
            if comp_dt is not None and (fail_dt is None or comp_dt >= fail_dt):
                arrival_delay_min = round((comp_dt - expected_dt).total_seconds() / 60.0, 2)
                freshness_min = round(max(0.0, (anchor_dt - comp_dt).total_seconds() / 60.0), 2)
                sla_status = SLAStatus.ON_TIME if comp_dt <= sla_deadline_dt else SLAStatus.LATE

            # Case 2: Failed load without subsequent recovery completion
            elif fail_dt is not None:
                ref_dt = start_dt or fail_dt
                arrival_delay_min = round((ref_dt - expected_dt).total_seconds() / 60.0, 2)
                sla_status = SLAStatus.FAILED

            # Case 3: Started but still running
            elif start_dt is not None:
                arrival_delay_min = round((start_dt - expected_dt).total_seconds() / 60.0, 2)
                sla_status = (
                    SLAStatus.LATE if anchor_dt > sla_deadline_dt else SLAStatus.IN_PROGRESS
                )

            # Case 4: Zero events received for this dataset & batch_date
            else:
                if anchor_dt > sla_deadline_dt:
                    arrival_delay_min = round((anchor_dt - expected_dt).total_seconds() / 60.0, 2)
                    sla_status = SLAStatus.MISSING
                else:
                    sla_status = SLAStatus.IN_PROGRESS

            status_lines.append(dumps({
                "dataset_id": ds_id,
                "batch_date": b_date_str,
                "expected_arrival_utc": expected_dt.isoformat(),
                "sla_deadline_utc": sla_deadline_dt.isoformat(),
                "actual_start_utc": start_dt.isoformat() if start_dt else None,
                "actual_completion_utc": comp_dt.isoformat() if comp_dt else None,
                "latest_failure_utc": fail_dt.isoformat() if fail_dt else None,
                "record_count": rec_count,
                "arrival_delay_minutes": arrival_delay_min,
                "freshness_minutes": freshness_min,
                "sla_status": sla_status.value,
                "last_updated_run_id": run_id,
                "updated_at_utc": now_iso,
            }))

            # Deterministic Alert Evaluation & Auto-Resolution on Rerun
            active_alerts_for_key: set[AlertType] = set()

            if sla_status == SLAStatus.MISSING:
                active_alerts_for_key.add(AlertType.MISSING_LOAD)
                alert_upsert_lines.append(dumps({
                    "alert_id": make_deterministic_id("alt", ds_id, b_date_str, AlertType.MISSING_LOAD.value),
                    "dataset_id": ds_id,
                    "batch_date": b_date_str,
                    "alert_type": AlertType.MISSING_LOAD.value,
                    "severity": crit,
                    "message": f"[{ds_name}] Missing load for {b_date_str}. SLA deadline {sla_deadline_dt.isoformat()} passed.",
                    "is_resolved": False,
                    "created_at_utc": now_iso,
                    "resolved_at_utc": None,
                    "run_id": run_id,
                }))
            elif sla_status == SLAStatus.FAILED:
                active_alerts_for_key.add(AlertType.LOAD_FAILED)
                alert_upsert_lines.append(dumps({
                    "alert_id": make_deterministic_id("alt", ds_id, b_date_str, AlertType.LOAD_FAILED.value),
                    "dataset_id": ds_id,
                    "batch_date": b_date_str,
                    "alert_type": AlertType.LOAD_FAILED.value,
                    "severity": crit,
                    "message": f"[{ds_name}] Load FAILED at {fail_dt.isoformat()} for batch {b_date_str}. Owner: {owner}.",
                    "is_resolved": False,
                    "created_at_utc": now_iso,
                    "resolved_at_utc": None,
                    "run_id": run_id,
                }))
            elif sla_status == SLAStatus.LATE:
                active_alerts_for_key.add(AlertType.SLA_BREACH)
                alert_upsert_lines.append(dumps({
                    "alert_id": make_deterministic_id("alt", ds_id, b_date_str, AlertType.SLA_BREACH.value),
                    "dataset_id": ds_id,
                    "batch_date": b_date_str,
                    "alert_type": AlertType.SLA_BREACH.value,
                    "severity": crit,
                    "message": f"[{ds_name}] SLA breach for {b_date_str}: delayed by {arrival_delay_min} mins (limit {sla_win} mins).",
                    "is_resolved": False,
                    "created_at_utc": now_iso,
                    "resolved_at_utc": None,
                    "run_id": run_id,
                }))

            # Check freshness SLA only for the anchor batch date to avoid flagging historical dates
            if (
                b_date_str == anchor_date_iso
                and freshness_min is not None
                and freshness_min > max_fresh
            ):
                active_alerts_for_key.add(AlertType.STALE_DATA)
                alert_upsert_lines.append(dumps({
                    "alert_id": make_deterministic_id("alt", ds_id, b_date_str, AlertType.STALE_DATA.value),
                    "dataset_id": ds_id,
                    "batch_date": b_date_str,
                    "alert_type": AlertType.STALE_DATA.value,
                    "severity": crit,
                    "message": f"[{ds_name}] Stale data: age is {freshness_min} mins (max allowed {max_fresh} mins).",
                    "is_resolved": False,
                    "created_at_utc": now_iso,
                    "resolved_at_utc": None,
                    "run_id": run_id,
                }))

            # Resolve any prior open alerts for this (dataset_id, batch_date) that no longer apply
            for a_type in AlertType:
                if a_type not in active_alerts_for_key:
                    alert_resolution_lines.append(dumps({
                        "alert_id": make_deterministic_id("alt", ds_id, b_date_str, a_type.value),
                        "resolved_at_utc": now_iso,
                        "run_id": run_id,
                    }))

    if status_lines:
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            tmp.write("\n".join(status_lines) + "\n")
        try:
            repo.conn.execute(f"""
                INSERT INTO fact_dataset_sla_status (
                    dataset_id, batch_date, expected_arrival_utc, sla_deadline_utc,
                    actual_start_utc, actual_completion_utc, latest_failure_utc,
                    record_count, arrival_delay_minutes, freshness_minutes,
                    sla_status, last_updated_run_id, updated_at_utc
                )
                SELECT
                    dataset_id,
                    CAST(batch_date AS DATE),
                    CAST(expected_arrival_utc AS TIMESTAMPTZ),
                    CAST(sla_deadline_utc AS TIMESTAMPTZ),
                    CAST(actual_start_utc AS TIMESTAMPTZ),
                    CAST(actual_completion_utc AS TIMESTAMPTZ),
                    CAST(latest_failure_utc AS TIMESTAMPTZ),
                    CAST(record_count AS BIGINT),
                    CAST(arrival_delay_minutes AS DOUBLE),
                    CAST(freshness_minutes AS DOUBLE),
                    sla_status,
                    last_updated_run_id,
                    CAST(updated_at_utc AS TIMESTAMPTZ)
                FROM read_json('{tmp_path.as_posix()}', auto_detect=true, format='newline_delimited')
                ON CONFLICT (dataset_id, batch_date) DO UPDATE SET
                    expected_arrival_utc = EXCLUDED.expected_arrival_utc,
                    sla_deadline_utc = EXCLUDED.sla_deadline_utc,
                    actual_start_utc = EXCLUDED.actual_start_utc,
                    actual_completion_utc = EXCLUDED.actual_completion_utc,
                    latest_failure_utc = EXCLUDED.latest_failure_utc,
                    record_count = EXCLUDED.record_count,
                    arrival_delay_minutes = EXCLUDED.arrival_delay_minutes,
                    freshness_minutes = EXCLUDED.freshness_minutes,
                    sla_status = EXCLUDED.sla_status,
                    last_updated_run_id = EXCLUDED.last_updated_run_id,
                    updated_at_utc = EXCLUDED.updated_at_utc;
            """)
        finally:
            tmp_path.unlink(missing_ok=True)

    if alert_upsert_lines:
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            tmp.write("\n".join(alert_upsert_lines) + "\n")
        try:
            repo.conn.execute(f"""
                INSERT INTO fact_sla_alerts (
                    alert_id, dataset_id, batch_date, alert_type, severity,
                    message, is_resolved, created_at_utc, resolved_at_utc, run_id
                )
                SELECT
                    alert_id,
                    dataset_id,
                    CAST(batch_date AS DATE),
                    alert_type,
                    severity,
                    message,
                    CAST(is_resolved AS BOOLEAN),
                    CAST(created_at_utc AS TIMESTAMPTZ),
                    CAST(resolved_at_utc AS TIMESTAMPTZ),
                    run_id
                FROM read_json('{tmp_path.as_posix()}', auto_detect=true, format='newline_delimited')
                ON CONFLICT (alert_id) DO UPDATE SET
                    severity = EXCLUDED.severity,
                    message = EXCLUDED.message,
                    is_resolved = FALSE,
                    resolved_at_utc = NULL,
                    run_id = EXCLUDED.run_id;
            """)
        finally:
            tmp_path.unlink(missing_ok=True)

    if alert_resolution_lines:
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            tmp.write("\n".join(alert_resolution_lines) + "\n")
        try:
            repo.conn.execute(f"""
                UPDATE fact_sla_alerts AS target
                SET
                    is_resolved = TRUE,
                    resolved_at_utc = CAST(src.resolved_at_utc AS TIMESTAMPTZ),
                    run_id = src.run_id
                FROM read_json('{tmp_path.as_posix()}', auto_detect=true, format='newline_delimited') AS src
                WHERE target.alert_id = src.alert_id AND target.is_resolved = FALSE;
            """)
        finally:
            tmp_path.unlink(missing_ok=True)

    active_count = repo.conn.execute(
        "SELECT COUNT(*) FROM fact_sla_alerts WHERE is_resolved = FALSE"
    ).fetchone()[0]

    return {
        "status_rows_upserted": len(status_lines),
        "alerts_active": active_count,
    }