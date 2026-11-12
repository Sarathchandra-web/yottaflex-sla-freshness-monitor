import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import duckdb
from src.models import DatasetSLAConfig

try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


class SLARepository:
    """Embedded DuckDB storage layer managing DDL, bulk upserts, and run metadata."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self.create_tables()

    def create_tables(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_dataset_sla (
                dataset_id VARCHAR PRIMARY KEY,
                dataset_name VARCHAR NOT NULL,
                owner_team VARCHAR NOT NULL,
                expected_arrival_hour_utc INTEGER NOT NULL,
                expected_arrival_minute_utc INTEGER NOT NULL,
                sla_window_minutes INTEGER NOT NULL,
                max_freshness_minutes INTEGER NOT NULL,
                criticality VARCHAR NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at_utc TIMESTAMPTZ NOT NULL
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ops_pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                started_at_utc TIMESTAMPTZ NOT NULL,
                ended_at_utc TIMESTAMPTZ,
                duration_seconds DOUBLE NOT NULL DEFAULT 0.0,
                status VARCHAR NOT NULL,
                input_sources VARCHAR NOT NULL DEFAULT '[]',
                records_read BIGINT NOT NULL DEFAULT 0,
                records_valid BIGINT NOT NULL DEFAULT 0,
                records_rejected BIGINT NOT NULL DEFAULT 0,
                alerts_active BIGINT NOT NULL DEFAULT 0,
                error_summary VARCHAR
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_load_events (
                event_id VARCHAR PRIMARY KEY,
                dataset_id VARCHAR NOT NULL,
                batch_date DATE NOT NULL,
                event_type VARCHAR NOT NULL,
                event_timestamp_utc TIMESTAMPTZ NOT NULL,
                record_count BIGINT,
                source_file VARCHAR NOT NULL,
                ingested_run_id VARCHAR NOT NULL,
                ingested_at_utc TIMESTAMPTZ NOT NULL
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS quarantine_rejected_records (
                reject_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                source_file VARCHAR NOT NULL,
                raw_payload VARCHAR NOT NULL,
                reject_reason VARCHAR NOT NULL,
                rejected_at_utc TIMESTAMPTZ NOT NULL
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_dataset_sla_status (
                dataset_id VARCHAR NOT NULL,
                batch_date DATE NOT NULL,
                expected_arrival_utc TIMESTAMPTZ NOT NULL,
                sla_deadline_utc TIMESTAMPTZ NOT NULL,
                actual_start_utc TIMESTAMPTZ,
                actual_completion_utc TIMESTAMPTZ,
                latest_failure_utc TIMESTAMPTZ,
                record_count BIGINT,
                arrival_delay_minutes DOUBLE,
                freshness_minutes DOUBLE,
                sla_status VARCHAR NOT NULL,
                last_updated_run_id VARCHAR NOT NULL,
                updated_at_utc TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (dataset_id, batch_date)
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_sla_alerts (
                alert_id VARCHAR PRIMARY KEY,
                dataset_id VARCHAR NOT NULL,
                batch_date DATE NOT NULL,
                alert_type VARCHAR NOT NULL,
                severity VARCHAR NOT NULL,
                message VARCHAR NOT NULL,
                is_resolved BOOLEAN NOT NULL DEFAULT FALSE,
                created_at_utc TIMESTAMPTZ NOT NULL,
                resolved_at_utc TIMESTAMPTZ,
                run_id VARCHAR NOT NULL
            );
        """)

    def upsert_dataset_configs(self, configs: list[DatasetSLAConfig]) -> int:
        if not configs:
            return 0
        now_utc = datetime.now(timezone.utc)
        rows = [
            (
                c.dataset_id,
                c.dataset_name,
                c.owner_team,
                c.expected_arrival_hour_utc,
                c.expected_arrival_minute_utc,
                c.sla_window_minutes,
                c.max_freshness_minutes,
                c.criticality.value if hasattr(c.criticality, "value") else str(c.criticality),
                c.is_active,
                now_utc,
            )
            for c in configs
        ]
        self.conn.executemany(
            """
            INSERT INTO dim_dataset_sla (
                dataset_id, dataset_name, owner_team,
                expected_arrival_hour_utc, expected_arrival_minute_utc,
                sla_window_minutes, max_freshness_minutes,
                criticality, is_active, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (dataset_id) DO UPDATE SET
                dataset_name = EXCLUDED.dataset_name,
                owner_team = EXCLUDED.owner_team,
                expected_arrival_hour_utc = EXCLUDED.expected_arrival_hour_utc,
                expected_arrival_minute_utc = EXCLUDED.expected_arrival_minute_utc,
                sla_window_minutes = EXCLUDED.sla_window_minutes,
                max_freshness_minutes = EXCLUDED.max_freshness_minutes,
                criticality = EXCLUDED.criticality,
                is_active = EXCLUDED.is_active,
                updated_at_utc = EXCLUDED.updated_at_utc;
            """,
            rows,
        )
        return len(rows)

    def get_active_dataset_ids(self) -> set[str]:
        rows = self.conn.execute(
            "SELECT dataset_id FROM dim_dataset_sla WHERE is_active = TRUE"
        ).fetchall()
        return {r[0] for r in rows}

    def get_active_datasets(self) -> list[dict[str, Any]]:
        cursor = self.conn.execute(
            """
            SELECT
                dataset_id, dataset_name, owner_team,
                expected_arrival_hour_utc, expected_arrival_minute_utc,
                sla_window_minutes, max_freshness_minutes, criticality
            FROM dim_dataset_sla
            WHERE is_active = TRUE
            ORDER BY dataset_id
            """
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_existing_event_ids(self) -> set[str]:
        rows = self.conn.execute(
            "SELECT event_id FROM raw_load_events"
        ).fetchall()
        return {r[0] for r in rows}

    def insert_valid_events(self, events: list[Any], run_id: str | None = None) -> int:
        if not events:
            return 0
        now_dt = datetime.now(timezone.utc)
        rid = run_id or ""

        if HAS_PYARROW:
            n = len(events)
            event_ids = [None] * n
            dataset_ids = [None] * n
            batch_dates = [None] * n
            event_types = [None] * n
            event_ts = [None] * n
            record_counts = [None] * n
            source_files = [None] * n
            run_ids = [rid] * n
            ing_ts = [now_dt] * n

            for i, e in enumerate(events):
                event_ids[i] = e.event_id
                dataset_ids[i] = e.dataset_id
                batch_dates[i] = e.batch_date
                et = e.event_type
                event_types[i] = et.value if hasattr(et, "value") else str(et)
                event_ts[i] = e.event_timestamp_utc
                record_counts[i] = e.record_count
                source_files[i] = e.source_file

            arrow_tbl = pa.table({
                "event_id": event_ids,
                "dataset_id": dataset_ids,
                "batch_date": batch_dates,
                "event_type": event_types,
                "event_timestamp_utc": event_ts,
                "record_count": pa.array(record_counts, type=pa.int64()),
                "source_file": source_files,
                "ingested_run_id": run_ids,
                "ingested_at_utc": ing_ts,
            })
            before = self.conn.execute("SELECT COUNT(*) FROM raw_load_events").fetchone()[0]
            self.conn.register("_tmp_valid_events_arrow", arrow_tbl)
            try:
                self.conn.execute("""
                    INSERT INTO raw_load_events
                    SELECT
                        event_id,
                        dataset_id,
                        CAST(batch_date AS DATE),
                        event_type,
                        CAST(event_timestamp_utc AS TIMESTAMPTZ),
                        record_count,
                        source_file,
                        ingested_run_id,
                        CAST(ingested_at_utc AS TIMESTAMPTZ)
                    FROM _tmp_valid_events_arrow
                    ON CONFLICT (event_id) DO NOTHING;
                """)
            finally:
                self.conn.unregister("_tmp_valid_events_arrow")
            after = self.conn.execute("SELECT COUNT(*) FROM raw_load_events").fetchone()[0]
            return int(after - before)

        now_iso = now_dt.isoformat()
        dumps = json.dumps
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            lines = [
                dumps({
                    "event_id": e.event_id,
                    "dataset_id": e.dataset_id,
                    "batch_date": e.batch_date,
                    "event_type": e.event_type.value if hasattr(e.event_type, "value") else str(e.event_type),
                    "event_timestamp_utc": e.event_timestamp_utc.isoformat(),
                    "record_count": e.record_count,
                    "source_file": e.source_file,
                    "ingested_run_id": rid,
                    "ingested_at_utc": now_iso,
                })
                for e in events
            ]
            tmp.write("\n".join(lines) + "\n")
        try:
            before = self.conn.execute("SELECT COUNT(*) FROM raw_load_events").fetchone()[0]
            self.conn.execute(
                f"""
                INSERT INTO raw_load_events
                SELECT
                    event_id,
                    dataset_id,
                    CAST(batch_date AS DATE),
                    event_type,
                    CAST(event_timestamp_utc AS TIMESTAMPTZ),
                    CAST(record_count AS BIGINT),
                    source_file,
                    ingested_run_id,
                    CAST(ingested_at_utc AS TIMESTAMPTZ)
                FROM read_json('{tmp_path.as_posix()}', auto_detect=true, format='newline_delimited')
                ON CONFLICT (event_id) DO NOTHING;
                """
            )
            after = self.conn.execute("SELECT COUNT(*) FROM raw_load_events").fetchone()[0]
            return int(after - before)
        finally:
            tmp_path.unlink(missing_ok=True)

    def insert_rejected_records(self, records: list[Any]) -> int:
        if not records:
            return 0
        now_iso = datetime.now(timezone.utc).isoformat()
        dumps = json.dumps
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as tmp:
            tmp_path = Path(tmp.name)
            lines = [
                dumps({
                    "reject_id": r.reject_id,
                    "run_id": r.run_id,
                    "source_file": r.source_file,
                    "raw_payload": r.raw_payload,
                    "reject_reason": r.reject_reason,
                    "rejected_at_utc": r.rejected_at_utc.isoformat() if hasattr(r.rejected_at_utc, "isoformat") else now_iso,
                })
                for r in records
            ]
            tmp.write("\n".join(lines) + "\n")
        try:
            before = self.conn.execute("SELECT COUNT(*) FROM quarantine_rejected_records").fetchone()[0]
            self.conn.execute(
                f"""
                INSERT INTO quarantine_rejected_records
                SELECT
                    reject_id,
                    run_id,
                    source_file,
                    raw_payload,
                    reject_reason,
                    CAST(rejected_at_utc AS TIMESTAMPTZ)
                FROM read_json('{tmp_path.as_posix()}', auto_detect=true, format='newline_delimited')
                ON CONFLICT (reject_id) DO NOTHING;
                """
            )
            after = self.conn.execute("SELECT COUNT(*) FROM quarantine_rejected_records").fetchone()[0]
            return int(after - before)
        finally:
            tmp_path.unlink(missing_ok=True)

    def record_pipeline_run(self, run_data: dict[str, Any]) -> None:
        sources = run_data.get("input_sources", [])
        sources_str = json.dumps(sources) if not isinstance(sources, str) else sources
        self.conn.execute(
            """
            INSERT INTO ops_pipeline_runs (
                run_id, started_at_utc, ended_at_utc, duration_seconds, status,
                input_sources, records_read, records_valid, records_rejected,
                alerts_active, error_summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id) DO UPDATE SET
                ended_at_utc = EXCLUDED.ended_at_utc,
                duration_seconds = EXCLUDED.duration_seconds,
                status = EXCLUDED.status,
                records_read = EXCLUDED.records_read,
                records_valid = EXCLUDED.records_valid,
                records_rejected = EXCLUDED.records_rejected,
                alerts_active = EXCLUDED.alerts_active,
                error_summary = EXCLUDED.error_summary
            """,
            [
                run_data["run_id"],
                run_data["started_at_utc"],
                run_data.get("ended_at_utc"),
                run_data.get("duration_seconds", 0.0),
                run_data["status"],
                sources_str,
                run_data.get("records_read", 0),
                run_data.get("records_valid", 0),
                run_data.get("records_rejected", 0),
                run_data.get("alerts_active", 0),
                run_data.get("error_summary"),
            ],
        )

    def close(self) -> None:
        self.conn.close()