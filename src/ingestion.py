from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Set
from pydantic import ValidationError

from src.models import EventType, RawLoadEvent, RejectedRecord, make_deterministic_id, parse_iso_utc
from src.repository import SLARepository

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_VALID_EVENT_TYPES = {"STARTED", "COMPLETED", "FAILED"}
_ALLOWED_KEYS = {
    "event_id",
    "dataset_id",
    "batch_date",
    "event_type",
    "event_timestamp_utc",
    "record_count",
    "source_file",
}


@dataclass
class IngestionSummary:
    records_read: int
    records_valid: int
    records_rejected: int


def _fast_parse_event(parsed: dict, default_source_file: str) -> RawLoadEvent | None:
    """Fast-path validation for clean JSONL records to avoid per-row Pydantic overhead at 100k+ scale."""
    if not parsed.keys() <= _ALLOWED_KEYS:
        return None

    ev_id = parsed.get("event_id")
    ds_id = parsed.get("dataset_id")
    b_date = parsed.get("batch_date")
    ev_type = parsed.get("event_type")
    ev_ts = parsed.get("event_timestamp_utc")
    rec_cnt = parsed.get("record_count")
    src_file = parsed.get("source_file") or default_source_file

    if not (isinstance(ev_id, str) and isinstance(ds_id, str) and isinstance(b_date, str)):
        return None
    ev_id = ev_id.strip()
    ds_id = ds_id.strip()
    b_date = b_date.strip()
    if not (1 <= len(ev_id) <= 64 and 3 <= len(ds_id) <= 64 and _DATE_RE.match(b_date)):
        return None

    if not isinstance(ev_type, str):
        return None
    ev_type_clean = ev_type.strip()
    if ev_type_clean not in _VALID_EVENT_TYPES:
        return None

    if rec_cnt is not None and (isinstance(rec_cnt, bool) or not isinstance(rec_cnt, int) or rec_cnt < 0):
        return None

    if not isinstance(src_file, str) or not src_file.strip():
        return None

    try:
        dt = parse_iso_utc(ev_ts) if isinstance(ev_ts, (str, datetime)) else None
    except ValueError:
        return None
    if dt is None:
        return None

    return RawLoadEvent.model_construct(
        event_id=ev_id,
        dataset_id=ds_id,
        batch_date=b_date,
        event_type=EventType(ev_type_clean),
        event_timestamp_utc=dt,
        record_count=rec_cnt,
        source_file=src_file.strip(),
    )


def _iter_file_rows(file_path: Path):
    """Stream raw rows from .jsonl or .csv files without double-buffering."""
    suffix = file_path.suffix.lower()
    loads = json.loads

    if suffix == ".jsonl":
        with file_path.open("r", encoding="utf-8") as f:
            for idx, line in enumerate(f, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    parsed = loads(stripped)
                    if not isinstance(parsed, dict):
                        yield idx, stripped, None, "JSON root is not an object"
                    else:
                        yield idx, stripped, parsed, None
                except json.JSONDecodeError as exc:
                    yield idx, stripped, None, f"Malformed JSON: {exc.msg}"
    elif suffix == ".csv":
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for idx, record in enumerate(reader, start=2):
                raw_str = json.dumps(record)
                if None in record:
                    yield idx, raw_str, None, "Schema drift: extra unmapped CSV columns"
                else:
                    cleaned = {k: (v if v != "" else None) for k, v in record.items()}
                    yield idx, raw_str, cleaned, None
    else:
        raise ValueError(f"Unsupported file format: {suffix}. Expected .jsonl or .csv")


def ingest_events_file(
    repo: SLARepository,
    file_path: str | Path,
    run_id: str,
) -> IngestionSummary:
    """
    Ingest load events with schema validation, FK checking, deduplication,
    and quarantine routing for invalid records.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    valid_dataset_ids: Set[str] = repo.get_active_dataset_ids()
    seen_event_ids: Set[str] = repo.get_existing_event_ids()

    valid_events: List[RawLoadEvent] = []
    rejected_records: List[RejectedRecord] = []
    now_utc = datetime.now(timezone.utc)
    file_name = path.name
    records_read = 0

    for line_no, raw_payload, parsed_dict, parse_err in _iter_file_rows(path):
        records_read += 1

        if parse_err is not None or parsed_dict is None:
            reject_id = make_deterministic_id("rej", file_name, str(line_no), raw_payload)
            rejected_records.append(
                RejectedRecord(
                    reject_id=reject_id,
                    run_id=run_id,
                    source_file=file_name,
                    raw_payload=raw_payload,
                    reject_reason=parse_err or "Unparseable payload",
                    rejected_at_utc=now_utc,
                )
            )
            continue

        event = _fast_parse_event(parsed_dict, file_name)
        if event is None:
            parsed_dict.setdefault("source_file", file_name)
            try:
                event = RawLoadEvent(**parsed_dict)
            except ValidationError as exc:
                err_summary = "; ".join(
                    f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}"
                    for e in exc.errors()
                )
                reject_id = make_deterministic_id("rej", file_name, str(line_no), raw_payload)
                rejected_records.append(
                    RejectedRecord(
                        reject_id=reject_id,
                        run_id=run_id,
                        source_file=file_name,
                        raw_payload=raw_payload,
                        reject_reason=f"Schema/Validation error: {err_summary}",
                        rejected_at_utc=now_utc,
                    )
                )
                continue

        # Referential integrity check against dim_dataset_sla
        if event.dataset_id not in valid_dataset_ids:
            reject_id = make_deterministic_id("rej", file_name, str(line_no), event.event_id)
            rejected_records.append(
                RejectedRecord(
                    reject_id=reject_id,
                    run_id=run_id,
                    source_file=file_name,
                    raw_payload=raw_payload,
                    reject_reason=f"Foreign key violation: unknown dataset_id '{event.dataset_id}'",
                    rejected_at_utc=now_utc,
                )
            )
            continue

        # Duplicate event_id check (both intra-batch and cross-run)
        if event.event_id in seen_event_ids:
            reject_id = make_deterministic_id("rej", file_name, str(line_no), event.event_id, "dup")
            rejected_records.append(
                RejectedRecord(
                    reject_id=reject_id,
                    run_id=run_id,
                    source_file=file_name,
                    raw_payload=raw_payload,
                    reject_reason=f"Duplicate event_id '{event.event_id}'",
                    rejected_at_utc=now_utc,
                )
            )
            continue

        seen_event_ids.add(event.event_id)
        valid_events.append(event)

    if records_read == 0:
        return IngestionSummary(records_read=0, records_valid=0, records_rejected=0)

    inserted_valid = repo.insert_valid_events(valid_events, run_id=run_id)
    inserted_rejected = repo.insert_rejected_records(rejected_records)

    return IngestionSummary(
        records_read=records_read,
        records_valid=inserted_valid,
        records_rejected=inserted_rejected,
    )