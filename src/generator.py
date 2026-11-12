from __future__ import annotations

import argparse
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any

from src.models import DatasetSLAConfig, Criticality


def get_default_dataset_configs() -> List[DatasetSLAConfig]:
    return [
        DatasetSLAConfig(
            dataset_id="ds_orders_core",
            dataset_name="Core E-Commerce Orders",
            owner_team="commerce-data",
            expected_arrival_hour_utc=6,
            expected_arrival_minute_utc=0,
            sla_window_minutes=30,
            max_freshness_minutes=360,
            criticality=Criticality.HIGH,
        ),
        DatasetSLAConfig(
            dataset_id="ds_payments_ledger",
            dataset_name="Payments Settlement Ledger",
            owner_team="finops-eng",
            expected_arrival_hour_utc=7,
            expected_arrival_minute_utc=0,
            sla_window_minutes=45,
            max_freshness_minutes=480,
            criticality=Criticality.HIGH,
        ),
        DatasetSLAConfig(
            dataset_id="ds_clickstream_agg",
            dataset_name="Hourly Clickstream Rollup",
            owner_team="growth-analytics",
            expected_arrival_hour_utc=8,
            expected_arrival_minute_utc=30,
            sla_window_minutes=60,
            max_freshness_minutes=720,
            criticality=Criticality.MEDIUM,
        ),
        DatasetSLAConfig(
            dataset_id="ds_inventory_snap",
            dataset_name="Warehouse Inventory Snapshot",
            owner_team="supply-chain",
            expected_arrival_hour_utc=5,
            expected_arrival_minute_utc=0,
            sla_window_minutes=30,
            max_freshness_minutes=1440,
            criticality=Criticality.MEDIUM,
        ),
        DatasetSLAConfig(
            dataset_id="ds_marketing_spend",
            dataset_name="Daily Ad Spend Attribution",
            owner_team="marketing-ops",
            expected_arrival_hour_utc=9,
            expected_arrival_minute_utc=0,
            sla_window_minutes=120,
            max_freshness_minutes=1440,
            criticality=Criticality.LOW,
        ),
    ]


def generate_starter_sample(output_dir: str | Path, seed: int = 42) -> Dict[str, Any]:
    """
    Generate a small, deterministic starter sample covering all edge cases:
    - ON_TIME load (ds_orders_core)
    - LATE / SLA_BREACH load (ds_payments_ledger)
    - OUT_OF_ORDER events arriving COMPLETED before STARTED (ds_clickstream_agg)
    - FAILED load (ds_inventory_snap)
    - MISSING load (ds_marketing_spend has no events for batch_date)
    - Invalid records: duplicate event_id, unknown dataset_id FK, schema drift, missing field, malformed JSON
    """
    random.seed(seed)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    labels_dir = out_path / "expected_labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    configs = get_default_dataset_configs()
    config_file = out_path / "dataset_sla_config.json"
    config_file.write_text(
        json.dumps([c.model_dump(mode="json") for c in configs], indent=2),
        encoding="utf-8",
    )

    batch_date = "2026-11-12"
    events_file = out_path / "sample_events.jsonl"

    lines: List[str] = [
        # 1. ds_orders_core -> ON_TIME (Expected 06:00, SLA deadline 06:30, Completed 06:18)
        json.dumps({
            "event_id": "evt_sample_001",
            "dataset_id": "ds_orders_core",
            "batch_date": batch_date,
            "event_type": "STARTED",
            "event_timestamp_utc": "2026-11-12T06:02:00Z",
            "record_count": 0,
            "source_file": "sample_events.jsonl",
        }),
        json.dumps({
            "event_id": "evt_sample_002",
            "dataset_id": "ds_orders_core",
            "batch_date": batch_date,
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:18:00Z",
            "record_count": 48210,
            "source_file": "sample_events.jsonl",
        }),

        # 2. ds_payments_ledger -> LATE / SLA_BREACH (Expected 07:00, SLA deadline 07:45, Completed 08:25)
        json.dumps({
            "event_id": "evt_sample_003",
            "dataset_id": "ds_payments_ledger",
            "batch_date": batch_date,
            "event_type": "STARTED",
            "event_timestamp_utc": "2026-11-12T07:50:00Z",
            "record_count": 0,
            "source_file": "sample_events.jsonl",
        }),
        json.dumps({
            "event_id": "evt_sample_004",
            "dataset_id": "ds_payments_ledger",
            "batch_date": batch_date,
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T08:25:00Z",
            "record_count": 19450,
            "source_file": "sample_events.jsonl",
        }),

        # 3. ds_clickstream_agg -> OUT-OF-ORDER arrival (COMPLETED appears in file before STARTED, ON_TIME)
        json.dumps({
            "event_id": "evt_sample_005",
            "dataset_id": "ds_clickstream_agg",
            "batch_date": batch_date,
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T08:45:00Z",
            "record_count": 152000,
            "source_file": "sample_events.jsonl",
        }),
        json.dumps({
            "event_id": "evt_sample_006",
            "dataset_id": "ds_clickstream_agg",
            "batch_date": batch_date,
            "event_type": "STARTED",
            "event_timestamp_utc": "2026-11-12T08:20:00Z",
            "record_count": 0,
            "source_file": "sample_events.jsonl",
        }),

        # 4. ds_inventory_snap -> FAILED load (Expected 05:00, Failed at 05:14)
        json.dumps({
            "event_id": "evt_sample_007",
            "dataset_id": "ds_inventory_snap",
            "batch_date": batch_date,
            "event_type": "STARTED",
            "event_timestamp_utc": "2026-11-12T05:01:00Z",
            "record_count": 0,
            "source_file": "sample_events.jsonl",
        }),
        json.dumps({
            "event_id": "evt_sample_008",
            "dataset_id": "ds_inventory_snap",
            "batch_date": batch_date,
            "event_type": "FAILED",
            "event_timestamp_utc": "2026-11-12T05:14:00Z",
            "record_count": 0,
            "source_file": "sample_events.jsonl",
        }),

        # --- DELIBERATELY INVALID / EDGE-CASE RECORDS (5 rejects) ---
        # Reject A: Duplicate event_id (evt_sample_002)
        json.dumps({
            "event_id": "evt_sample_002",
            "dataset_id": "ds_orders_core",
            "batch_date": batch_date,
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:18:00Z",
            "record_count": 48210,
            "source_file": "sample_events.jsonl",
        }),
        # Reject B: Invalid dataset_id foreign key
        json.dumps({
            "event_id": "evt_sample_bad_fk",
            "dataset_id": "ds_unregistered_shadow_table",
            "batch_date": batch_date,
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:10:00Z",
            "record_count": 100,
            "source_file": "sample_events.jsonl",
        }),
        # Reject C: Unexpected schema drift (extra unknown column)
        json.dumps({
            "event_id": "evt_sample_drift",
            "dataset_id": "ds_orders_core",
            "batch_date": batch_date,
            "event_type": "COMPLETED",
            "event_timestamp_utc": "2026-11-12T06:20:00Z",
            "record_count": 100,
            "unexpected_new_column": "drift_value",
            "source_file": "sample_events.jsonl",
        }),
        # Reject D: Missing required field (event_timestamp_utc omitted)
        json.dumps({
            "event_id": "evt_sample_missing_ts",
            "dataset_id": "ds_orders_core",
            "batch_date": batch_date,
            "event_type": "STARTED",
            "source_file": "sample_events.jsonl",
        }),
        # Reject E: Malformed JSON line
        '{"event_id": "evt_sample_corrupt", "dataset_id": "ds_orders_core", BROKEN_JSON',
    ]

    events_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Separate ground-truth expectations (kept strictly outside input directory)
    expected_summary = {
        "evaluation_anchor_utc": "2026-11-12T12:00:00Z",
        "batch_date": batch_date,
        "reconciliation": {
            "records_read": 13,
            "records_valid": 8,
            "records_rejected": 5,
        },
        "expected_dataset_statuses": {
            "ds_orders_core": "ON_TIME",
            "ds_payments_ledger": "LATE",
            "ds_clickstream_agg": "ON_TIME",
            "ds_inventory_snap": "FAILED",
            "ds_marketing_spend": "MISSING",
        },
        "expected_alert_types": [
            "SLA_BREACH",
            "LOAD_FAILED",
            "MISSING_LOAD",
        ],
    }
    (labels_dir / "sample_expected_summary.json").write_text(
        json.dumps(expected_summary, indent=2), encoding="utf-8"
    )
    return expected_summary


def generate_scale_dataset(
    output_dir: str | Path,
    num_records: int = 100_000,
    seed: int = 42,
) -> Path:
    """
    Generate a large reproducible JSONL dataset (default 100,000 records)
    with controlled defect injection (~1% invalid/duplicate records) for benchmarking.
    """
    rng = random.Random(seed)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    configs = get_default_dataset_configs()
    dataset_ids = [c.dataset_id for c in configs]
    base_date = date(2026, 1, 1)

    target_file = out_path / f"scale_events_{num_records}.jsonl"
    with target_file.open("w", encoding="utf-8") as f:
        for i in range(num_records):
            # Inject 0.5% duplicate event_ids and 0.5% invalid dataset_ids
            if i > 0 and rng.random() < 0.005:
                evt_id = f"evt_scale_{i - 1:07d}"
            else:
                evt_id = f"evt_scale_{i:07d}"

            if rng.random() < 0.005:
                ds_id = "ds_invalid_ref_key"
            else:
                ds_id = dataset_ids[i % len(dataset_ids)]

            day_offset = (i // 10) % 300
            b_date = base_date + timedelta(days=day_offset)
            evt_type = "STARTED" if i % 2 == 0 else "COMPLETED"
            ts = datetime(
                b_date.year, b_date.month, b_date.day,
                rng.randint(4, 11), rng.randint(0, 59), rng.randint(0, 59),
                tzinfo=timezone.utc,
            )

            row = {
                "event_id": evt_id,
                "dataset_id": ds_id,
                "batch_date": b_date.isoformat(),
                "event_type": evt_type,
                "event_timestamp_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "record_count": 0 if evt_type == "STARTED" else rng.randint(1000, 250000),
                "source_file": target_file.name,
            }
            f.write(json.dumps(row) + "\n")

    return target_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic SLA monitoring datasets.")
    parser.add_argument("--mode", choices=["sample", "scale", "both"], default="both")
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.mode in ("sample", "both"):
        summary = generate_starter_sample("data/sample", seed=args.seed)
        print(f"[OK] Starter sample generated -> data/sample/ (expected valid={summary['reconciliation']['records_valid']}, rejects={summary['reconciliation']['records_rejected']})")

    if args.mode in ("scale", "both"):
        path = generate_scale_dataset("data/generated", num_records=args.records, seed=args.seed)
        print(f"[OK] Scale dataset ({args.records:,} rows) generated -> {path}")