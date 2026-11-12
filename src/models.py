from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict


class EventType(str, Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Criticality(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class SLAStatus(str, Enum):
    ON_TIME = "ON_TIME"
    LATE = "LATE"
    MISSING = "MISSING"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"


class AlertType(str, Enum):
    MISSING_LOAD = "MISSING_LOAD"
    SLA_BREACH = "SLA_BREACH"
    STALE_DATA = "STALE_DATA"
    LOAD_FAILED = "LOAD_FAILED"


def make_deterministic_id(prefix: str, *parts: str) -> str:
    """Create a stable, deterministic ID from canonical key components."""
    raw = "|".join(str(p).strip() for p in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def parse_iso_utc(value: str | datetime) -> datetime:
    """Parse ISO-8601 string or datetime and normalize strictly to UTC."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.strip())
    else:
        raise ValueError(f"Unsupported timestamp type: {type(value)}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class DatasetSLAConfig(BaseModel):
    """Master SLA configuration per dataset (dim_dataset_sla)."""
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    dataset_id: str = Field(..., min_length=3, max_length=64)
    dataset_name: str = Field(..., min_length=3, max_length=128)
    owner_team: str = Field(..., min_length=2, max_length=64)
    expected_arrival_hour_utc: int = Field(..., ge=0, le=23)
    expected_arrival_minute_utc: int = Field(..., ge=0, le=59)
    sla_window_minutes: int = Field(..., gt=0, le=1440)
    max_freshness_minutes: int = Field(..., gt=0, le=10080)
    criticality: Criticality = Criticality.MEDIUM
    is_active: bool = True


class RawLoadEvent(BaseModel):
    """Incoming load lifecycle event (raw_load_events). Rejects unexpected schema drift."""
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(..., min_length=1, max_length=64)
    dataset_id: str = Field(..., min_length=3, max_length=64)
    batch_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    event_type: EventType
    event_timestamp_utc: datetime
    record_count: Optional[int] = Field(default=None, ge=0)
    source_file: str = Field(default="unknown", min_length=1)

    @field_validator("event_timestamp_utc", mode="before")
    @classmethod
    def validate_utc_timestamp(cls, v: str | datetime) -> datetime:
        return parse_iso_utc(v)


class RejectedRecord(BaseModel):
    """Quarantine record preserving full lineage for anomalous/invalid inputs."""
    reject_id: str
    run_id: str
    source_file: str
    raw_payload: str
    reject_reason: str
    rejected_at_utc: datetime