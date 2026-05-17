"""Source registry — UI-managed videos / URLs / live streams.

Single state lives at ``.state.json["sources"]`` keyed by a uuid4 source id.
Each Source is one row the user added through the dashboard (uploaded file,
pasted URL, or live RTSP/RTMP).

Persistence uses the atomic write pattern (.tmp + rename) so a crash
mid-write cannot corrupt the file. Migration is automatic — if state has
no ``sources`` key (old file), ``list_sources()`` returns ``[]`` and the
first ``add_source`` creates the key.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Literal, get_args

from wildwatch.state_io import atomic_write_json

logger = logging.getLogger(__name__)

STATE_FILE = Path(__file__).resolve().parent.parent / ".state.json"

# Concurrent ingest tasks (reconnect + progress emit) can interleave a
# load/update/save sequence on .state.json. Without this lock the second
# writer's load happens AFTER the first's save → first's update is lost.
# threading.Lock (not asyncio.Lock) because _load_state/_save_state do
# blocking IO and can be called from sync route handlers AND from
# ingest.py background tasks running on the asyncio thread.
_STATE_LOCK = threading.Lock()

SourceKind = Literal["upload", "youtube", "hls", "rtsp", "rtmp"]
SourceStatus = Literal[
    "queued", "connecting", "ingesting", "indexing", "ready", "error", "disconnected"
]
# Runtime tuples for validation (Pydantic/dataclass enforcement is overkill
# for hackathon scope; we validate in add_source / update_source instead).
KIND_VALUES: tuple[str, ...] = get_args(SourceKind)
STATUS_VALUES: tuple[str, ...] = get_args(SourceStatus)


@dataclass
class Source:
    id: str
    kind: SourceKind
    input: str
    name: str
    status: SourceStatus = "queued"
    progress_pct: int | None = None
    stage_msg: str | None = None
    error: str | None = None
    video_id: str | None = None
    rtstream_id: str | None = None
    indexes: dict[str, str] = field(default_factory=dict)
    credit_estimate_usd: float | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


# Whitelist of fields update_source accepts. **fields was untyped which
# let any caller corrupt arbitrary fields silently. This list is the
# single source of truth — adding a field to Source requires adding it
# here too, which is the right kind of friction.
_UPDATABLE_FIELDS: frozenset[str] = frozenset(
    {
        "kind",
        "input",
        "name",
        "status",
        "progress_pct",
        "stage_msg",
        "error",
        "video_id",
        "rtstream_id",
        "indexes",
        "credit_estimate_usd",
    }
)
_SOURCE_FIELD_NAMES: frozenset[str] = frozenset(f.name for f in fields(Source))


# ──── state helpers ───────────────────────────────────────────────────────


def _load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        logger.warning("state file %s corrupt; treating as empty", STATE_FILE)
        return {}


def _save_state(state: dict[str, Any]) -> None:
    atomic_write_json(STATE_FILE, state)


def _sources_dict() -> dict[str, dict]:
    return _load_state().get("sources", {})


def _from_dict(d: dict) -> Source:
    # Filter to known fields so a forward-migrated .state.json with extra
    # keys (or a stale dict from an old version) doesn't crash with
    # `TypeError: unexpected keyword argument`.
    return Source(**{k: v for k, v in d.items() if k in _SOURCE_FIELD_NAMES})


# ──── public API ──────────────────────────────────────────────────────────


def add_source(*, kind: str, input: str, name: str) -> Source:
    if kind not in KIND_VALUES:
        raise ValueError(f"invalid kind {kind!r}; must be one of {KIND_VALUES}")
    src = Source(
        id=str(uuid.uuid4()),
        kind=kind,  # type: ignore[arg-type]  -- validated above
        input=input,
        name=name,
    )
    with _STATE_LOCK:
        state = _load_state()
        state.setdefault("sources", {})[src.id] = asdict(src)
        _save_state(state)
    return src


def update_source(source_id: str, **fields: Any) -> Source:
    # Whitelist enforcement: anything not in _UPDATABLE_FIELDS is silently
    # rejected with a loud error — closes the type-design HIGH where
    # `update_source(sid, indexes="oops")` could corrupt a typed field.
    bad = set(fields) - _UPDATABLE_FIELDS
    if bad:
        raise ValueError(
            f"update_source: unknown / non-updatable fields: {sorted(bad)}. "
            f"Allowed: {sorted(_UPDATABLE_FIELDS)}"
        )
    if "status" in fields and fields["status"] not in STATUS_VALUES:
        raise ValueError(f"invalid status {fields['status']!r}; must be one of {STATUS_VALUES}")
    if "kind" in fields and fields["kind"] not in KIND_VALUES:
        raise ValueError(f"invalid kind {fields['kind']!r}; must be one of {KIND_VALUES}")

    with _STATE_LOCK:
        state = _load_state()
        sources = state.setdefault("sources", {})
        if source_id not in sources:
            raise KeyError(source_id)
        sources[source_id].update(fields)
        sources[source_id]["updated_at"] = time.time()
        _save_state(state)
        return _from_dict(sources[source_id])


def delete_source(source_id: str) -> None:
    with _STATE_LOCK:
        state = _load_state()
        sources = state.setdefault("sources", {})
        sources.pop(source_id, None)
        _save_state(state)


def get_source(source_id: str) -> Source | None:
    d = _sources_dict().get(source_id)
    return _from_dict(d) if d else None


def list_sources() -> list[Source]:
    return [_from_dict(d) for d in _sources_dict().values()]
