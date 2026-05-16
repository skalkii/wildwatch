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
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from wildwatch.state_io import atomic_write_json

logger = logging.getLogger(__name__)

STATE_FILE = Path(__file__).resolve().parent.parent / ".state.json"

KIND_VALUES: tuple[str, ...] = ("upload", "youtube", "hls", "rtsp", "rtmp")
STATUS_VALUES: tuple[str, ...] = (
    "queued",
    "connecting",
    "ingesting",
    "indexing",
    "ready",
    "error",
    "disconnected",
)


@dataclass
class Source:
    id: str
    kind: str
    input: str
    name: str
    status: str = "queued"
    progress_pct: int | None = None
    stage_msg: str | None = None
    error: str | None = None
    video_id: str | None = None
    rtstream_id: str | None = None
    indexes: dict[str, str] = field(default_factory=dict)
    credit_estimate_usd: float | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


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
    return Source(**d)


# ──── public API ──────────────────────────────────────────────────────────


def add_source(*, kind: str, input: str, name: str) -> Source:
    if kind not in KIND_VALUES:
        raise ValueError(f"invalid kind {kind!r}; must be one of {KIND_VALUES}")
    src = Source(
        id=str(uuid.uuid4()),
        kind=kind,
        input=input,
        name=name,
    )
    state = _load_state()
    state.setdefault("sources", {})[src.id] = asdict(src)
    _save_state(state)
    return src


def update_source(source_id: str, **fields: Any) -> Source:
    state = _load_state()
    sources = state.setdefault("sources", {})
    if source_id not in sources:
        raise KeyError(source_id)
    if "status" in fields and fields["status"] not in STATUS_VALUES:
        raise ValueError(f"invalid status {fields['status']!r}; must be one of {STATUS_VALUES}")
    sources[source_id].update(fields)
    sources[source_id]["updated_at"] = time.time()
    _save_state(state)
    return _from_dict(sources[source_id])


def delete_source(source_id: str) -> None:
    state = _load_state()
    sources = state.setdefault("sources", {})
    sources.pop(source_id, None)
    _save_state(state)


def get_source(source_id: str) -> Source | None:
    d = _sources_dict().get(source_id)
    return _from_dict(d) if d else None


def list_sources() -> list[Source]:
    return [_from_dict(d) for d in _sources_dict().values()]
