"""Tests for the durable atomic-write helper."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from wildwatch.state_io import atomic_write_json


def test_writes_and_reads_back(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    atomic_write_json(path, {"a": 1, "b": [1, 2, 3]})
    assert json.loads(path.read_text()) == {"a": 1, "b": [1, 2, 3]}


def test_no_tmp_file_left_behind_on_success(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    atomic_write_json(path, {"x": 1})
    assert path.exists()
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_tmp_file_cleaned_on_serialisation_failure(tmp_path: Path) -> None:
    path = tmp_path / "state.json"

    class Unserialisable:
        pass

    with pytest.raises(TypeError):
        atomic_write_json(path, {"x": Unserialisable()})
    assert not path.exists()
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_overwrites_existing_file_atomically(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text('{"old": true}')
    atomic_write_json(path, {"new": True})
    assert json.loads(path.read_text()) == {"new": True}


def test_fsync_failure_does_not_block_write(tmp_path: Path) -> None:
    """fsync errors on weird filesystems must be logged but not fatal."""
    path = tmp_path / "state.json"
    with patch("wildwatch.state_io.os.fsync", side_effect=OSError("fake fsync fail")):
        # Must not raise — write should still succeed
        atomic_write_json(path, {"k": "v"})
    assert json.loads(path.read_text()) == {"k": "v"}
