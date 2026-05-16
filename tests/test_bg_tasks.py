"""Tests for the background-task wrapper that prevents leaks + silent deaths.

Regression for review finding: webhooks.py spawned asyncio.create_task without
add_done_callback. Successful tasks lingered in _BG_TASKS forever (slow leak);
failed tasks died silently and only surfaced as "Task exception was never
retrieved" at GC time.

The fix: a single helper that adds + removes + logs exceptions.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from wildwatch import webhooks as wh_mod


@pytest.fixture(autouse=True)
def _clear_bg_tasks() -> None:
    wh_mod._BG_TASKS.clear()
    yield
    wh_mod._BG_TASKS.clear()


@pytest.mark.asyncio
async def test_spawn_bg_removes_task_from_set_after_completion() -> None:
    async def quick() -> None:
        await asyncio.sleep(0)

    task = wh_mod._spawn_bg(quick(), label="quick-test")
    assert task in wh_mod._BG_TASKS
    await task
    # done_callback fires synchronously in the event loop iteration; yield once
    await asyncio.sleep(0)
    assert task not in wh_mod._BG_TASKS, "completed task must be removed from set"


@pytest.mark.asyncio
async def test_spawn_bg_logs_exception_instead_of_silently_dying(caplog) -> None:
    async def boom() -> None:
        raise RuntimeError("kaboom")

    with caplog.at_level(logging.ERROR, logger="wildwatch.webhooks"):
        task = wh_mod._spawn_bg(boom(), label="boom-test")
        with pytest.raises(RuntimeError):
            await task
        # done_callback queued; yield once
        await asyncio.sleep(0)

    assert task not in wh_mod._BG_TASKS
    assert any("boom-test" in r.message and "kaboom" in r.message for r in caplog.records), (
        "background task failure must be logged with label + exception detail"
    )


@pytest.mark.asyncio
async def test_spawn_bg_cancelled_task_does_not_log_error(caplog) -> None:
    """CancelledError is operator-initiated, not a real failure."""

    async def long_sleep() -> None:
        await asyncio.sleep(10)

    with caplog.at_level(logging.ERROR, logger="wildwatch.webhooks"):
        task = wh_mod._spawn_bg(long_sleep(), label="cancel-test")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)

    # No ERROR records for the cancellation
    assert not any("cancel-test" in r.message for r in caplog.records if r.levelno >= logging.ERROR)
