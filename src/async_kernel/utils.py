# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import contextlib
import sys
import threading
from typing import TYPE_CHECKING, Any

import anyio
import anyio.to_thread

import async_kernel

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "do_not_debug_this_thread",
    "get_metadata",
    "get_tags",
    "mark_thread_pydev_do_not_trace",
    "wait_thread_event",
]

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules


def mark_thread_pydev_do_not_trace(thread: threading.Thread, name="", *, remove=False):
    """Modifies the given thread's attributes to hide or unhide it from the debugger (e.g., debugpy)."""
    thread.pydev_do_not_trace = not remove  # pyright: ignore[reportAttributeAccessIssue]
    if name:
        thread.name = name


@contextlib.contextmanager
def do_not_debug_this_thread(name=""):
    "A context to mark the thread for debugpy to not debug."
    if not LAUNCHED_BY_DEBUGPY:
        mark_thread_pydev_do_not_trace(threading.current_thread(), name)
    try:
        yield
    finally:
        if not LAUNCHED_BY_DEBUGPY:
            mark_thread_pydev_do_not_trace(threading.current_thread(), remove=True)


async def wait_thread_event(event: threading.Event):
    """Wait for the threading event using an anyio worker thread.

    The event will be set event if the coroutine is cancelled to ensure the thread is cleared.
    """

    def _in_thread_call():
        with do_not_debug_this_thread():
            event.wait()

    try:
        await anyio.to_thread.run_sync(_in_thread_call)
    finally:
        event.set()


def get_metadata() -> Mapping[str, Any]:
    "Gets metadata from current [`Job`][async_kernel.typing.Job] context if there is one."
    return (async_kernel.Kernel().job.get("msg") or {}).get("metadata") or {}


def get_tags() -> list[str]:
    "Gets the list of tags from current [`Job`][async_kernel.typing.Job] context if there is one."
    return get_metadata().get("tags") or []
