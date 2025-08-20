# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import contextlib
import sys
import threading
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

import anyio
import anyio.to_thread

import async_kernel
from async_kernel.typing import Message, MetadataKeys

if TYPE_CHECKING:
    from collections.abc import Mapping

    from async_kernel.kernel import Kernel
    from async_kernel.typing import Job

__all__ = [
    "do_not_debug_this_thread",
    "get_execute_request_timeout",
    "get_execution_count",
    "get_job",
    "get_metadata",
    "get_parent",
    "get_tags",
    "mark_thread_pydev_do_not_trace",
    "wait_thread_event",
]

LAUNCHED_BY_DEBUGPY = "debugpy" in sys.modules

_job_var = ContextVar("job")
_execution_count_var: ContextVar[int] = ContextVar("execution_count")
_execute_request_timeout: ContextVar[float | None] = ContextVar("execute_request_timeout", default=None)


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


def get_kernel() -> Kernel:
    "Get the current kernel."
    return async_kernel.Kernel()


def get_job() -> Job[dict] | dict:
    "Get the job for the current context."
    try:
        return _job_var.get()
    except Exception:
        return {}


def get_parent(job: Job | None = None, /) -> Message[dict[str, Any]] | None:
    "Get the [parent message]() for the current context."
    return (job or get_job()).get("msg")


def get_metadata(job: Job | None = None, /) -> Mapping[str, Any]:
    "Gets [metadata]() for the current context."
    return (job or get_job()).get("msg", {}).get("metadata", {})


def get_tags(job: Job | None = None, /) -> list[str]:
    "Gets the [tags]() for the current context."
    return get_metadata(job).get("tags", [])


def get_execute_request_timeout(job: Job | None = None, /) -> float | None:
    "Gets the execute_request_timeout for the current context."
    try:
        if timeout := get_metadata(job).get(MetadataKeys.timeout):
            return float(timeout)
        return get_kernel().cell_execute_timeout
    except Exception:
        return None


def get_execution_count() -> int:
    "Gets the execution count for the current context, defaults to the current kernel count."

    return _execution_count_var.get(None) or async_kernel.Kernel()._execution_count  # pyright: ignore[reportPrivateUsage]
