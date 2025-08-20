# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import pytest

from async_kernel import utils as ak_utils

if TYPE_CHECKING:
    from async_kernel.typing import ExecuteContent, Job


class TestUtils:
    def test_do_not_debug_this_thread(self):
        with ak_utils.do_not_debug_this_thread("test"):
            thread = threading.current_thread()
            assert thread.name == "test"
            assert getattr(thread, "pydev_do_not_trace", None)

    async def test_get_job(self, anyio_backend, job: Job[ExecuteContent]):
        with pytest.raises(LookupError):
            ak_utils._job_var.get()  # pyright: ignore[reportPrivateUsage]
        ak_utils.get_job()
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_job() is job

    async def test_get_execution_count(self, anyio_backend, job: Job[ExecuteContent]):
        assert ak_utils.get_execution_count() == 0
        ak_utils._execution_count_var.set(3)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_execution_count() == 3

    async def test_get_metadata(self, anyio_backend, job: Job[ExecuteContent]):
        assert ak_utils.get_metadata() == {}
        assert ak_utils.get_metadata(job) is job["msg"]["metadata"]
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_metadata() is job["msg"]["metadata"]

    async def test_get_parent(self, anyio_backend, job: Job[ExecuteContent]):
        assert ak_utils.get_parent() is None
        assert ak_utils.get_parent(job) is job["msg"]
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_parent(job) is job["msg"]

    async def test_get_tags(self, anyio_backend, job: Job[ExecuteContent]):
        job["msg"]["metadata"]["tags"] = tags = []  # pyright: ignore[reportIndexIssue]
        assert ak_utils.get_tags() == []
        assert ak_utils.get_tags(job) is tags

    async def test_get_execute_request_timeout(self, anyio_backend, job: Job[ExecuteContent]):
        job["msg"]["metadata"] = {"timeout": 3}
        assert ak_utils.get_execute_request_timeout(job) == 3
        ak_utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        assert ak_utils.get_execute_request_timeout() == 3
