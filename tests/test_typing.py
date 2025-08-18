# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.


from async_kernel.typing import RunMode


class TestRunMode:
    def test_str(self):
        assert str(RunMode.task) == RunMode.task

    def test_repr(self):
        assert repr(RunMode.task) == RunMode.task

    def test_hash(self):
        assert hash(RunMode.task) == hash(RunMode.task)

    def test_members(self):
        assert list(RunMode) == ["queue", "task", "thread", "direct"]
        assert list(RunMode) == ["##queue", "##task", "##thread", "##direct"]
        assert list(RunMode) == [
            "<RunMode.queue: 'queue'>",
            "<RunMode.task: 'task'>",
            "<RunMode.thread: 'thread'>",
            "<RunMode.direct: 'direct'>",
        ]
