# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.


from async_kernel import Kernel
from async_kernel.typing import MetadataKeys, MsgType, RunMode, Tags


class TestRunMode:
    def test_str(self):
        assert str(RunMode.task) == RunMode.task

    def test_repr(self):
        assert repr(RunMode.task) == RunMode.task

    def test_hash(self):
        assert hash(RunMode.task) == hash(RunMode.task)

    def test_members(self):
        assert list(RunMode) == ["queue", "task", "thread", "blocking"]
        assert list(RunMode) == ["##queue", "##task", "##thread", "##blocking"]
        assert list(RunMode) == [
            "<RunMode.queue: 'queue'>",
            "<RunMode.task: 'task'>",
            "<RunMode.thread: 'thread'>",
            "<RunMode.blocking: 'blocking'>",
        ]


class TestMetadataKeys:
    def test_str(self):
        assert str(MetadataKeys.suppress_error_message) == MetadataKeys.suppress_error_message
        assert MetadataKeys.suppress_error_message == MetadataKeys.suppress_error_message.name


class TestMsgType:
    def test_all_names(self):
        assert set(MsgType).intersection(vars(Kernel))


class TestTags:
    def test_equality(self):
        assert Tags.suppress_error == str(Tags.suppress_error)
        assert Tags.suppress_error == Tags.suppress_error.name

    def test_hash(self):
        assert hash(Tags.suppress_error) == hash(Tags.suppress_error)
