import unittest.mock

from asynckernel.comm import Comm, CommManager


async def test_comm(kernel) -> None:
    assert isinstance(kernel.comm_manager, CommManager)
    c = Comm(target_name="bar")
    msgs = []

    assert c.kernel is kernel

    def on_close(msg):
        msgs.append(msg)

    def on_message(msg):
        msgs.append(msg)

    c.publish_msg("foo")
    c.open({})
    c.on_msg(on_message)
    c.on_close(on_close)
    c.handle_msg({})
    c.handle_close({})
    c.close()
    assert len(msgs) == 2
    assert c.target_name == "bar"


async def test_comm_manager(kernel) -> None:
    manager = kernel.comm_manager
    msgs = []

    def foo(comm, msg):
        msgs.append(msg)
        comm.close()

    def fizz(comm, msg):
        msg = "hi"
        raise RuntimeError(msg)

    def on_close(msg):
        msgs.append(msg)

    def on_msg(msg):
        msgs.append(msg)

    manager.register_target("foo", foo)
    manager.register_target("fizz", fizz)

    kernel.comm_manager = manager
    with unittest.mock.patch.object(Comm, "publish_msg") as publish_msg:
        comm = Comm()
        comm.on_msg(on_msg)
        comm.on_close(on_close)
        manager.register_comm(comm)
        assert publish_msg.call_count == 1

    assert manager.get_comm(comm.comm_id) == comm
    assert manager.get_comm("foo") is None

    msg = {"content": {"comm_id": comm.comm_id, "target_name": "foo"}}
    manager.comm_open(None, None, msg)
    assert len(msgs) == 1
    msg["content"]["target_name"] = "bar"
    manager.comm_open(None, None, msg)
    assert len(msgs) == 1
    msg = {"content": {"comm_id": comm.comm_id, "target_name": "fizz"}}
    manager.comm_open(None, None, msg)
    assert len(msgs) == 1

    manager.register_comm(comm)
    assert manager.get_comm(comm.comm_id) == comm
    msg = {"content": {"comm_id": comm.comm_id}}
    manager.comm_msg(None, None, msg)
    assert len(msgs) == 2
    msg["content"]["comm_id"] = "foo"
    manager.comm_msg(None, None, msg)
    assert len(msgs) == 2

    manager.register_comm(comm)
    assert manager.get_comm(comm.comm_id) == comm
    msg = {"content": {"comm_id": comm.comm_id}}
    manager.comm_close(None, None, msg)
    assert len(msgs) == 3

    assert comm._closed
