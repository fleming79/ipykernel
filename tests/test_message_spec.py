"""Test suite for our zeromq-based message specification."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import re
import sys
from queue import Empty

import anyio
import pytest
from jupyter_client._version import version_info
from jupyter_client.asynchronous.client import AsyncKernelClient
from packaging.version import Version as V
from traitlets import Bool, Dict, Enum, HasTraits, Integer, List, TraitError, Unicode, observe

from tests import utils
from tests.utils import execute, get_reply


class Reference(HasTraits):
    """
    Base class for message spec specification testing.

    This class is the core of the message specification test.  The
    idea is that child classes implement trait attributes for each
    message keys, so that message keys can be tested against these
    traits using :meth:`check` method.

    """

    def __str__(self):
        return str(self.__class__)

    def check(self, d):
        """validate a dict against our traits"""
        for key in self.trait_names():
            if key not in d:
                raise KeyError(f"{key=} is missing for {self} in {d=}")
            # FIXME: always allow None, probably not a good idea
            if d[key] is None:
                continue
            try:
                setattr(self, key, d[key])
            except TraitError as e:
                e.add_note(f"Validation failed for {key=} with  value:{d[key]}")
                raise


class Version(Unicode):
    def __init__(self, *args, **kwargs):
        self.min = kwargs.pop("min", None)
        self.max = kwargs.pop("max", None)
        kwargs["default_value"] = self.min
        super().__init__(*args, **kwargs)

    def validate(self, obj, value):
        if self.min and V(value) < V(self.min):
            msg = f"bad version: {value} < {self.min}"
            raise TraitError(msg)
        if self.max and (V(value) > V(self.max)):
            msg = f"bad version: {value} > {self.max}"
            raise TraitError(msg)


class RMessage(Reference):
    msg_id = Unicode()
    msg_type = Unicode()
    header = Dict()
    parent_header = Dict()
    content = Dict()

    def check(self, d):
        super().check(d)
        RHeader().check(self.header)
        if self.parent_header:
            RHeader().check(self.parent_header)


class RHeader(Reference):
    msg_id = Unicode()
    msg_type = Unicode()
    session = Unicode()
    username = Unicode()
    version = Version(min="5.0")


mime_pat = re.compile(r"^[\w\-\+\.]+/[\w\-\+\.]+$")


class MimeBundle(Reference):
    metadata = Dict()
    data = Dict()

    @observe("data")
    def _on_data_changed(self, change):
        for k, v in change["new"].items():
            assert mime_pat.match(k)
            assert isinstance(v, str)


# shell replies
class Reply(Reference):
    status = Enum(("ok", "error"), default_value="ok")


class ExecuteReply(Reply):
    execution_count = Integer()

    def check(self, d):
        super().check(d)
        if d["status"] == "ok":
            ExecuteReplyOkay().check(d)
        elif d["status"] == "error":
            ExecuteReplyError().check(d)
        elif d["status"] == "aborted":
            "Deprectated"
            raise NotImplementedError
            ExecuteReplyAborted().check(d)


class ExecuteReplyOkay(Reply):
    status = Enum("ok")
    user_expressions = Dict()


class ExecuteReplyError(Reply):
    status = Enum("error")
    ename = Unicode()
    evalue = Unicode()
    traceback = List(Unicode())


class ExecuteReplyAborted(Reply):
    status = Enum("aborted")


class InspectReply(Reply, MimeBundle):
    found = Bool()


class ArgSpec(Reference):
    args = List(Unicode())
    varargs = Unicode()
    varkw = Unicode()
    defaults = List()


class Status(Reference):
    execution_state = Enum(("busy", "idle", "starting"), default_value="busy")


class CompleteReply(Reply):
    matches = List(Unicode())
    cursor_start = Integer()
    cursor_end = Integer()
    status = Unicode()  # type:ignore


class LanguageInfo(Reference):
    name = Unicode("python")
    version = Unicode(sys.version.split()[0])


class KernelInfoReply(Reply):
    protocol_version = Version(min="5.0")
    implementation = Unicode("ipython")
    implementation_version = Version(min="2.1")
    language_info = Dict()
    banner = Unicode()

    def check(self, d):
        super().check(d)
        LanguageInfo().check(d["language_info"])


class ConnectReply(Reference):
    shell_port = Integer()
    control_port = Integer()
    stdin_port = Integer()
    iopub_port = Integer()
    hb_port = Integer()


class CommInfoReply(Reply):
    comms = Dict()


class IsCompleteReply(Reference):
    status = Enum(("complete", "incomplete", "invalid", "unknown"), default_value="complete")

    def check(self, d):
        super().check(d)
        if d["status"] == "incomplete":
            IsCompleteReplyIncomplete().check(d)


class IsCompleteReplyIncomplete(Reference):
    indent = Unicode()


# IOPub messages


class ExecuteInput(Reference):
    code = Unicode()
    execution_count = Integer()


class Error(ExecuteReplyError):
    """Errors are the same as ExecuteReply, but without status"""

    status = None  # type:ignore  # no status field


class Stream(Reference):
    name = Enum(("stdout", "stderr"), default_value="stdout")
    text = Unicode()


class DisplayData(MimeBundle):
    pass


class ExecuteResult(MimeBundle):
    execution_count = Integer()


class HistoryReply(Reply):
    history = List(List())


# Subshell control messages


class CreateSubshellReply(Reply):
    subshell_id = Unicode()


class DeleteSubshellReply(Reply):
    pass


class ListSubshellReply(Reply):
    subshell_id = List(Unicode())


references = {
    "execute_reply": ExecuteReply(),
    "inspect_reply": InspectReply(),
    "status": Status(),
    "complete_reply": CompleteReply(),
    "kernel_info_reply": KernelInfoReply(),
    "connect_reply": ConnectReply(),
    "comm_info_reply": CommInfoReply(),
    "is_complete_reply": IsCompleteReply(),
    "execute_input": ExecuteInput(),
    "execute_result": ExecuteResult(),
    "history_reply": HistoryReply(),
    "error": Error(),
    "stream": Stream(),
    "display_data": DisplayData(),
    "header": RHeader(),
    "create_subshell_reply": CreateSubshellReply(),
    "delete_subshell_reply": DeleteSubshellReply(),
    "list_subshell_reply": ListSubshellReply(),
}

# -----------------------------------------------------------------------------
# Specifications of `content` part of the reply messages.
# -----------------------------------------------------------------------------


def validate_message(msg, msg_type=None, parent=None):
    """validate a message

    This is a generator, and must be iterated through to actually
    trigger each test.

    If msg_type and/or parent are given, the msg_type and/or parent msg_id
    are compared with the given values.
    """
    RMessage().check(msg)
    if msg_type and msg["msg_type"] != msg_type:
        msg_ = f"Expected {msg_type=} but got '{msg['msg_type']}'  for {msg=}"
        raise ValueError(msg_)
    if parent and msg["parent_header"]["msg_id"] != parent:
        raise RuntimeError(f"This parent 'msg_id' does not match {msg=} {parent=}")
    content = msg["content"]
    ref = references[msg["msg_type"]]
    try:
        ref.check(content)
    except Exception as e:
        e.add_note(f"\n{msg_type=}\n{parent=}\n{content=}")
        raise e


async def flush_channels(kc):
    """flush any messages waiting on the queue"""
    from tests.test_message_spec import validate_message

    while True:
        for get_msg in (kc.get_shell_msg, kc.get_iopub_msg):
            msg = None
            with anyio.move_on_after(0.01):
                msg = await get_msg()
                validate_message(msg)
            if not msg:
                return


async def check_pub_message(client: AsyncKernelClient, msg_id: str, *, msg_type="status", **content_checks):
    msg = await client.get_iopub_msg()
    validate_message(msg, msg_type, msg_id)
    content = msg["content"]
    for k, v in content_checks.items():
        assert content[k] == v
    return msg


async def get_shell_message(client: AsyncKernelClient, msg_id: str, msg_type: str):
    msg = await client.get_shell_msg()
    validate_message(msg, msg_type, msg_id)
    return msg["content"]


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

# Shell channel


async def test_execute(client, kernel):
    msg_id = client.execute(code="x=1")
    reply = await get_reply(client, msg_id)
    validate_message(reply, "execute_reply", msg_id)
    assert kernel.user_ns["x"] == 1


async def test_execute_silent(client):
    msg_id, reply = await execute(client, code="x=1", silent=True)
    count = reply["execution_count"]
    await check_pub_message(client, msg_id, execution_state="busy")
    await check_pub_message(client, msg_id, execution_state="idle")
    with pytest.raises(Empty):
        await client.get_iopub_msg(timeout=0.1)

    # Do a second execution
    msg_id, reply = await execute(client, code="x=2", silent=True)
    await check_pub_message(client, msg_id, execution_state="busy")
    await check_pub_message(client, msg_id, execution_state="idle")
    with pytest.raises(Empty):
        await client.get_iopub_msg(timeout=0.1)
    count_2 = reply["execution_count"]

    assert count_2 == count, "count should not increment when silent"


async def test_execute_error(client):
    msg_id, reply = await execute(client, code="1/0")
    assert reply["status"] == "error"
    assert reply["ename"] == "ZeroDivisionError"

    await check_pub_message(client, msg_id, execution_state="busy")
    await check_pub_message(client, msg_id, msg_type="execute_input")
    await check_pub_message(client, msg_id, msg_type="error")


async def test_execute_inc(client):
    """execute request should increment execution_count"""

    _, reply = await execute(client, code="x=1")
    count = reply["execution_count"]

    _, reply = await execute(client, code="x=2")
    count_2 = reply["execution_count"]
    assert count_2 == count + 1


async def test_execute_stop_on_error(client):
    """execute request should not abort execution queue with stop_on_error False"""

    bad_code = "\n".join([
        # sleep to ensure subsequent message is waiting in the queue to be aborted
        # async sleep to ensure coroutines are processing while this happens
        "import anyio",
        "await anyio.sleep(1)",
        "raise ValueError()",
    ])

    msg_id_bad_code = client.execute(bad_code)
    msg_id_1 = client.execute('print("Hello")')
    msg_id_2 = client.execute('print("world")')
    content = await get_shell_message(client, msg_id_bad_code, "execute_reply")
    assert content.get("status") == "error"
    assert content.get("traceback")

    content = await get_shell_message(client, msg_id_1, "execute_reply")
    assert content["status"] == "error"

    content = await get_shell_message(client, msg_id_2, "execute_reply")
    assert content["status"] == "error"

    #  Test stop_on_error=False
    msg_id_3 = client.execute(bad_code, stop_on_error=False)
    msg_id_4 = client.execute('print("Hello")')
    content = await get_shell_message(client, msg_id_3, "execute_reply")
    content = await get_shell_message(client, msg_id_4, "execute_reply")
    assert content["status"] == "ok"


async def test_non_execute_stop_on_error(client):
    """test that non-execute_request's are not aborted after an error"""

    execute_id = client.execute("raise ValueError")
    content = await get_shell_message(client, execute_id, "execute_reply")
    assert content.get("status") == "error"

    kernel_info_id = client.kernel_info()
    comm_info_id = client.comm_info()
    inspect_id = client.inspect(code="print")

    content = await get_shell_message(client, kernel_info_id, "kernel_info_reply")
    assert content.get("status") == "ok"
    content = await get_shell_message(client, comm_info_id, "comm_info_reply")
    assert content.get("status") == "ok"
    content = await get_shell_message(client, inspect_id, "inspect_reply")
    assert content.get("status") == "ok"


async def test_user_expressions(client):
    msg_id = client.execute(code="x=1", user_expressions=dict(foo="x+1"))
    reply = await get_reply(client, msg_id)  # execute
    user_expressions = reply["content"]["user_expressions"]
    assert user_expressions == {
        "foo": {
            "status": "ok",
            "data": {"text/plain": "2"},
            "metadata": {},
        }
    }


async def test_user_expressions_fail(client):
    msg_id, reply = await execute(client, code="x=0", user_expressions=dict(foo="nosuchname"))
    user_expressions = reply["user_expressions"]
    foo = user_expressions["foo"]
    assert foo["status"] == "error"
    assert foo["ename"] == "NameError"


async def test_oinfo(client):
    msg_id = client.inspect("a")
    reply = await get_reply(client, msg_id)
    validate_message(reply, "inspect_reply", msg_id)


async def test_oinfo_found(client):
    msg_id, reply = await execute(client, code="a=5")

    msg_id = client.inspect("a")
    reply = await get_reply(client, msg_id)
    validate_message(reply, "inspect_reply", msg_id)
    content = reply["content"]
    assert content["found"]
    text = content["data"]["text/plain"]
    assert "Type:" in text
    assert "Docstring:" in text


async def test_oinfo_detail(client):
    msg_id, reply = await execute(client, code="ip=get_ipython()")

    msg_id = client.inspect("ip.object_inspect", cursor_pos=10, detail_level=1)
    reply = await get_reply(client, msg_id)
    validate_message(reply, "inspect_reply", msg_id)
    content = reply["content"]
    assert content["found"]
    text = content["data"]["text/plain"]
    assert "Signature:" in text
    assert "Source:" in text


async def test_oinfo_not_found(client):
    msg_id = client.inspect("does_not_exist")
    reply = await get_reply(client, msg_id)
    validate_message(reply, "inspect_reply", msg_id)
    content = reply["content"]
    assert not content["found"]


async def test_complete(client):
    msg_id, reply = await execute(client, code="alpha = albert = 5")

    msg_id = client.complete("al", 2)
    reply = await get_reply(client, msg_id)
    validate_message(reply, "complete_reply", msg_id)
    matches = reply["content"]["matches"]
    for name in ("alpha", "albert"):
        assert name in matches


async def test_kernel_info_request(client):
    msg_id = client.kernel_info()
    reply = await get_reply(client, msg_id)
    validate_message(reply, "kernel_info_reply", msg_id)
    assert "supported_features" in reply["content"]
    assert "kernel subshells" in reply["content"]["supported_features"]


async def test_connect_request(client):
    msg = client.session.msg("connect_request")
    client.shell_channel.send(msg)
    msg_id = msg["header"]["msg_id"]
    reply = await get_reply(client, msg_id)
    validate_message(reply, "connect_reply", msg_id)


async def test_subshell(client):
    msg = client.session.msg("create_subshell_request")
    client.control_channel.send(msg)
    msg_id = msg["header"]["msg_id"]
    reply = await get_reply(client, msg_id, channel="control")
    validate_message(reply, "create_subshell_reply", msg_id)
    subshell_id = reply["content"]["subshell_id"]

    msg = client.session.msg("list_subshell_request")
    client.control_channel.send(msg)
    msg_id = msg["header"]["msg_id"]
    reply = await get_reply(client, msg_id, channel="control")
    validate_message(reply, "list_subshell_reply", msg_id)

    msg = client.session.msg("delete_subshell_request", {"subshell_id": subshell_id})
    client.control_channel.send(msg)
    msg_id = msg["header"]["msg_id"]
    reply = await get_reply(client, msg_id, channel="control")
    validate_message(reply, "delete_subshell_reply", msg_id)


@pytest.mark.skipif(
    version_info < (5, 0),
    reason="earlier Jupyter Client don't have comm_info",
)
async def test_comm_info_request(client):
    msg_id = client.comm_info()
    reply = await get_reply(client, msg_id)
    validate_message(reply, "comm_info_reply", msg_id)


async def test_single_payload(client):
    """
    We want to test the set_next_input is not triggered several time per cell.
    This is (was ?) mostly due to the fact that `?` in a loop would trigger
    several set_next_input.

    I'm tempted to thing that we actually want to _allow_ multiple
    set_next_input (that's users' choice). But that `?` itself (and ?'s
    transform) should avoid setting multiple set_next_input).
    """

    msg_id, reply = await execute(
        client, code="ip = get_ipython()\nfor i in range(3):\n   ip.set_next_input('Hello There')\n"
    )
    payload = reply["payload"]
    next_input_pls = [pl for pl in payload if pl["source"] == "set_next_input"]
    assert len(next_input_pls) == 1


async def test_is_complete(client):
    msg_id = client.is_complete("a = 1")
    reply = await get_reply(client, msg_id)
    validate_message(reply, "is_complete_reply", msg_id)


async def test_history_range(client):
    await execute(client, code="x=1", store_history=True)
    msg_id = client.history(hist_access_type="range", raw=True, output=True, start=1, stop=2, session=0)
    reply = await get_reply(client, msg_id)
    validate_message(reply, "history_reply", msg_id)
    content = reply["content"]
    assert len(content["history"]) == 1


async def test_history_tail(client):
    await execute(client, code="x=1", store_history=True)

    msg_id = client.history(hist_access_type="tail", raw=True, output=True, n=1, session=0)
    reply = await get_reply(client, msg_id)
    validate_message(reply, "history_reply", msg_id)
    content = reply["content"]
    assert len(content["history"]) == 1


async def test_history_search(client):
    await execute(client, code="x=1", store_history=True)

    msg_id = client.history(hist_access_type="search", raw=True, output=True, n=1, pattern="*", session=0)
    reply = await get_reply(client, msg_id)
    validate_message(reply, "history_reply", msg_id)
    content = reply["content"]
    assert len(content["history"]) == 1


# IOPub channel


async def test_stream(client):
    client.execute("print('hi')")
    stdout, stderr = await utils.assemble_output(client)
    assert stdout == "hi\n"

async def test_display_data(client):
    msg_id, reply = await execute(client, "from IPython.display import display; display(1)")
    await check_pub_message(client, msg_id, execution_state="busy")
    await check_pub_message(client, msg_id, msg_type="execute_input")
    msg = await check_pub_message(client, msg_id, msg_type="display_data", execution_state="idle")

    validate_message(msg, "display_data", parent=msg_id)
    data = msg["content"]["data"]
    assert data["text/plain"] == "1"
