"""test the IPython Kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import anyio
import pytest

from tests import utils


async def test_simple_print(client):
    """simple print statement in kernel"""
    await utils.clear_pub_message(client)
    client.execute("print('hi')")
    stdout, stderr = await utils.assemble_output(client)
    assert stdout == "hi\n"
    assert stderr == ""



async def test_raw_input(client):
    """test input"""

    pytest.skip("Blocks forever")

    input_f = "input"
    theprompt = "prompt> "
    code = f'print({input_f}("{theprompt}"))'
    client.execute(code, allow_stdin=True)
    await anyio.sleep(0.1)
    msg = await client.get_stdin_msg()
    assert msg["header"]["msg_type"] == "input_request"
    content = msg["content"]
    assert content["prompt"] == theprompt
    text = "some text"
    client.input(text)
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "ok"
    stdout, stderr = await utils.assemble_output(client)
    assert stdout == text + "\n"


async def test_save_history(client, tmp_path):
    file = tmp_path.joinpath("hist.out")
    client.execute("a=1")
    await utils.wait_for_idle(client)
    client.execute('b="abcþ"')
    await utils.wait_for_idle(client)
    _, reply = await utils.execute(client, f"%hist -f {file}")
    assert reply["status"] == "ok"
    with open(file, encoding="utf-8") as f:
        content = f.read()
    assert "a=1" in content
    assert 'b="abcþ"' in content


async def test_is_complete(client):
    # There are more test cases for this in core - here we just check
    # that the kernel exposes the interface correctly.
    client.is_complete("2+2")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "complete"

    # SyntaxError
    client.is_complete("raise = 2")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "invalid"

    client.is_complete("a = [1,\n2,")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "incomplete"
    assert reply["content"]["indent"] == ""

    # Cell magic ends on two blank lines for console UIs
    client.is_complete("%%timeit\na\n\n")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "complete"


async def test_message_order(client, kernel):
    N = 100  # number of messages to test

    _, reply = await utils.execute(client, "a = 1")
    offset = reply["execution_count"] + 1
    cell = "a += 1\na"
    msg_ids = []
    # submit N executions as fast as we can
    for _ in range(N):
        msg_ids.append(client.execute(cell))
    # check message-handling order
    for i, msg_id in enumerate(msg_ids, offset):
        reply = await client.get_shell_msg()
        assert reply["content"]["execution_count"] == i
        assert reply["parent_header"]["msg_id"] == msg_id


async def test_shutdown(client, app):
    """Kernel exits after polite shutdown_request"""
    # TODO: write me