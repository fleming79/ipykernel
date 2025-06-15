"""test the IPython Kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import ast
import subprocess
import sys

import anyio
import pytest

from tests import utils


def _check_status(content):
    """If status=error, show the traceback"""
    if content["status"] == "error":
        raise AssertionError("".join(["\n"] + content["traceback"]))


# printing tests


async def test_simple_print(client):
    """simple print statement in kernel"""
    await utils.clear_pub_message(client)
    client.execute("print('hi')")
    stdout, stderr = await utils.assemble_output(client)
    assert stdout == "hi\n"
    assert stderr == ""


async def test_sys_path(client):
    """test that sys.path doesn't get messed up by default"""
    await utils.clear_pub_message(client)
    client.execute("import sys; print(repr(sys.path))")
    stdout, stderr = await utils.assemble_output(client)
    # for error-output on failure
    sys.stderr.write(stderr)

    sys_path = ast.literal_eval(stdout.strip())
    assert "" in sys_path


async def test_sys_path_profile_dir(client):
    """test that sys.path doesn't get messed up when `--profile-dir` is specified"""
    client.execute("import sys; print(repr(sys.path))")
    stdout, stderr = await utils.assemble_output(client)
    # for error-output on failure
    sys.stderr.write(stderr)

    sys_path = ast.literal_eval(stdout.strip())
    assert "" in sys_path


# raw_input tests


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


async def test_help_output(client, kernel):
    """ipython kernel --help-all works"""
    cmd = [sys.executable, "-m", "IPython", "kernel", "--help-all"]
    proc = subprocess.run(cmd, timeout=30, capture_output=True, check=True)
    assert proc.returncode == 0, proc.stderr
    assert b"Traceback" not in proc.stderr
    assert b"Options" in proc.stdout
    assert b"Class" in proc.stdout


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


@pytest.mark.skipif(sys.platform != "win32", reason="only run on Windows")
async def test_complete(client, kernel):
    client.execute("a = 1")
    await utils.wait_for_idle(client)
    cell = "import IPython\nb = a."
    client.complete(cell)
    reply = await client.get_shell_msg()

    c = reply["content"]
    assert c["status"] == "ok"
    start = cell.find("a.")
    end = start + 2
    assert c["cursor_end"] == cell.find("a.") + 2
    assert c["cursor_start"] <= end

    # there are many right answers for cursor_start,
    # so verify application of the completion
    # rather than the value of cursor_start

    matches = c["matches"]
    assert matches
    for m in matches:
        completed = cell[: c["cursor_start"]] + m
        assert completed.startswith(cell)


async def test_matplotlib_inline_on_import(client, kernel):
    pytest.importorskip("matplotlib", reason="this test requires matplotlib")

    cell = "\n".join(["import matplotlib, matplotlib.pyplot as plt", "backend = matplotlib.get_backend()"])
    _, reply = client.execute(cell, user_expressions={"backend": "backend"})
    _check_status(reply)
    backend_bundle = reply["user_expressions"]["backend"]
    _check_status(backend_bundle)
    assert "backend_inline" in backend_bundle["data"]["text/plain"]


async def test_message_order(client, kernel):
    N = 100  # number of messages to test

    _, reply = await utils.execute(client, "a = 1")
    _check_status(reply)
    offset = reply["execution_count"] + 1
    cell = "a += 1\na"
    msg_ids = []
    # submit N executions as fast as we can
    for _ in range(N):
        msg_ids.append(client.execute(cell))
    # check message-handling order
    for i, msg_id in enumerate(msg_ids, offset):
        reply = await client.get_shell_msg()
        _check_status(reply["content"])
        assert reply["content"]["execution_count"] == i
        assert reply["parent_header"]["msg_id"] == msg_id


@pytest.mark.skipif(
    sys.platform.startswith("linux") or sys.platform.startswith("darwin"),
    reason="test only on windows",
)
async def test_unc_paths(client, kernel, tmp_path):
    drive_file_path = tmp_path.joinpath("unc.txt")
    with drive_file_path.open("w+") as f:
        f.write("# UNC test")
    unc_root = "\\\\localhost\\C$"
    unc_file_path = f"{unc_root}\\{drive_file_path.name}"

    client.execute(f"cd {unc_file_path:s}")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "ok"
    out, err = await utils.assemble_output(client)
    assert unc_file_path in out

    client.execute("ls")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "ok"
    out, err = await utils.assemble_output(client)
    assert "unc.txt" in out

    client.execute("cd")
    reply = await client.get_shell_msg()
    assert reply["content"]["status"] == "ok"



async def test_shutdown(client, app):
    """Kernel exits after polite shutdown_request"""
    # TODO: write me