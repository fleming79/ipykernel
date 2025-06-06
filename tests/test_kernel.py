"""test the IPython Kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import ast
import os.path
import platform
import signal
import subprocess
import sys
import time
from subprocess import Popen
from tempfile import TemporaryDirectory

import IPython
import psutil
import pytest
from flaky import flaky

from tests.utils import TIMEOUT, assemble_output, execute, get_reply, wait_for_idle


async def _check_main(kc, kernel, expected=True, stream="stdout"):
    await execute(kc, kernel, code="import sys")

    msg_id, content = await execute(kc, kernel, code="print(sys.%s._is_main_process())" % stream)
    stdout, stderr = await assemble_output(kc, kernel)
    assert stdout.strip() == repr(expected)


def _check_status(content):
    """If status=error, show the traceback"""
    if content["status"] == "error":
        raise AssertionError("".join(["\n"] + content["traceback"]))


# printing tests


async def test_simple_print(client, kernel):
    """simple print statement in kernel"""
    msg_id, content = await execute(client, kernel, code="print('hi')")
    stdout, stderr = await assemble_output(client, kernel)
    assert stdout == "hi\n"
    assert stderr == ""
    await _check_main(client, kernel, expected=True)


async def test_print_to_correct_cell_from_thread(client, kernel):
    """should print to the cell that spawned the thread, not a subsequently run cell"""
    iterations = 5
    interval = 0.25
    code = f"""\
    from threading import Thread
    from time import sleep

    def thread_target():
        for i in range({iterations}):
            print(i, end='', flush=True)
            sleep({interval})

    Thread(target=thread_target).start()
    """
    ka, client = client, kernel
    thread_msg_id = client.execute(client, kernel, code)
    _ = client.execute(client, kernel, "pass")

    received = 0
    while received < iterations:
        msg = client.get_iopub_msg(timeout=interval * 2)
        if msg["msg_type"] != "stream":
            continue
        content = msg["content"]
        assert content["name"] == "stdout"
        assert content["text"] == str(received)
        # this is crucial as the parent header decides to which cell the output goes
        assert msg["parent_header"]["msg_id"] == thread_msg_id
        received += 1


async def test_print_to_correct_cell_from_child_thread(client, kernel):
    """should print to the cell that spawned the thread, not a subsequently run cell"""
    iterations = 5
    interval = 0.25
    code = f"""\
    from threading import Thread
    from time import sleep

    def child_target():
        for i in range({iterations}):
            print(i, end='', flush=True)
            sleep({interval})

    def parent_target():
        sleep({interval})
        thread = Thread(target=child_target)
        thread.start()
        thread.join()

    Thread(target=parent_target).start()
    """
    ka, client = client, kernel
    thread_msg_id = client.execute(client, kernel, code)
    _ = client.execute(client, kernel, "pass")

    received = 0
    while received < iterations:
        msg = client.get_iopub_msg(timeout=interval * 2)
        if msg["msg_type"] != "stream":
            continue
        content = msg["content"]
        assert content["name"] == "stdout"
        assert content["text"] == str(received)
        # this is crucial as the parent header decides to which cell the output goes
        assert msg["parent_header"]["msg_id"] == thread_msg_id
        received += 1


async def test_print_to_correct_cell_from_asyncio(client, kernel):
    """should print to the cell that scheduled the task, not a subsequently run cell"""
    iterations = 5
    interval = 0.25
    code = f"""\
    import asyncio

    async def async_task():
        for i in range({iterations}):
            print(i, end='', flush=True)
            await asyncio.sleep({interval})

    loop = asyncio.get_event_loop()
    loop.create_task(async_task());
    """
    ka, client = client, kernel
    thread_msg_id = client.execute(client, kernel, code)
    _ = client.execute(client, kernel, "pass")

    received = 0
    while received < iterations:
        msg = client.get_iopub_msg(timeout=interval * 2)
        if msg["msg_type"] != "stream":
            continue
        content = msg["content"]
        assert content["name"] == "stdout"
        assert content["text"] == str(received)
        # this is crucial as the parent header decides to which cell the output goes
        assert msg["parent_header"]["msg_id"] == thread_msg_id
        received += 1


@pytest.mark.skip(reason="Currently don't capture during test as pytest does its own capturing")
async def test_capture_fd(client, kernel):
    """simple print statement in kernel"""
    msg_id, content = await execute(client, kernel, code="import os; os.system('echo capsys')")
    stdout, stderr = await assemble_output(client, kernel)
    assert stdout == "capsys\n"
    assert stderr == ""
    await _check_main(client, kernel, expected=True)


@pytest.mark.skip(reason="Currently don't capture during test as pytest does its own capturing")
async def test_subprocess_peek_at_stream_fileno(client, kernel):
    msg_id, content = await execute(
        client,
        code="import subprocess, sys; subprocess.run(['python', '-c', 'import os; os.system(\"echo CAP1\"); print(\"CAP2\")'], stderr=sys.stderr)",
    )
    stdout, stderr = await assemble_output(client, kernel)
    assert stdout == "CAP1\nCAP2\n"
    assert stderr == ""
    await _check_main(client, kernel, expected=True)


async def test_sys_path(client, kernel):
    """test that sys.path doesn't get messed up by default"""
    msg_id, content = await execute(client, kernel, code="import sys; print(repr(sys.path))")
    stdout, stderr = await assemble_output(client, kernel)
    # for error-output on failure
    sys.stderr.write(stderr)

    sys_path = ast.literal_eval(stdout.strip())
    assert "" in sys_path


async def test_sys_path_profile_dir(client, kernel):
    """test that sys.path doesn't get messed up when `--profile-dir` is specified"""
    msg_id, content = await execute(client, kernel, code="import sys; print(repr(sys.path))")
    stdout, stderr = await assemble_output(client, kernel)
    # for error-output on failure
    sys.stderr.write(stderr)

    sys_path = ast.literal_eval(stdout.strip())
    assert "" in sys_path


@flaky(max_runs=3)
@pytest.mark.skipif(
    sys.platform == "win32" or (sys.platform == "darwin"),
    reason="subprocess prints fail on Windows and MacOS Python 3.8+",
)
async def test_subprocess_print(client, kernel):
    """printing from forked mp.Process"""
    await _check_main(client, kernel, expected=True)

    np = 5
    code = "\n".join(
        [
            "import time",
            "import multiprocessing as mp",
            "pool = [mp.Process(target=print, args=('hello', i,)) for i in range(%i)]" % np,
            "for p in pool: p.start()",
            "for p in pool: p.join()",
            "time.sleep(0.5),",
        ]
    )

    msg_id, content = await execute(client, kernel, code=code)
    stdout, stderr = await assemble_output(client, kernel)
    assert stdout.count("hello") == np, stdout
    for n in range(np):
        assert stdout.count(str(n)) == 1, stdout
    assert stderr == ""
    await _check_main(client, kernel, expected=True)
    await _check_main(client, kernel, expected=True, stream="stderr")


@flaky(max_runs=3)
async def test_subprocess_noprint(client, kernel):
    """mp.Process without print doesn't trigger iostream mp_mode"""
    ka, client = client, kernel
    np = 5
    code = "\n".join(
        [
            "import multiprocessing as mp",
            "pool = [mp.Process(target=range, args=(i,)) for i in range(%i)]" % np,
            "for p in pool: p.start()",
            "for p in pool: p.join()",
        ]
    )

    msg_id, content = await execute(client, kernel, code=code)
    stdout, stderr = await assemble_output(client, kernel)
    assert stdout == ""
    assert stderr == ""

    await _check_main(client, kernel, expected=True)
    await _check_main(client, kernel, expected=True, stream="stderr")


@flaky(max_runs=3)
@pytest.mark.skipif(
    (sys.platform == "win32") or (sys.platform == "darwin"),
    reason="subprocess prints fail on Windows and MacOS Python 3.8+",
)
async def test_subprocess_error(client, kernel):
    """error in mp.Process doesn't crash"""
    ka, client = client, kernel
    code = "\n".join(
        [
            "import multiprocessing as mp",
            "p = mp.Process(target=int, args=('hi',))",
            "p.start()",
            "p.join()",
        ]
    )

    msg_id, content = await execute(client, kernel, code=code)
    stdout, stderr = await assemble_output(client, kernel)
    assert stdout == ""
    assert "ValueError" in stderr

    await _check_main(client, kernel, expected=True)
    await _check_main(client, kernel, expected=True, stream="stderr")


# raw_input tests


async def test_raw_input(client, kernel):
    """test input"""
    ka, client = client, kernel
    input_f = "input"
    theprompt = "prompt> "
    code = f'print({input_f}("{theprompt}"))'
    client.execute(client, kernel, code, allow_stdin=True)
    msg = client.get_stdin_msg(timeout=TIMEOUT)
    assert msg["header"]["msg_type"] == "input_request"
    content = msg["content"]
    assert content["prompt"] == theprompt
    text = "some text"
    client.input(text)
    reply = await client.get_shell_msg(timeout=TIMEOUT)
    assert reply["content"]["status"] == "ok"
    stdout, stderr = await assemble_output(client, kernel)
    assert stdout == text + "\n"


async def test_save_history(client, kernel, tmp_path):
    # Saving history from the kernel with %hist -f was failing because of
    # unicode problems on Python 2.
    ka, client = client, kernel
    file = tmp_path.joinpath("hist.out")
    await execute(client, kernel, "a=1")
    await wait_for_idle(client)
    await execute(client, kernel, 'b="abcþ"')
    await wait_for_idle(client)
    _, reply = await execute(client, kernel, f"%hist -f {file}")
    assert reply["status"] == "ok"
    with open(file, encoding="utf-8") as f:
        content = f.read()
    assert "a=1" in content
    assert 'b="abcþ"' in content


async def test_smoke_faulthandler(client, kernel):
    pytest.importorskip("faulthandler", reason="this test needs faulthandler")
    ka, client = client, kernel
    # Note: faulthandler.register is not available on windows.
    code = "\n".join(
        [
            "import sys",
            "import faulthandler",
            "import signal",
            "faulthandler.enable()",
            'if not sys.platform.startswith("win32"):',
            "    faulthandler.register(signal.SIGTERM)",
        ]
    )
    _, reply = await execute(client, kernel, code)
    assert reply["status"] == "ok", reply.get("traceback", "")


async def test_help_output(client, kernel):
    """ipython kernel --help-all works"""
    cmd = [sys.executable, "-m", "IPython", "kernel", "--help-all"]
    proc = subprocess.run(cmd, timeout=30, capture_output=True, check=True)
    assert proc.returncode == 0, proc.stderr
    assert b"Traceback" not in proc.stderr
    assert b"Options" in proc.stdout
    assert b"Class" in proc.stdout


async def test_is_complete(client, kernel):
    ka, client = client, kernel
    # There are more test cases for this in core - here we just check
    # that the kernel exposes the interface correctly.
    client.is_complete("2+2")
    reply = await client.get_shell_msg(timeout=TIMEOUT)
    assert reply["content"]["status"] == "complete"

    # SyntaxError
    client.is_complete("raise = 2")
    reply = await client.get_shell_msg(timeout=TIMEOUT)
    assert reply["content"]["status"] == "invalid"

    client.is_complete("a = [1,\n2,")
    reply = await client.get_shell_msg(timeout=TIMEOUT)
    assert reply["content"]["status"] == "incomplete"
    assert reply["content"]["indent"] == ""

    # Cell magic ends on two blank lines for console UIs
    client.is_complete("%%timeit\na\n\n")
    reply = await client.get_shell_msg(timeout=TIMEOUT)
    assert reply["content"]["status"] == "complete"


@pytest.mark.skipif(sys.platform != "win32", reason="only run on Windows")
async def test_complete(client, kernel):
    ka, client = client, kernel
    await execute(client, kernel, "a = 1")
    await wait_for_idle(client)
    cell = "import IPython\nb = a."
    client.complete(cell)
    reply = await client.get_shell_msg(timeout=TIMEOUT)

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
    ka, client = client, kernel
    cell = "\n".join(["import matplotlib, matplotlib.pyplot as plt", "backend = matplotlib.get_backend()"])
    _, reply = await execute(client, kernel, cell, user_expressions={"backend": "backend"})
    _check_status(reply)
    backend_bundle = reply["user_expressions"]["backend"]
    _check_status(backend_bundle)
    assert "backend_inline" in backend_bundle["data"]["text/plain"]


async def test_message_order(client, kernel):
    N = 100  # number of messages to test
    ka, client = client, kernel
    _, reply = await execute(client, kernel, "a = 1", client)
    _check_status(reply)
    offset = reply["execution_count"] + 1
    cell = "a += 1\na"
    msg_ids = []
    # submit N executions as fast as we can
    for _ in range(N):
        msg_ids.append(client.execute(cell))
    # check message-handling order
    for i, msg_id in enumerate(msg_ids, offset):
        reply = await client.get_shell_msg(timeout=TIMEOUT)
        _check_status(reply["content"])
        assert reply["content"]["execution_count"] == i
        assert reply["parent_header"]["msg_id"] == msg_id


@pytest.mark.skipif(
    sys.platform.startswith("linux") or sys.platform.startswith("darwin"),
    reason="test only on windows",
)
async def test_unc_paths(client, kernel):
    with client, kernel() as client, TemporaryDirectory() as td:
        drive_file_path = os.path.join(td, "unc.txt")
        with open(drive_file_path, "w+") as f:
            f.write("# UNC test")
        unc_root = "\\\\localhost\\C$"
        file_path = os.path.splitdrive(os.path.dirname(drive_file_path))[1]
        unc_file_path = os.path.join(unc_root, file_path[1:])

        client.execute(f"cd {unc_file_path:s}")
        reply = await client.get_shell_msg(timeout=TIMEOUT)
        assert reply["content"]["status"] == "ok"
        out, err = await assemble_output(client, kernel)
        assert unc_file_path in out

        client.execute(client, kernel, code="ls")
        reply = await client.get_shell_msg(timeout=TIMEOUT)
        assert reply["content"]["status"] == "ok"
        out, err = await assemble_output(client, kernel)
        assert "unc.txt" in out

        client.execute(client, kernel, code="cd")
        reply = await client.get_shell_msg(timeout=TIMEOUT)
        assert reply["content"]["status"] == "ok"


@pytest.mark.skipif(
    platform.python_implementation() == "PyPy",
    reason="does not work on PyPy",
)
async def test_shutdown(client, kernel):
    """Kernel exits after polite shutdown_request"""
    ka, client = client, kernel
    km = client.parent
    await execute(client, kernel, "a = 1")
    await wait_for_idle(client)
    client.shutdown()
    for _ in range(300):  # 30s timeout
        if km.is_alive():
            time.sleep(0.1)
        else:
            break
    assert not km.is_alive()


async def test_interrupt_during_input(client, kernel):
    """
    The kernel exits after being interrupted while waiting in input().

    input() appears to have issues other functions don't, and it needs to be
    interruptible in order for pdb to be interruptible.
    """
    with new_kernel() as client:
        km = client.parent
        msg_id = client.execute(client, kernel, "input()")
        time.sleep(1)  # Make sure it's actually waiting for input.
        km.interrupt_kernel()
        from ipykernel.test_message_spec import validate_message

        # If we failed to interrupt interrupt, this will timeout:
        reply = get_reply(client, msg_id, TIMEOUT)
        validate_message(reply, "execute_reply", msg_id)


@pytest.mark.skipif(os.name == "nt", reason="Message based interrupt not supported on Windows")
async def test_interrupt_with_message(client, kernel):
    with new_kernel() as client:
        km = client.parent
        km.kernel_spec.interrupt_mode = "message"
        msg_id = client.execute(client, kernel, "input()")
        time.sleep(1)  # Make sure it's actually waiting for input.
        km.interrupt_kernel()
        from ipykernel.test_message_spec import validate_message

        # If we failed to interrupt interrupt, this will timeout:
        reply = get_reply(client, msg_id, TIMEOUT)
        validate_message(reply, "execute_reply", msg_id)


@pytest.mark.skipif(
    "__pypy__" in sys.builtin_module_names,
    reason="fails on pypy",
)
async def test_interrupt_during_pdb_set_trace(client, kernel):
    """
    The kernel exits after being interrupted while waiting in pdb.set_trace().

    Merely testing input() isn't enough, pdb has its own issues that need
    to be handled in addition.

    This test will fail with versions of IPython < 7.14.0.
    """
    with new_kernel() as client:
        km = client.parent
        msg_id = client.execute(client, kernel, "import pdb; pdb.set_trace()")
        msg_id2 = client.execute(client, kernel, "3 + 4")
        time.sleep(1)  # Make sure it's actually waiting for input.
        km.interrupt_kernel()
        from ipykernel.test_message_spec import validate_message

        # If we failed to interrupt interrupt, this will timeout:
        reply = get_reply(client, msg_id, TIMEOUT)
        validate_message(reply, "execute_reply", msg_id)
        # If we failed to interrupt interrupt, this will timeout:
        reply = get_reply(client, msg_id2, TIMEOUT)
        validate_message(reply, "execute_reply", msg_id2)


async def test_control_thread_priority(client, kernel):
    km, client = client, kernel
    N = 5
    msg_id = client.execute(client, kernel, "pass")
    await get_reply(client, msg_id)

    sleep_msg_id = client.execute(client, kernel, "import asyncio; await asyncio.sleep(2)")

    # submit N shell messages
    shell_msg_ids = []
    for i in range(N):
        shell_msg_ids.append(client.execute(f"i = {i}"))

    # ensure all shell messages have arrived at the kernel before any control messages
    time.sleep(0.5)
    # at this point, shell messages should be waiting in msg_queue,
    # rather than zmq while the kernel is still in the middle of processing
    # the first execution

    # now send N control messages
    control_msg_ids = []
    for _ in range(N):
        msg = client.session.msg("kernel_info_request")
        client.control_channel.send(msg)
        control_msg_ids.append(msg["header"]["msg_id"])

    # finally, collect the replies on both channels for comparison
    get_reply(client, sleep_msg_id)
    shell_replies = []
    for msg_id in shell_msg_ids:
        shell_replies.append(get_reply(client, msg_id))

    control_replies = []
    for msg_id in control_msg_ids:
        control_replies.append(get_reply(client, msg_id, channel="control"))

    # verify that all control messages were handled before all shell messages
    shell_dates = [msg["header"]["date"] for msg in shell_replies]
    control_dates = [msg["header"]["date"] for msg in control_replies]
    # comparing first to last ought to be enough, since queues preserve order
    # use <= in case of very-fast handling and/or low resolution timers
    assert control_dates[-1] <= shell_dates[0]


async def test_sequential_control_messages(client, kernel):
    _, client = client, kernel
    msg_id = client, kernel.execute(client, kernel, "import time")
    get_reply(client, msg_id)

    # Send multiple messages on the control channel.
    # Using execute messages to vary duration.
    sleeps = [0.0, 0.6, 0.3, 0.1, 0.0]

    # Prepare messages
    msgs = [
        client.session.msg(
            "execute_request",
            {"code": f"time.sleep({sleep})", "user_expressions": {"i": str(i)}},
        )
        for i, sleep in enumerate(sleeps)
    ]
    msg_ids = [msg["header"]["msg_id"] for msg in msgs]

    # Submit messages
    for msg in msgs:
        client.control_channel.send(msg)

    # Get replies
    for ii, reply in enumerate(get_reply(client, msg_id, channel="control") for msg_id in msg_ids):
        i = reply["content"]["user_expressions"]["i"]["data"]["text/plain"]
        assert str(ii) == i


def _child():
    print("in child", os.getpid())

    def _print_and_exit(sig, frame):
        print(f"Received signal {sig}")
        # take some time so retries are triggered
        time.sleep(0.5)
        sys.exit(-sig)

    signal.signal(signal.SIGTERM, _print_and_exit)
    time.sleep(30)


def _start_children():
    ip = IPython.get_ipython()
    ns = ip.user_ns

    cmd = [sys.executable, "-c", f"from {__name__} import _child; _child()"]
    child_pg = Popen(cmd, start_new_session=False)
    child_newpg = Popen(cmd, start_new_session=True)
    ns["pid"] = os.getpid()
    ns["child_pg"] = child_pg.pid
    ns["child_newpg"] = child_newpg.pid
    # give them time to start up and register signal handlers
    time.sleep(1)


@pytest.mark.skipif(
    platform.python_implementation() == "PyPy",
    reason="does not work on PyPy",
)
@pytest.mark.skipif(
    sys.platform.lower() == "linux",
    reason="Stalls on linux",
)
async def test_shutdown_subprocesses(client, kernel):
    """Kernel exits after polite shutdown_request"""
    with new_kernel() as client:
        km = client.parent
        msg_id, reply = await execute(
            f"from {__name__} import _start_children\n_start_children()",
            client,
            user_expressions={
                "pid": "pid",
                "child_pg": "child_pg",
                "child_newpg": "child_newpg",
            },
        )
        print(reply)
        expressions = reply["user_expressions"]
        kernel_process = psutil.Process(int(expressions["pid"]["data"]["text/plain"]))
        child_pg = psutil.Process(int(expressions["child_pg"]["data"]["text/plain"]))
        child_newpg = psutil.Process(int(expressions["child_newpg"]["data"]["text/plain"]))
        wait_for_idle(client)

        client.shutdown()
        for _ in range(300):  # 30s timeout
            if km.is_alive():
                time.sleep(0.1)
            else:
                break
        assert not km.is_alive()
        assert not kernel_process.is_running()
        # child in the process group shut down
        assert not child_pg.is_running()
        # child outside the process group was not shut down (unix only)
        if os.name != "nt":
            assert child_newpg.is_running()
        try:
            child_newpg.terminate()
        except psutil.NoSuchProcess:
            pass
