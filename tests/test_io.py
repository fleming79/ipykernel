"""Test IO capturing functionality"""

import io
import os
import subprocess
import sys
import threading
import time
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from unittest import mock

import pytest
import zmq
import zmq_anyio
from anyio import create_task_group
from jupyter_client.session import Session

from ipykernel.iostream import _PARENT, BackgroundSocket, IOPubThread, OutStream

pytestmark = pytest.mark.anyio


@pytest.fixture()
def ctx():
    ctx = zmq.Context()
    yield ctx
    ctx.destroy()


@pytest.fixture()
async def iopub_thread(ctx):
    try:
        async with create_task_group() as tg:
            pub = zmq_anyio.Socket(ctx.socket(zmq.PUB))
            await tg.start(pub.start)
            thread = IOPubThread(pub)
            thread.start()

            try:
                yield thread
            finally:
                await pub.stop()
                thread.stop()
                thread.close()
    except BaseException:
        pass


async def test_io_api(iopub_thread):
    """Test that wrapped stdout has the same API as a normal TextIO object"""
    session = Session()
    stream = OutStream(session, iopub_thread, "stdout")

    assert stream.errors is None
    assert not stream.isatty()
    with pytest.raises(io.UnsupportedOperation):
        stream.detach()
    with pytest.raises(io.UnsupportedOperation):
        next(stream)
    with pytest.raises(io.UnsupportedOperation):
        stream.read()
    with pytest.raises(io.UnsupportedOperation):
        stream.readline()
    with pytest.raises(io.UnsupportedOperation):
        stream.seek(0)
    with pytest.raises(io.UnsupportedOperation):
        stream.tell()
    with pytest.raises(TypeError):
        stream.write(b"")  # type: ignore[arg-type]


async def test_io_isatty(iopub_thread):
    session = Session()
    stream = OutStream(session, iopub_thread, "stdout", isatty=True)
    assert stream.isatty()


async def test_io_thread(iopub_thread):
    thread = iopub_thread
    thread._setup_pipe_in()
    msg = [thread._pipe_uuid, b"a"]
    await thread._handle_pipe_msg(msg)
    ctx1, pipe = thread._setup_pipe_out()
    pipe.close()
    thread._pipe_in1.close()
    thread._check_mp_mode = lambda: _PARENT
    thread._really_send([b"hi"])
    ctx1.destroy()
    thread.stop()


async def test_background_socket(iopub_thread):
    sock = BackgroundSocket(iopub_thread)
    assert sock.__class__ == BackgroundSocket
    assert sock.io_thread == iopub_thread
    sock.send(b"hi")


async def test_outstream(iopub_thread):
    session = Session()
    pub = iopub_thread.socket

    stream = OutStream(session, pub, "stdout")
    stream.close()

    stream = OutStream(session, iopub_thread, "stdout")
    stream.close()

    stream = OutStream(session, iopub_thread, "stdout", isatty=True, echo=io.StringIO())

    with stream:
        with pytest.raises(io.UnsupportedOperation):
            stream.fileno()
        stream.flush()
        stream.write("hi")
        stream.writelines(["ab", "cd"])
        assert stream.writable()


@pytest.mark.skip(reason="Cannot use a zmq-anyio socket on different threads")
async def test_event_pipe_gc(iopub_thread):
    session = Session(key=b"abc")
    stream = OutStream(
        session,
        iopub_thread,
        "stdout",
        isatty=True,
    )
    assert iopub_thread._event_pipes == {}
    with stream, mock.patch.object(sys, "stdout", stream), ThreadPoolExecutor(1) as pool:
        pool.submit(print, "x").result()
        pool_thread = pool.submit(threading.current_thread).result()
        threads = list(iopub_thread._event_pipes)
        assert threads[0] == pool_thread

    # run gc once in the iopub thread
    f: Future = Future()

    try:
        iopub_thread._event_pipe_gc()
    except Exception as e:
        f.set_exception(e)
    else:
        f.set_result(None)

    # wait for call to finish in iopub thread
    f.result()
    # assert iopub_thread._event_pipes == {}


async def subprocess_test_echo_watch():
    # handshake Pub subscription
    session = Session(key=b"abc")

    # use PUSH socket to avoid subscription issues
    with zmq.Context() as ctx:
        pub = zmq_anyio.Socket(ctx.socket(zmq.PUSH))
        pub.connect(os.environ["IOPUB_URL"])
        iopub_thread = IOPubThread(pub)
        iopub_thread.start()
        stdout_fd = sys.stdout.fileno()
        sys.stdout.flush()
        stream = OutStream(session, iopub_thread, "stdout", isatty=True, echo=sys.stdout)
        save_stdout = sys.stdout
        with stream, mock.patch.object(sys, "stdout", stream):
            # write to low-level FD
            os.write(stdout_fd, b"fd\n")
            # print (writes to stream)
            print("print\n", end="")
            sys.stdout.flush()
            # write to unwrapped __stdout__ (should also go to original FD)
            sys.__stdout__.write("__stdout__\n")
            sys.__stdout__.flush()
            # write to original sys.stdout (should be the same as __stdout__)
            save_stdout.write("stdout\n")
            save_stdout.flush()
            # is there another way to flush on the FD?
            fd_file = os.fdopen(stdout_fd, "w")
            fd_file.flush()
            # we don't have a sync flush on _reading_ from the watched pipe
            time.sleep(1)
            stream.flush()
        iopub_thread.stop()
        iopub_thread.close()

