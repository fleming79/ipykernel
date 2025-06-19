# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import asyncio
import atexit
import errno
import sys
import threading
import traceback
import typing as t
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

import anyio
import anyio.from_thread
import sniffio
import zmq
import zmq_anyio
from anyio import to_thread
from jupyter_client.connect import ConnectionFileMixin
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import Bool, Container, Dict, DottedObjectName, Instance, Set
from traitlets.utils.importstring import import_item
from typing_extensions import override

from ipykernel.kernelbase import Kernelbase, SocketID
from ipykernel.kernelspec import AsyncMode

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from anyio.abc import TaskGroup, TaskStatus

    from ipykernel.iostream import OutStream


def start_anyio_thread(
    func: Callable[[TaskStatus], CoroutineType],
    stop_event: threading.Event,
    tg: TaskGroup,
    *,
    backend: Literal["anyio", "trio", ""] = "",
    name="",
    pydev_do_not_trace=False,
    is_pydev_daemon_thread=False,
):
    """Run a coroutine function in a separate thread (and event loop) and manage its lifecycle using AnyIO.

    This function takes an asynchronous function, a stop event, and a task group,
    and starts the function in a separate thread. It ensures that the function
    is properly started and can be stopped gracefully.

    This coroutine returns once `task_status.started()`is called inside `func`
    running until the `stop_event` is set.

    Args:
        func: The asynchronous function to run in a separate thread. It should
            accept a TaskStatus object as an argument and return a coroutine.
        stop_event: A threading.Event that signals when the function should stop.
        tg: The AnyIO TaskGroup to use for managing the function's task.
        task_status: An AnyIO TaskStatus object to signal when the function has started.
    """

    backend = backend or sniffio.current_async_library()  # type: ignore[no-any-return]

    ready_event = threading.Event()

    def run_func():
        thread = threading.current_thread()
        if name:
            thread.name = name
        thread.pydev_do_not_trace = pydev_do_not_trace  # type: ignore[attr-defined]
        thread.is_pydev_daemon_thread = is_pydev_daemon_thread  # type: ignore[attr-defined]

        async def run_until_stop_event():
            async with anyio.create_task_group() as tg:
                await tg.start(func)
                ready_event.set()

                def _wait_stop():
                    thread = threading.current_thread()
                    thread.pydev_do_not_trace = True  # type: ignore[attr-defined]
                    thread.is_pydev_daemon_thread = True  # type: ignore[attr-defined]
                    stop_event.wait()

                await to_thread.run_sync(_wait_stop)
                tg.cancel_scope.cancel()

        anyio.run(run_until_stop_event, backend=backend)

    tg.start_soon(to_thread.run_sync, run_func)
    return to_thread.run_sync(ready_event.wait)


class Kernel(ConnectionFileMixin, Kernelbase):
    """The IPYKernel application class."""

    _instance: Self | None = None
    _io_modified = Bool(False)
    _socket_threads: Dict[zmq_anyio.Socket, threading.Thread] = Dict()

    _stopped = Instance(anyio.Event, ())
    _stop_thread_event = Instance(threading.Event, ())
    _Kernelbases: Dict[str, Kernelbase] = Dict()

    sockets: Dict[SocketID, zmq_anyio.Socket] = Dict()
    zmq_context = Instance(zmq.Context)
    shell_interrupt: Container[set[threading.Event]] = Set()
    quiet = Bool(True, help="Only send stdout/stderr to output stream").tag(config=True)
    outstream_class = DottedObjectName(
        "ipykernel.iostream.OutStream",
        help="The importstring for the OutStream factory",
        allow_none=True,
    ).tag(
        config=True,
    )
    displayhook_class = DottedObjectName(
        "ipykernel.displayhook.ZMQDisplayHook", help="The importstring for the DisplayHook factory"
    ).tag(config=True)

    def __new__(cls, **kwargs) -> Self:  # noqa: ARG004
        #  There is only one instance.
        if not (instance := cls._instance):
            cls._instance = instance = super().__new__(cls)
        return instance

    def __init__(self, **kwargs):
        if self.shell_handlers:
            return  # Only initialize once
        super().__init__(**kwargs)
        self._Kernelbases.pop(self.ident, None)
        self.control_handlers = {
            "shutdown_request": self.shutdown_request,
            "execute_request": self._execute_request,  # no task queue
        }
        self.init_crash_handler()

    def init_crash_handler(self):
        """Initialize the crash handler."""
        sys.excepthook = self.excepthook

    def excepthook(self, etype, evalue, tb):
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        traceback.print_exception(etype, evalue, tb, file=sys.__stderr__)

    def init_pubio(self):
        """Redirect input streams."""
        if threading.current_thread() != threading.main_thread():
            msg = "pubio expects to be running in the main thread"
            raise RuntimeError(msg)
        self._save_io()
        if self.outstream_class:
            cls: type[OutStream] = import_item(self.outstream_class)
            for name in ["stdout", "stderr"]:
                echo = getattr(sys, name)

                def flusher(string: str, name=name, echo=echo):
                    "Publish stdio or stderr when flush is called"
                    self.pubio_send(
                        msg_or_type="stream",
                        content={"name": name, "text": string},
                        ident=self._topic("status"),
                    )
                    if not self.quiet and echo:
                        echo.write(string)
                        echo.flush()

                wrapper = cls(name=name, flusher=flusher)  # type: ignore[call-arg]
                setattr(sys, name, wrapper)

    def _save_io(self):
        if not self._io_modified:
            self._original_io = sys.stdout, sys.stderr, sys.displayhook
            self._io_modified = True

    def reset_io(self):
        """restore original io

        restores state after init_io
        """
        if not self._io_modified:
            return
        stdout, stderr, displayhook = sys.stdout, sys.stderr, sys.displayhook
        sys.stdout, sys.stderr, sys.displayhook = self._original_io
        self._io_modified = False
        if finish_displayhook := getattr(displayhook, "finish_displayhook", None):
            finish_displayhook()
        if self.outstream_class:
            outstream_factory = import_item(str(self.outstream_class))
            if isinstance(stderr, outstream_factory):
                stderr.close()
            if isinstance(stdout, outstream_factory):
                stdout.close()

    def pubio_send(
        self,
        msg_or_type: dict[str, t.Any] | str,
        content: dict[str, t.Any] | None = None,
        metadata: dict[str, t.Any] | None = None,
        parent: dict[str, t.Any] | None = None,
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes | bytearray] | None = None,
    ):
        "Send the message on the iopub socket"
        if threading.current_thread() is not threading.main_thread():
            # Send the message from the main thread
            try:
                anyio.from_thread.run_sync(self.pubio_send, msg_or_type, content, metadata, parent, ident, buffers)
            except RuntimeError:
                pass
            return
        self.session.send(
            stream=self.sockets[SocketID.iopub],
            msg_or_type=msg_or_type,
            content=content,
            metadata=metadata,
            parent=parent or self.parent_msg,
            ident=ident,
            buffers=buffers,
        )

    def get_socket(
        self,
        socket_id: SocketID,
        socket_type: zmq.SocketType,
        linger: int,
        max_attempts=100,
        context: zmq.Context | None = None,
    ):
        "Open the socket for comms, it will be closed when the context is exited"
        # Create socket
        assert socket_id not in self.sockets
        socket = zmq_anyio.Socket(context or self.zmq_context, socket_type)
        socket.linger = linger
        # Bind port
        port_name = f"{socket_id}_port"
        port = self._bind_socket(socket, getattr(self, port_name, 0), max_attempts)
        setattr(self, port_name, port)
        self.sockets[socket_id] = socket
        self._socket_threads[socket] = threading.current_thread()
        self.log.debug("{%} {%} Channel on port: %i", socket_id, socket_type.name, port)
        return socket

    def _bind_socket(self, socket: zmq.Socket, port: int, max_attempts=100):
        try:
            win_in_use = errno.WSAEADDRINUSE  # type: ignore[attr-defined]
        except AttributeError:
            win_in_use = None

        def _try_bind_socket(port: int):
            if self.transport == "tcp":
                if port <= 0:
                    port = socket.bind_to_random_port(f"{self.transport}://{self.ip}")
                else:
                    socket.bind(f"tcp://{self.ip}:{port}")
            elif self.transport == "ipc":
                if port <= 0:
                    port = 1
                    while True:
                        port = port + 1
                        path = f"{self.ip}-{port}"
                        if Path(path).exists():
                            break
                else:
                    path = f"{self.ip}-{port}"
                socket.bind(f"ipc://{path}")
            return port

        # Try up to 100 times to bind a port when in conflict to avoid
        # infinite attempts in bad setups
        max_attempts = 1 if port else max_attempts
        for attempt in range(max_attempts):
            try:
                return _try_bind_socket(port)
            except zmq.ZMQError as e:
                # Raise if we have any error not related to socket binding
                if e.errno != errno.EADDRINUSE and e.errno != win_in_use:
                    raise
                if attempt == max_attempts - 1:
                    raise
        msg = f"Failed to bind a {socket}:{port}"
        raise RuntimeError(msg)

    async def _heartbeat(self, task_status: TaskStatus):
        """The heartbeat.

        Reference: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
        """
        # Inside heartbeat thread
        socket = self.get_socket(SocketID.heartbeat, zmq.SocketType.ROUTER, 1000)
        await anyio.sleep(1)
        async with socket:
            task_status.started()
            while True:
                data = await socket.arecv_multipart(copy=False).wait()
                socket.send_multipart(data)

    async def _receive_msg_loop(
        self,
        process_message: Callable[[zmq.Socket, list[bytes | bytearray], dict | None, str], CoroutineType],
        socket: zmq_anyio.Socket,
    ):
        """Receive messages from the socket, unpack them and pass them to be processed with process_message.

        Intended to be used with:
         - shell socket
         - control socket
        """
        while True:
            if not (msg_ := await socket.arecv_multipart(copy=False).wait()):
                self.log.error("Empty message received on socket %", socket)
                await anyio.sleep(0.1)
                continue
            copy = not isinstance(msg_[0], zmq.Message)
            idents, msg_ = self.session.feed_identities(msg_, copy=copy)
            msg = self.session.deserialize(msg_, content=True, copy=copy)
            msg_type = msg["header"]["msg_type"]
            self.log.debug("\n*** MESSAGE TYPE:%s***", msg_type)
            self.log.debug("   Content: %s\n   --->\n   ", msg["content"])
            await process_message(socket, idents, msg, msg_type)

    async def shutdown_request(self, socket, idents, msg):
        """Handle a shutdown request."""
        content = await self.do_shutdown(msg["content"]["restart"])
        self.session.send(
            stream=socket,
            msg_or_type="shutdown_reply",
            content=content,
            parent=msg,
            ident=idents,
        )

    async def do_shutdown(self, restart):
        """Handle kernel shutdown."""
        self.shell.exit_now = True
        await self._stopped.wait()
        return {"status": "ok", "restart": restart}

    async def _run_control_loop(self, task_status: TaskStatus):
        # Inside control thread
        # This code runs in a different thread having its own event loop
        async with self.get_socket(SocketID.control, zmq.SocketType.ROUTER, 1000) as control_socket:
            task_status.started()
            await self._receive_msg_loop(self._process_control, socket=control_socket)

    async def _process_control(self, socket, idents, msg, msg_type):
        # Inside control thread

        # Execute_requests
        handler = self.control_handlers.get(msg_type) or self.shell_handlers.get(msg_type)
        if not handler:
            self.log.error("Unknown message type: %r", msg_type)
        else:
            try:
                self._publish_status("busy", msg)
                await handler(socket, idents, msg)
            except Exception as e:
                self.log.error("Exception in message handler:", exc_info=e)
            except KeyboardInterrupt:
                # Ctrl-c shouldn't crash the kernel here.
                self.log.error("KeyboardInterrupt caught in kernel.")
            finally:
                self._publish_status("idle", msg)

    @classmethod
    def start(cls, connection_file="", async_mode=AsyncMode.asyncio) -> int:
        """Start the kernel.

        Or if there is already an anyio event loop running you can use

        ``` python
        async with Kernel().start_in_context() as kernel:
           await anyio.sleep_forever()
        ...

        """
        async_mode = AsyncMode(async_mode)

        async def _start() -> None:
            """ """
            if async_mode is AsyncMode.asyncio_eager and sys.version_info >= (3, 12):
                loop = asyncio.get_running_loop()
                loop.set_task_factory(asyncio.eager_task_factory)

            async with kernel.start_in_context():
                await anyio.sleep_forever()

        kernel = cls()
        kernel.connection_file = connection_file

        if (
            sys.platform == "win32"
            and async_mode in [AsyncMode.asyncio, AsyncMode.asyncio_eager]
            and (policy := asyncio.get_event_loop_policy())
            and policy.__class__.__name__ == "WindowsProactorEventLoopPolicy"
        ):
            from anyio._core._asyncio_selector_thread import get_selector  # noqa: PLC0415

            selector = get_selector()
            selector._thread.pydev_do_not_trace = True
        try:
            anyio.run(_start, backend="trio" if async_mode is AsyncMode.trio else "asyncio")
        finally:
            pass
        return 0

    @asynccontextmanager
    async def start_in_context(self):
        """Start the Kernel in a context with necessary resources.

        This function initializes the kernel's sockets, sets up the ZMQ context,
        creates task groups for asynchronous operations, and starts the main loops
        for handling messages and execution requests. It also manages the
        connection file and performs cleanup operations.
        """
        if self.sockets:
            msg = "Already started"
            raise RuntimeError(msg)
        if self.connection_file and Path(self.connection_file).exists():
            self.load_connection_file()

        with zmq.Context() as zmq_context:
            self.zmq_context = zmq_context
            async with (
                anyio.create_task_group() as tg,
                self.get_socket(SocketID.shell, zmq.SocketType.ROUTER, 1000) as shell_socket,
                self.get_socket(SocketID.iopub, zmq.SocketType.PUB, 1000),
                self.get_socket(SocketID.stdin, zmq.SocketType.ROUTER, 1000),
            ):
                try:
                    self.init_pubio()
                    await start_anyio_thread(self._heartbeat, self._stop_thread_event, tg, name="Heartbeat")
                    await start_anyio_thread(self._run_control_loop, self._stop_thread_event, tg, name="Control")
                    tg.start_soon(self._receive_msg_loop, self._process_shell, shell_socket)
                    tg.start_soon(self._start_async)

                    if not self.connection_file:
                        self.connection_file = str(Path(jupyter_runtime_dir()).joinpath(f"kernel-{self.ident}.json"))
                    self.write_connection_file()
                    self.log.info(
                        'To connect another client to this kernel, use:\n      --existing "%s"', {self.connection_file}
                    )
                    atexit.register(self.cleanup_connection_file)
                    self.comm_manager.kernel = self
                    yield self
                finally:
                    self.comm_manager.kernel = None
                    self.stop()
                    self.reset_io()
                    tg.cancel_scope.cancel()

    @override
    def stop(self):
        self._stop_thread_event.set()
        super().stop()
