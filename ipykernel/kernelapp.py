"""An Application for launching a kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import enum
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
import zmq
import zmq_anyio
from anyio import to_thread
from anyio.abc import TaskGroup
from anyio.from_thread import BlockingPortal
from jupyter_client.connect import ConnectionFileMixin
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import Bool, Container, Dict, DottedObjectName, Instance, Set, Type, Unicode
from traitlets.config import SingletonConfigurable
from traitlets.utils.importstring import import_item

from ipykernel.iostream import OutStream, SocketID
from ipykernel.kernelbase import Kernel
from ipykernel.kernelspec import KERNEL_NAME

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from anyio.abc import TaskStatus

class AsyncMode(enum.StrEnum):
    asyncio = "asyncio"
    trio = "trio"
    asyncio_eager = "asyncio_eager"


def run_in_thread(
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

    import sniffio

    backend = backend or sniffio.current_async_library()  # type: ignore[no-any-return]

    ready_event = threading.Event()

    def run_func():
        thread = threading.current_thread()
        if name:
            thread.name = name
        thread.pydev_do_not_trace = pydev_do_not_trace  # type: ignore  # noqa: PGH003
        thread.is_pydev_daemon_thread = is_pydev_daemon_thread  # type: ignore  # noqa: PGH003

        async def run_until_stop_event():
            async with anyio.create_task_group() as tg:
                await tg.start(func)
                ready_event.set()
                await to_thread.run_sync(stop_event.wait)
                tg.cancel_scope.cancel()

        anyio.run(run_until_stop_event, backend=backend)

    tg.start_soon(to_thread.run_sync, run_func)
    return to_thread.run_sync(ready_event.wait)


class MainKernel(SingletonConfigurable, ConnectionFileMixin, Kernel):
    """The IPYKernel application class."""

    kernel_name = Unicode(KERNEL_NAME)
    _ports = Dict()
    sockets: Dict[SocketID, zmq_anyio.Socket] = Dict()
    _socket_threads: Dict[zmq_anyio.Socket, threading.Thread] = Dict()

    _log_map: Dict[int, t.Any] = Dict()
    _io_modified = Bool(False)
    _tg_main = Instance(TaskGroup)
    _stop_event = Instance(threading.Event, ())
    zmq_context = Instance(zmq.Context)
    shell_interrupt: Container[set[threading.Event]] = Set()
    _stopped = Instance(anyio.Event, ())
    _portal = Instance(BlockingPortal)

    def __new__(cls, **kwargs) -> Self:  # noqa: ARG003
        #  There is only one instance.
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, **kwargs):
        if self.shell_handlers:
            return  # Only initialize once
        super().__init__(**kwargs)
        self.control_handlers = {
            "shutdown_request": self.shutdown_request,
            "debug_request": self.debug_request,
            "execute_request": self._execute_request,  # bypass
        }
        self.init_crash_handler()

    @property
    def ports(self):
        return {
            "shell": self.shell_port,
            "iopub": self.iopub_port,
            "stdin": self.stdin_port,
            "hb": self.hb_port,
            "control": self.control_port,
        }

    quiet = Bool(True, help="Only send stdout/stderr to output stream").tag(config=True)
    outstream_class = DottedObjectName(
        "ipykernel.iostream.OutStream",
        help="The importstring for the OutStream factory",
        allow_none=True,
    ).tag(config=True)
    displayhook_class = DottedObjectName(
        "ipykernel.displayhook.ZMQDisplayHook", help="The importstring for the DisplayHook factory"
    ).tag(config=True)

    def init_crash_handler(self):
        """Initialize the crash handler."""
        sys.excepthook = self.excepthook

    def excepthook(self, etype, evalue, tb):
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        traceback.print_exception(etype, evalue, tb, file=sys.__stderr__)

    def init_pubio(self, iopub_socket: zmq.Socket):
        """Redirect input streams."""
        if threading.current_thread() != threading.main_thread():
            msg = "pubio expects to be running in the main thread"
            raise RuntimeError(msg)

        self._save_io()
        if self.outstream_class:
            cls: type[OutStream] = import_item(self.outstream_class)
            for name in ["stdout", "stderr"]:
                echo = None if self.quiet else getattr(sys, name)
                if echo is not None:
                    echo.flush()

                def flusher(string: str, name=name):
                    "Publish stdio or stderr when flush is called"
                    self.pubio_send(
                        msg_or_type="stream",
                        content={"name": name, "text": string},
                        ident=self._topic("status"),
                    )

                wrapper = cls(name=name, flusher=flusher)  # type: ignore[call-arg]
                setattr(sys, name, wrapper)

    def _save_io(self):
        if not self._io_modified:
            self._original_io = sys.stdout, sys.stderr, sys.displayhook
            self._log_map = {}
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
            self._log_map = {}
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
            anyio.from_thread.run_sync(self.pubio_send, msg_or_type, content, metadata, parent, ident, buffers)
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

    async def init_pdb(self):
        """Replace pdb with IPython's version that is interruptible.

        With the non-interruptible version, stopping pdb() locks up the kernel in a
        non-recoverable state.
        """
        import pdb

        from IPython.core import debugger

        if hasattr(debugger, "InterruptiblePdb"):
            # Only available in newer IPython releases:
            debugger.Pdb = debugger.InterruptiblePdb  # type:ignore[misc]
            pdb.Pdb = debugger.Pdb  # type:ignore[assignment,misc]
            pdb.set_trace = debugger.set_trace

    @asynccontextmanager
    async def start_in_context(self):
        """Start inside the current anyio event loop.

        ``` python
        kernel = MainKernel.instance()
        async kernel.start_in_context():
            await anyio.sleep_forever()
        ```
        """
        if self.sockets:  # TODO: Make change to cls._instance
            msg = "Already started"
            raise RuntimeError(msg)
        if self.connection_file and Path(self.connection_file).exists():
            self.load_connection_file()
        self.zmq_context = zmq.Context()
        try:
            async with (
                anyio.create_task_group() as tg,
                BlockingPortal() as portal,
                self.get_socket(SocketID.shell, zmq.SocketType.ROUTER, 1000) as shell_socket,
                self.get_socket(SocketID.iopub, zmq.SocketType.PUB, 1000) as iopub_socket,
                self.get_socket(SocketID.stdin, zmq.SocketType.ROUTER, 1000),
            ):
                self._portal = portal
                tg.cancel_scope.shield = True
                self._tg_main = tg
                try:
                    self.init_pubio(iopub_socket)
                    await run_in_thread(self._heartbeat, self._stop_event, tg, name="Heartbeat")
                    await run_in_thread(self._run_control_loop, self._stop_event, tg, name="Control")
                    # Ensure sockets are created
                    if missing_sockets := set(SocketID).difference(self.sockets):
                        msg = f"Failed to create sockets: {missing_sockets}"
                        raise RuntimeError(msg)

                    # writing/displaying connection info must be *after* init_sockets/heartbeat
                    if not self.connection_file:
                        self.connection_file = str(Path(jupyter_runtime_dir()).joinpath(f"kernel-{self.ident}.json"))
                    self.write_connection_file()
                    # Log connection info after writing connection file, so that the connection
                    # file is definitely available at the time someone reads the log.
                    self.log.info(
                        'To connect another client to this kernel, use:\n      --existing "%s"', {self.connection_file}
                    )

                    tg.start_soon(self._receive_msg_loop, self._process_shell, shell_socket)
                    tg.start_soon(self._shell_execute_request_loop)
                    tg.start_soon(self.init_pdb)

                    # flush stdout/stderr, so that anything written to these streams during
                    # initialization do not get associated with the first execution request
                    sys.stdout.flush()
                    sys.stderr.flush()
                    self.comm_manager.kernel = self
                    yield
                finally:
                    # enable a message to be sent incase anyone is waiting
                    self._stopped.set()
                    await anyio.sleep(0)
                    self.comm_manager.kernel = None
                    tg.cancel_scope.cancel()

        finally:
            self.zmq_context.destroy(linger=0)
            self.reset_io()
            self.cleanup_connection_file()

    def start_soon(self, func, *args, name: str | None = None):
        "Run a coroutine in the main thread taskgroup."
        try:
            if self._portal._event_loop_thread_id == threading.get_ident():
                self._tg_main.start_soon(func, *args, name=name)
            else:
                self._portal.start_task_soon(func, *args, name=name)
        except Exception:
            self.log.exception("portal call failed")
            raise

    @classmethod
    def start(cls, connection_file="", async_mode=AsyncMode.asyncio) -> int:
        """Start the application.

        Other options:

        Using stored config.

        ``` python
        MainKernel.launch_instance()
        ```

        Or if there is already an anyio event loop running you can use

        ``` python
        async with MainKernel.instance().start_in_context() as kernel:
        ...

        """
        async_mode = AsyncMode(async_mode)

        async def _start() -> None:
            """ """
            if async_mode is AsyncMode.asyncio_eager and sys.version_info >= (3, 11):
                import asyncio

                loop = asyncio.get_running_loop()
                loop.set_task_factory(asyncio.eager_task_factory)

            async with kernel.start_in_context():
                await anyio.sleep_forever()

        kernel = cls.instance()
        kernel.connection_file = connection_file

        if async_mode in [AsyncMode.asyncio, AsyncMode.asyncio_eager] and sys.platform == "win32":
            import asyncio

            policy = asyncio.get_event_loop_policy()
            if policy.__class__.__name__ == "WindowsProactorEventLoopPolicy":
                from anyio._core._asyncio_selector_thread import get_selector

                selector = get_selector()
                selector._thread.pydev_do_not_trace = True
        try:
            anyio.run(_start, backend="trio" if async_mode is AsyncMode.trio else "asyncio")
        finally:
            pass
        return 0

    def stop(self):
        self._stop_event.set()

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

    async def _process_shell(self, socket, idents, msg):
        msg_type = msg["header"]["msg_type"]

        self.log.debug("\n*** MESSAGE TYPE:%s***", msg_type)
        self.log.debug("   Content: %s\n   --->\n   ", msg["content"])

        if msg_type == "execute_request":
            await self.execute_request(socket, idents, msg)
        else:
            handler = self.shell_handlers.get(msg_type)
            if handler is None:
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

    async def _receive_msg_loop(
        self,
        process_message: Callable[[zmq.Socket, list[bytes | bytearray], dict | None], CoroutineType],
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
            await process_message(socket, idents, msg)

    async def shutdown_request(self, socket, ident, parent):
        """Handle a shutdown request."""
        content = await self.do_shutdown(parent["content"]["restart"])
        self.session.send(
            stream=socket,
            msg_or_type="shutdown_reply",
            content=content,
            parent=parent,
            ident=ident,
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

    async def _process_control(self, socket, idents, msg):
        # Inside control thread
        msg_type = msg["header"]["msg_type"]

        self.log.debug("\n*** MESSAGE TYPE:%s***", msg_type)
        self.log.debug("   Content: %s\n   --->\n   ", msg["content"])

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
