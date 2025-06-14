"""An Application for launching a kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import contextlib
import errno
import functools
import os
import sys
import threading
import traceback
import typing as t
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self, Unpack, cast

import anyio
import anyio.from_thread
import anyio.to_thread
import zmq
import zmq_anyio
from anyio import to_thread
from anyio.from_thread import BlockingPortal
from jupyter_client.connect import ConnectionFileMixin
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import Bool, Dict, DottedObjectName, Instance, Integer, Type, Unicode, default
from traitlets.utils.importstring import import_item

from ipykernel.heartbeat import Heartbeat
from ipykernel.iostream import OutStream, SendKwgs, SocketID
from ipykernel.kernelbase import Kernel

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from anyio.abc import TaskGroup, TaskStatus


class MainKernel(ConnectionFileMixin, Kernel):
    """The IPYKernel application class."""

    # the kernel class, as an importstring
    kernel_class = Type(
        cast("type[Kernel]", "ipykernel.kernelbase.Kernel"),
        klass=Kernel,
        help="""The Kernel subclass to be used.

    This should allow easy reuse of the IPKernelApp entry point
    to configure and launch kernels other than IPython's own.
    """,
    ).tag(config=True)

    heartbeat = Instance(Heartbeat)
    _ports = Dict()
    sockets: Dict[SocketID, zmq_anyio.Socket] = Dict()
    _socket_threads: Dict[zmq_anyio.Socket, threading.Thread] = Dict()

    _log_map: Dict[int, t.Any] = Dict()
    _io_modified = Bool(False)
    _running = False
    # connection info:
    connection_dir = Unicode()
    _instance = None
    _stop_event = Instance(threading.Event, ())
    zmq_context = Instance(zmq.Context)

    def __new__(cls) -> Self:
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
        }
        # self.init_connection_file()
        self.init_crash_handler()

    @default("connection_dir")
    def _default_connection_dir(self):
        return jupyter_runtime_dir()

    @property
    def abs_connection_file(self):
        if Path(self.connection_file).name == self.connection_file and self.connection_dir:
            return str(Path(str(self.connection_dir)) / self.connection_file)
        return self.connection_file

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

    # polling
    parent_handle = Integer(
        int(os.environ.get("JPY_PARENT_PID") or 0),
        help="""kill this process if its parent dies.  On Windows, the argument
        specifies the HANDLE of the parent process, otherwise it is simply boolean.
        """,
    ).tag(config=True)
    interrupt = Integer(
        int(os.environ.get("JPY_INTERRUPT_EVENT") or 0),
        help="""ONLY USED ON WINDOWS
        Interrupt this process when the parent is signaled.
        """,
    ).tag(config=True)

    def init_crash_handler(self):
        """Initialize the crash handler."""
        sys.excepthook = self.excepthook

    def excepthook(self, etype, evalue, tb):
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        traceback.print_exception(etype, evalue, tb, file=sys.__stderr__)

    # def write_connection_file(self, **kwargs: t.Any) -> None:
    #     """write connection info to JSON file"""

    #     cf = self.abs_connection_file
    #     connection_info = {
    #         "ip": self.ip,
    #         "key": self.session.key,
    #         "transport": self.transport,
    #         "shell_port": self.shell_port,
    #         "stdin_port": self.stdin_port,
    #         "hb_port": self.hb_port,
    #         "iopub_port": self.iopub_port,
    #         "control_port": self.control_port,
    #         "kernel_name": self.kernel_name,
    #     }
    #     if Path(cf).exists():
    #         # If the file exists, merge our info into it. For example, if the
    #         # original file had port number 0, we update with the actual port
    #         # used.
    #         existing_connection_info = get_connection_info(cf, unpack=True)
    #         assert isinstance(existing_connection_info, dict)
    #         connection_info = dict(existing_connection_info, **connection_info)
    #         if connection_info == existing_connection_info:
    #             self.log.debug("Connection file %s with current information already exists", cf)
    #             return

    #     self.log.debug("Writing connection file: %s", cf)
    #     write_connection_file(cf, **connection_info)

    # def cleanup_connection_file(self):
    #     """Clean up our connection file."""
    #     cf = self.abs_connection_file
    #     self.log.debug("Cleaning up connection file: %s", cf)
    #     try:
    #         Path(cf).unlink()
    #     except OSError:
    #         pass

    #     self.cleanup_ipc_files()

    # def init_connection_file(self):
    #     """Initialize our connection file."""
    #     if not self.connection_file:
    #         self.connection_file = f"kernel-{os.getpid()}.json"
    #     try:
    #         self.connection_file = filefind(self.connection_file, [".", self.connection_dir])
    #     except OSError:
    #         self.log.debug("Connection file not found: %s", self.connection_file)
    #         # This means I own it, and I'll create it in this directory:
    #         Path(self.abs_connection_file).parent.mkdir(mode=0o700, exist_ok=True, parents=True)
    #         # Also, I will clean it up:
    #         atexit.register(self.cleanup_connection_file)
    #         return
    #     try:
    #         self.load_connection_file()
    #     except Exception:
    #         self.log.error(
    #             "Failed to load connection file: %r", self.connection_file, exc_info=True
    #         )

    def log_connection_info(self):
        """display connection info, and store ports"""
        basename = Path(self.connection_file).name
        if basename == self.connection_file or str(Path(self.connection_file).parent) == self.connection_dir:
            # use shortname
            tail = basename
        else:
            tail = self.connection_file
        lines = [
            "To connect another client to this kernel, use:",
            "    --existing %s" % tail,
        ]
        # log connection info
        # info-level, so often not shown.
        # frontends should use the %connect_info magic
        # to see the connection info
        for line in lines:
            self.log.info(line)

    def init_pubio(self, iopub_socket: zmq.Socket):
        """Redirect input streams and set a display hook."""
        self._save_io()
        if self.outstream_class:
            cls: type[OutStream] = import_item(self.outstream_class)
            for name in ["stdout", "stderr"]:
                echo = None if self.quiet else getattr(sys, name)
                if echo is not None:
                    echo.flush()

                def flush(string: str, name=name):
                    self.send(
                        stream=iopub_socket,
                        msg_or_type="stream",
                        content={"name": name, "text": string},
                        parent=self.parent_msg,
                        ident=self._topic("status"),
                    )

                wrapper = cls(name=name, flusher=flush)  # type: ignore
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
        MainKernel()
        async app.start_here():
            await anyio.sleep_forever()
        # app has closed
        ```
        """
        # TODO: convert to a classmethod, start the instance and accept configuration
        if self.sockets:  # TODO: Make change to cls._instance
            msg = "Already started"
            raise RuntimeError(msg)
        self.zmq_context = zmq.Context()
        try:
            async with (
                anyio.create_task_group() as tg,
                BlockingPortal() as portal,
                self.get_socket(SocketID.shell, zmq.SocketType.ROUTER, 1000) as shell_socket,
                self.get_socket(SocketID.iopub, zmq.SocketType.PUB, 1000) as iopub_socket,
                self.get_socket(SocketID.stdin, zmq.SocketType.ROUTER, 1000),
            ):
                self._shell_portal = portal
                try:
                    await tg.start(start_thread_wait_ready, self._run_control_loop)
                    await tg.start(start_thread_wait_ready, self._heartbeat_thread)
                    self.init_pubio(iopub_socket)
                    # Ensure sockets are created
                    if missing_sockets := set(self.sockets).difference(SocketID):
                        msg = f"Failed to create sockets: {missing_sockets}"
                        raise RuntimeError(msg)

                    # writing/displaying connection info must be *after* init_sockets/heartbeat
                    self.write_connection_file()
                    # Log connection info after writing connection file, so that the connection
                    # file is definitely available at the time someone reads the log.
                    self.log_connection_info()

                    tg.start_soon(self._receive_msg_loop, self._process_shell, shell_socket)
                    tg.start_soon(self._shell_execute_request_loop)
                    tg.start_soon(self.init_pdb)
                    self.init_shell()

                    # flush stdout/stderr, so that anything written to these streams during
                    # initialization do not get associated with the first execution request
                    sys.stdout.flush()
                    sys.stderr.flush()
                    self.comm_manager.kernel = self
                    yield self
                finally:
                    self.comm_manager.kernel = None
                    self.stop()
                    tg.cancel_scope.cancel()

        finally:
            # while self.sockets:
            #     await anyio.sleep(0.01)
            #     for socket in self.sockets.values():
            #         socket.close(linger=0)
            self.zmq_context.destroy(linger=0)
            self.reset_io()
            self.cleanup_connection_file()
            self.__class__._instance = None

    def start_soon(self, func, *args, name: str | None = None):
        "Run a coroutine in the main thread taskgroup."
        try:
            if self._shell_portal._event_loop_thread_id == threading.get_ident():
                self._tg_main.start_soon(func, *args, name=name)
            else:
                self._shell_portal.start_task_soon(func, *args, name=name)
        except Exception:
            self.log.exception("portal call failed")
            raise

    def send(self, **kwgs: Unpack[SendKwgs]):
        target_thread = self._socket_threads.get(kwgs["stream"])  # type: ignore
        if target_thread is not threading.current_thread():
            try:
                anyio.from_thread.run_sync(functools.partial(super().send, **kwgs))
                return
            except Exception:
                pass
        super().send(**kwgs)

    # async def __DELETE_ME_start(self, tg: anyio.abc.TaskGroup) -> None:
    #     """Process messages on shell and control channels"""
    #     self._tg_main = tg
    #     self.control_stop = threading.Event()
    #     if not self._is_test and self.control_socket is not None:
    #         if self.control_thread:
    #             self.control_thread.start_soon(self.control_main)
    #             self.control_thread.start()
    #         else:
    #             tg.start_soon(self.control_main)

    #     self.shell_interrupt: queue.Queue[bool] = queue.Queue()
    #     self.shell_is_awaiting = False
    #     self.shell_is_blocking = False
    #     self.shell_stop = threading.Event()

    #     tg.start_soon(self.shell_main, None)
    #     await self._main_subshell_ready.wait()
    #     if self.shell_channel_thread:
    #         # Assign tasks to and start shell channel thread.
    #         manager = self.shell_channel_thread.manager
    #         self.shell_channel_thread.start_soon(self.shell_channel_thread_main)
    #         self.shell_channel_thread.start_soon(
    #             partial(manager.listen_from_control, self.shell_main, self.shell_channel_thread)
    #         )
    #         self.shell_channel_thread.start_soon(manager.listen_from_subshells)
    #         self.shell_channel_thread.start()

    def start(self, backend: Literal["trio", "asyncio", "asyncio_eager"] = "asyncio") -> None:
        """Start the application."""

        # if backend == "asyncio" and sys.platform == "win32":
        #     import asyncio

        #     policy = asyncio.get_event_loop_policy()
        #     if policy.__class__.__name__ == "WindowsProactorEventLoopPolicy":
        #         from anyio._core._asyncio_selector_thread import get_selector

        #         selector = get_selector()
        #         selector._thread.pydev_do_not_trace = True

        async def _start() -> None:
            """ """
            async with self.start_in_context():
                # Block forever
                await anyio.Event().wait()

        anyio.run(_start, backend=backend)

    def stop(self):
        self._stop_event.set()

    def get_socket(
        self,
        socket_id: SocketID,
        socket_type: zmq.SocketType,
        linger: int,
        max_attempts=100,
        context: zmq.Context | None = None,
        tg: TaskGroup = None,
    ):
        "Open the socket for comms, it will be closed when the context is exited"
        # Create socket
        assert socket_id not in self.sockets
        socket = zmq_anyio.Socket(context or self.zmq_context, socket_type, task_group=tg)
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
        if msg_type == "execute_request":
            await self.execute_request(socket, idents, msg)
            self._publish_status("busy", msg)

        # Print some info about this message and leave a '--->' marker, so it's
        # easier to trace visually the message chain when debugging.  Each
        # handler prints its message at the end.
        self.log.debug("\n*** MESSAGE TYPE:%s***", msg_type)
        self.log.debug("   Content: %s\n   --->\n   ", msg["content"])

        handler = self.shell_handlers.get(msg_type)
        if handler is None:
            self.log.error("Unknown message type: %r", msg_type)
        else:
            self.log.debug("%s: %s", msg_type, msg)
            try:
                await handler(idents, idents, msg)
            except Exception as e:
                self.log.error("Exception in message handler:", exc_info=e)
            except KeyboardInterrupt:
                # Ctrl-c shouldn't crash the kernel here.
                self.log.error("KeyboardInterrupt caught in kernel.")
        if msg_type != "execute_request":
            self._publish_status("idle", msg)

    # async def shell_main(self, socket: zmq_anyio.Socket, *, task_status: TaskStatus):
    #     """Main loop for a single subshell."""
    #     async with create_task_group() as tg:
    #         try:
    #             await tg.start(self._process_message_loop, SocketID.shell, self._process_shell, tg)
    #             tg.start_soon(self._execute_request_loop, receive_stream)
    #             async with create_task_group() as tg_main:
    #                 tg_main.cancel_scope.shield = True
    #                 self._tg_main = tg_main
    #                 async with BlockingPortal() as portal:
    #                     # Provide a portal for general threadsafe access
    #                     self._shell_portal = portal
    #                     task_status.started()
    #                     await to_thread.run_sync(self._stop_event.wait)
    #                     await portal.stop(True)
    #                 tg_main.cancel_scope.cancel()
    #                 tg.cancel_scope.cancel()
    #         except BaseException:
    #             raise
    #             # if not self.shell_stop.is_set():
    #             #     raise
    #         finally:
    #             self._stop_event.set()
    #             await self._send_exec_request.aclose()
    #             await receive_stream.aclose()

    async def _receive_msg_loop(
        self,
        process_message: Callable[[zmq.Socket, list[bytes | bytearray], dict | None], CoroutineType],
        socket: zmq_anyio.Socket,
    ):
        """Receive messages  from the socket, unpack themm and pass them to be processed with process_message.

        Intended to be used with:
         - shell socket
         - control socket
        """
        while True:
            # try:
            future = socket.arecv_multipart(copy=False)
            if not (msg_ := await future.wait()):
                self.log.error("Empty message received on socket %", socket)
                await anyio.sleep(0.1)
                continue
            copy = not isinstance(msg_[0], zmq.Message)
            idents, msg_ = self.session.feed_identities(msg_, copy=copy)
            msg = self.session.deserialize(msg_, content=True, copy=copy)
            await process_message(socket, idents, msg)
        # except BaseException as e:
        #     self.log.error("Invalid Message", exc_info=e)
        #     continue

    async def shutdown_request(self, socket, ident, parent):
        """Handle a shutdown request."""
        content = await self.do_shutdown(parent["content"]["restart"])
        self.send(
            stream=socket,
            msg_or_type="shutdown_reply",
            content=content,
            parent=parent,
            ident=ident,
        )
        self.stop()

    async def do_shutdown(self, restart):
        """Handle kernel shutdown."""
        if self.shell:
            self.shell.exit_now = True
        return {"status": "ok", "restart": restart}

    def _run_control_loop(self, ready_event):
        # This code runs in a different thread with a different event loop

        async def start_control():
            async def _process_control(socket, idents, msg):
                msg_type = msg["header"]["msg_type"]
                if msg_type != "execute_request":
                    self._publish_status("busy", msg)

                # Print some info about this message and leave a '--->' marker, so it's
                # easier to trace visually the message chain when debugging.  Each
                # handler prints its message at the end.
                self.log.debug("\n*** MESSAGE TYPE:%s***", msg_type)
                self.log.debug("   Content: %s\n   --->\n   ", msg["content"])

                handler = self.shell_handlers.get(msg_type)
                if handler is None:
                    self.log.error("Unknown message type: %r", msg_type)
                else:
                    self.log.debug("%s: %s", msg_type, msg)
                    try:
                        await handler(idents, msg)
                    except Exception as e:
                        self.log.error("Exception in message handler:", exc_info=e)
                    except KeyboardInterrupt:
                        # Ctrl-c shouldn't crash the kernel here.
                        self.log.error("KeyboardInterrupt caught in kernel.")
                if msg_type != "execute_request":
                    self._publish_status("idle", msg)

            async with (
                anyio.create_task_group() as tg,
                self.get_socket(SocketID.control, zmq.SocketType.ROUTER, 1000) as socket,
            ):
                tg.start_soon(self._receive_msg_loop, _process_control, socket)
                await set_ready_wait_stop(ready_event, self._stop_event)
                tg.cancel_scope.cancel()

        anyio.run(start_control)

        # self.log.debug("Control received: %s", msg)
        # self._publish_status("busy", msg)
        # header = msg["header"]
        # msg_type = header["msg_type"]

        # handler = self.control_handlers.get(msg_type, None)
        # if handler is None:
        #     self.log.error("UNKNOWN CONTROL MESSAGE TYPE: %r", msg_type)
        # else:
        #     try:
        #         result = handler(self.control_socket, idents, msg)
        #         if inspect.isawaitable(result):
        #             await result
        #         else:
        #             # If the handler is not awaitable, ensure it completes before proceeding
        #             time.sleep(0.00001)  # Small delay to ensure sequential processing
        #     except Exception:
        #         self.log.error("Exception in control handler:", exc_info=True)
        # if sys.stdout is not None:
        #     sys.stdout.flush()
        # if sys.stderr is not None:
        #     sys.stderr.flush()
        # self._publish_status("idle", msg)

        # Wait for the control thread to be ready

    def _heartbeat_thread(self, ready_event: threading.Event):
        """The heartbeat run in its own thread.

        Reference: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels
        """

        context = zmq.Context()

        async def run_heartbeat():
            async def heartbeat_loop(*, task_status: TaskStatus):
                socket = self.get_socket(SocketID.heartbeat, zmq.SocketType.ROUTER, 1000, context=context)
                count = 0
                await anyio.sleep(1)
                async with socket:
                    task_status.started()
                    count += 1
                    while True:
                        data = await socket.arecv_multipart(copy=False).wait()
                        socket.send_multipart(data)

            try:
                async with anyio.create_task_group() as tg:
                    await tg.start(heartbeat_loop)
                    await set_ready_wait_stop(ready_event, self._stop_event)
                    tg.cancel_scope.cancel()
            finally:
                context.destroy(linger=0)

        # TODO: see if we can push this call to `start_thread_wait_ready`
        anyio.run(run_heartbeat)


async def start_thread_wait_ready(func: Callable[[threading.Event], None], *, task_status: TaskStatus):
    async with anyio.create_task_group() as tg:
        ready_event = threading.Event()
        tg.start_soon(to_thread.run_sync, func, ready_event)
        await to_thread.run_sync(ready_event.wait)
        task_status.started()


async def set_ready_wait_stop(ready_event: threading.Event | None, stop_event: threading.Event):
    def _set_ready_wait():
        if ready_event:
            ready_event.set()
        stop_event.wait()

    await to_thread.run_sync(_set_ready_wait)