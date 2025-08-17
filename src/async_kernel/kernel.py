# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import asyncio
import atexit
import builtins
import contextlib
import contextvars
import errno
import functools
import getpass
import logging
import os
import pathlib
import signal
import sys
import threading
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from logging import Logger, LoggerAdapter
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

import anyio
import sniffio
import zmq
from IPython.core.completer import provisionalcompleter as _provisionalcompleter
from IPython.core.completer import rectify_completions as _rectify_completions
from IPython.core.error import StdinNotImplementedError
from IPython.utils.tokenutil import token_at_cursor
from jupyter_client.connect import ConnectionFileMixin
from jupyter_client.session import Session
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import CaselessStrEnum, CBool, Container, Dict, Float, Instance, Int, Set, Tuple, UseEnum, default
from zmq import Context, Flag, PollEvent, Socket, SocketOption, SocketType, ZMQError

from async_kernel import Caller, _version, utils
from async_kernel.asyncshell import AsyncInteractiveShell
from async_kernel.debugger import Debugger
from async_kernel.iostream import OutStream
from async_kernel.kernelspec import Backend, KernelName
from async_kernel.typing import (
    CODE_MODE_MAPPINGS,
    ExecuteContent,
    HandlerType,
    Job,
    MetadataKeys,
    MsgType,
    NoValue,
    RunMode,
    SocketID,
    Tags,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator
    from types import FrameType

    from anyio.abc import TaskStatus
    from IPython.core.interactiveshell import ExecutionResult

    from async_kernel.comm import CommManager


__all__ = ["Kernel", "KernelInterruptError"]


def error_to_dict(error: BaseException) -> dict[str, str | list[str]]:
    """Convert the error to a dict.

    ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#request-reply
    """
    return {
        "status": "error",
        "ename": type(error).__name__,
        "evalue": str(error),
        "traceback": traceback.format_exception(error),
    }


def bind_socket(
    socket: Socket[SocketType],
    transport: Literal["tcp", "ipc"],
    ip: str,
    port: int = 0,
    max_attempts: int | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
) -> int:
    """Bind the socket to a port using the settings.

    max_attempts: The maximum number of attempts to bind the socket. If un-specified,
    defaults to 100 if port missing, else 2 attempts.
    """

    def _try_bind_socket(port: int):
        if transport == "tcp":
            if not port:
                port = socket.bind_to_random_port(f"tcp://{ip}")
            else:
                socket.bind(f"tcp://{ip}:{port}")
        elif transport == "ipc":
            if not port:
                port = 1
                while True:
                    port = port + 1
                    path = f"{ip}-{port}"
                    if not Path(path).exists():
                        break
            else:
                path = f"{ip}-{port}"
            socket.bind(f"ipc://{path}")
        return port

    if transport == "ipc":
        ip = Path(ip).as_posix()
    if socket.TYPE == SocketType.ROUTER:
        # ref: https://github.com/ipython/ipykernel/issues/270
        socket.router_handover = 1
    try:
        win_in_use = errno.WSAEADDRINUSE  # type: ignore[attr-defined]
    except AttributeError:
        win_in_use = None
    # Try up to 100 times to bind a port when in conflict to avoid
    # infinite attempts in bad setups
    if max_attempts is NoValue:
        max_attempts = 2 if port else 100
    e = None
    for _ in range(max_attempts):
        try:
            return _try_bind_socket(port)
        except ZMQError as e_:
            # Raise if we have any error not related to socket binding
            # 135: Protocol not supported
            if e_.errno in {errno.EADDRINUSE, win_in_use, 135}:
                e = e_
                break
            if port:
                time.sleep(1)
    msg = f"Failed to bind {socket} for {transport=}" + (f" to {port=}!" if port else "!")
    raise RuntimeError(msg) from e

@functools.cache
def wrap_handler(run_handler:Callable[[HandlerType, Job]], handler:HandlerType) -> HandlerType:

    async def queued_handler(job: Job) -> None:
        await run_handler(handler, job)
    return queued_handler


class KernelInterruptError(InterruptedError):
    "Raised to interrupt the kernel."

    # We subclass from InterruptedError so if the backend is a SelectorEventLoop it can catch the exception.
    # Other event loops don't appear to have this issue.


class Kernel(ConnectionFileMixin):
    """An asynchronous kernel with an anyio backend providing an IPython AsyncInteractiveShell with zmq sockets.

    To start the kernel.

    === Shell

    At the command prompt.

    ``` shell
    async-kernel -f .
    ```

    === Normal
        ``` python
        from async_kernel.__main__ import main

        main()
        ```

    === Direct

        ``` python
        Kernel.start()
        ```

    === Asynchronously inside anyio event loop.

        ``` python
        kernel = Kernel()
        async with kernel.start_in_context():
            await anyio.sleep_forever()
    ```

    """

    _job_var: ClassVar[contextvars.ContextVar[Job]] = contextvars.ContextVar("job")
    _execution_count_var: ClassVar[contextvars.ContextVar[int]] = contextvars.ContextVar("execution_count")
    _instance: Self | None = None
    _interrupt_requested = False
    _last_interrupt_frame = None
    _stop_event = Instance(threading.Event, ())
    _stop_on_error_time: float = 0
    _interrupts: Container[set[Callable[[], object]]] = Set()
    _sockets: Dict[SocketID, zmq.Socket] = Dict()
    message_handlers: Dict[Literal[SocketID.shell, SocketID.control], dict[MsgType, HandlerType]] = Dict()
    _execution_count = Int(0)
    anyio_backend = UseEnum(Backend)
    help_links = Tuple()
    quiet = CBool(True, help="Only send stdout/stderr to output stream")
    shell = Instance(AsyncInteractiveShell)
    session = Instance(Session)
    log = Instance(logging.LoggerAdapter)
    debugger = Instance(Debugger, ())
    comm_manager: Instance[CommManager] = Instance("async_kernel.comm.CommManager")
    transport: CaselessStrEnum[str] = CaselessStrEnum(
        ["tcp", "ipc"] if sys.platform == "linux" else ["tcp"], default_value="tcp", config=True
    )
    cell_execute_timeout = Float(None, allow_none=True)
    "A default timeout to apply to use for non-silent [execute requests][async_kernel.Kernel.execute_request]."

    def __new__(cls, **kwargs) -> Self:  # noqa: ARG004
        #  There is only one instance.
        if not (instance := cls._instance):
            cls._instance = instance = super().__new__(cls)
        return instance

    def __init__(self, **kwargs) -> None:
        if self.message_handlers:
            return  # Only initialize once
        super().__init__(**kwargs)
        self.message_handlers[SocketID.shell] = {
            MsgType.execute_request: self.execute_request,
            MsgType.kernel_info_request: self.kernel_info_request,
            MsgType.comm_info_request: self.comm_info_request,
            MsgType.interrupt_request: self.interrupt_request,
            MsgType.complete_request: self.complete_request,
            MsgType.is_complete_request: self.is_complete_request,
            MsgType.inspect_request: self.inspect_request,
            MsgType.history_request: self.history_request,
            MsgType.comm_open: self.comm_open,
            MsgType.comm_msg: self.comm_msg,
            MsgType.comm_close: self.comm_close,
        }
        self.message_handlers[SocketID.control] = self.message_handlers[SocketID.shell] | {
            MsgType.shutdown_request: self.control_shutdown_request,
            MsgType.debug_request: self.debug_request,
        }
        sys.excepthook = self.excepthook
        sys.unraisablehook = self.unraisablehook
        signal.signal(signal.SIGINT, self._signal_handler)
        if not os.environ.get("MPLBACKEND"):
            os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"

    @property
    def job(self) -> Job | dict:
        "The job in context of the current coroutine."
        try:
            return self._job_var.get()
        except LookupError:
            return {}

    @property
    def execution_count(self) -> int:
        "The execution count in context of the current coroutine, else the current value if there isn't one in context."
        return self._execution_count_var.get(self._execution_count)

    @property
    def kernel_info(self) -> dict[str, str | dict[str, str | dict[str, str | int]] | Any | tuple[Any, ...] | bool]:
        return {
            "protocol_version": _version.kernel_protocol_version,
            "implementation": "async_kernel",
            "implementation_version": _version.__version__,
            "language_info": _version.language_info,
            "banner": self.shell.banner,
            "help_links": self.help_links,
            "debugger": not utils.LAUNCHED_BY_DEBUGPY,
            "kernel_name": self.kernel_name,
        }

    @default("help_links")
    def _default_help_links(self) -> tuple[dict[str, str], ...]:
        return (
            {
                "text": "Async Kernel Reference ",
                "url": "TODO",
            },
            {
                "text": "IPython Reference",
                "url": "https://ipython.readthedocs.io/en/stable/",
            },
            {
                "text": "IPython magic Reference",
                "url": "https://ipython.readthedocs.io/en/stable/interactive/magics.html",
            },
            {
                "text": "Matplotlib ipympl Reference",
                "url": "https://matplotlib.org/ipympl/",
            },
            {
                "text": "Matplotlib Reference",
                "url": "https://matplotlib.org/contents.html",
            },
        )

    @default("log")
    def _default_log(self) -> LoggerAdapter[Logger]:
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    @default("kernel_name")
    def _default_kernel_name(self) -> Literal[KernelName.trio, KernelName.asyncio]:
        try:
            if sniffio.current_async_library() == "trio":
                return KernelName.trio
        except Exception:
            pass
        return KernelName.asyncio

    @default("comm_manager")
    def _default_comm_manager(self) -> CommManager:
        from async_kernel import comm  # noqa: PLC0415

        comm.set_comm()
        return comm.get_comm_manager()

    @default("session")
    def _default_session(self) -> Any:
        return Session(parent=self)

    @default("shell")
    def _default_shell(self) -> AsyncInteractiveShell:
        return AsyncInteractiveShell.instance(parent=self, kernel=self)

    @classmethod
    def stop(cls) -> None:
        """Stop the kernel."""
        if instance := cls._instance:
            cls._instance = None
            instance._stop_event.set()

    @asynccontextmanager
    async def start_in_context(self) -> AsyncGenerator[Self, Any]:
        """Start the Kernel in an already running anyio event loop."""
        if self._sockets:
            msg = "Already started"
            raise RuntimeError(msg)
        self.CancelledError = anyio.get_cancelled_exc_class()
        self.anyio_backend = sniffio.current_async_library()
        if self.connection_file and Path(self.connection_file).exists():
            self.load_connection_file()
        try:
            async with Caller(log=self.log, create=True, protected=True) as caller:
                tg = caller._taskgroup  # pyright: ignore[reportPrivateUsage]
                assert tg
                await tg.start(self._wait_stopped)
                try:
                    await tg.start(self._start_heartbeat)
                    await tg.start(self._start_stdin)
                    await tg.start(self._start_iopub_proxy)
                    await tg.start(self._start_control_loop)
                    await tg.start(self._receive_msg_loop, SocketID.shell)
                    assert len(self._sockets) == len(SocketID)
                    if not self.connection_file:
                        self.connection_file = str(Path(jupyter_runtime_dir()).joinpath(f"kernel-{uuid.uuid4()}.json"))
                    pathlib.Path(self.connection_file).parent.mkdir(parents=True, exist_ok=True)
                    self.write_connection_file()
                    atexit.register(self.cleanup_connection_file)
                    print(
                        f"""Kernel started with backend: {self.anyio_backend}. To connect a client use: --existing "{self.connection_file}" """
                    )
                    await tg.start(self._start_iopub)
                    yield self
                finally:
                    self.stop()
        finally:
            Context.instance().term()

    def _signal_handler(self, signum, frame: FrameType | None) -> None:
        "Handle interrupt signals."
        if self._interrupt_requested:
            self._interrupt_requested = False
            if frame and frame.f_locals is self.shell.user_ns:
                raise KernelInterruptError
            if self._last_interrupt_frame is frame:
                # A blocking call that is not an execute_request
                raise KernelInterruptError
            self._last_interrupt_frame = frame
        else:
            signal.default_int_handler(signum, frame)

    async def _start_heartbeat(self, task_status: TaskStatus[None]) -> None:
        # Reference: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels

        def heartbeat():
            socket: Socket = Context.instance().socket(zmq.ROUTER)
            with utils.do_not_debug_this_thread("heartbeat"), self._bind_socket(SocketID.heartbeat, socket):
                ready_event.set()
                try:
                    zmq.proxy(socket, socket)
                except zmq.ContextTerminated:
                    return

        ready_event = threading.Event()
        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        heartbeat_thread.start()
        ready_event.wait(10)
        task_status.started()

    async def _start_stdin(self, task_status: TaskStatus[None]) -> None:
        socket = Context.instance().socket(SocketType.ROUTER)
        with self._bind_socket(SocketID.stdin, socket), contextlib.suppress(self.CancelledError):
            task_status.started()
            await anyio.sleep_forever()

    async def _start_iopub_proxy(self, task_status: TaskStatus[None]) -> None:
        """Provide an io proxy"""

        def pub_proxy():
            # We use an internal proxy to collect pub messages for distribution.
            # Each thread needs to open its own socket to publish to the internal proxy.
            # When thread-safe sockets become available, this could be changed...
            # Ref: https://zguide.zeromq.org/docs/chapter2/#Working-with-Messages (fig 14)
            frontend: zmq.Socket = Context.instance().socket(zmq.XSUB)
            frontend.bind(Caller.iopub_url)
            iopub_socket: zmq.Socket = Context.instance().socket(zmq.XPUB)
            with utils.do_not_debug_this_thread("iopub"), self._bind_socket(SocketID.iopub, iopub_socket):
                ready_event.set()
                try:
                    zmq.proxy(frontend, iopub_socket)
                except zmq.ContextTerminated:
                    frontend.close(linger=500)

        ready_event = threading.Event()
        iopub_thread = threading.Thread(target=pub_proxy, name="iopub proxy", daemon=True)
        iopub_thread.start()
        ready_event.wait(10)
        task_status.started()

    async def _start_iopub(self, task_status: TaskStatus[None]) -> None:
        # Save IO
        self._original_io = sys.stdout, sys.stderr, sys.displayhook, builtins.input, self.getpass

        builtins.input = self.raw_input
        getpass.getpass = self.getpass
        for name in ["stdout", "stderr"]:

            def flusher(string: str, name=name):
                "Publish stdio or stderr when flush is called"
                self.iopub_send(
                    msg_or_type="stream",
                    content={"name": name, "text": string},
                    ident=f"stream.{name}".encode(),
                )
                if not self.quiet and (echo := (sys.__stdout__ if name == "stdout" else sys.__stderr__)):
                    echo.write(string)
                    echo.flush()

            wrapper = OutStream(flusher=flusher)
            setattr(sys, name, wrapper)
        task_status.started()
        self.comm_manager.kernel = self
        try:
            await anyio.sleep_forever()
        except self.CancelledError:
            return
        finally:
            self.comm_manager.kernel = None
            # Reset IO
            sys.stdout, sys.stderr, sys.displayhook, builtins.input, getpass.getpass = self._original_io

    async def _start_control_loop(self, task_status: TaskStatus[None]) -> None:
        async def run_in_control_event_loop():
            assert caller._taskgroup  # pyright: ignore[reportPrivateUsage]
            await caller._taskgroup.start(self._receive_msg_loop, SocketID.control)  # pyright: ignore[reportPrivateUsage]
            ready_event.set()

        self.control_thread_caller = caller = Caller.start_new(
            backend=self.anyio_backend, name="ControlThread", protected=True
        )
        ready_event = threading.Event()
        caller.call_soon(run_in_control_event_loop)
        ready_event.wait(10)
        task_status.started()

    async def _wait_stopped(self, task_status: TaskStatus[None]) -> None:
        task_status.started()
        try:
            await utils.wait_thread_event(self._stop_event)
        except BaseException:
            pass
        Caller.stop_all(_stop_protected=True)

    async def _receive_msg_loop(
        self, socket_id: Literal[SocketID.control, SocketID.shell], *, task_status: TaskStatus[None]
    ) -> None:
        """Receive shell and control messages over zmq sockets."""
        if (
            sys.platform == "win32"
            and sniffio.current_async_library() == "asyncio"
            and (policy := asyncio.get_event_loop_policy())
            and policy.__class__.__name__ == "WindowsProactorEventLoopPolicy"
        ):
            from anyio._core._asyncio_selector_thread import get_selector  # noqa: PLC0415

            utils.mark_thread_pydev_do_not_trace(get_selector()._thread)  # pyright: ignore[reportPrivateUsage]
        socket: Socket[Literal[SocketType.ROUTER]] = Context.instance().socket(SocketType.ROUTER)
        with self._bind_socket(socket_id, socket):
            try:
                task_status.started()
                while True:
                    while socket.get(SocketOption.EVENTS) & PollEvent.POLLIN:  # pyright: ignore[reportOperatorIssue]
                        try:
                            ident, msg = self.session.recv(socket, copy=False)
                            assert ident
                            assert msg
                            if socket_id == SocketID.shell:
                                # Reset the frame to show the main thread is not blocked.
                                self._last_interrupt_frame = None
                            self.log.debug("*** _receive_msg_loop %s*** %s", socket_id, msg)
                            await self.handle_message_request(
                                Job(
                                    socket_id=socket_id,
                                    socket=socket,
                                    ident=ident,
                                    msg=msg,  # pyright: ignore[reportArgumentType]
                                    received_time=time.monotonic(),
                                )
                            )
                        except Exception as e:
                            self.log.debug("Bad message on %s: %s", socket_id, e)
                            continue
                        await anyio.sleep(0)
                    await anyio.wait_readable(socket)
            except (zmq.ContextTerminated, self.CancelledError):
                return

    async def _run_handler(self, handler: HandlerType, job: Job) -> None:
        self._job_var.set(job)
        try:
            self._publish_status("busy", job)
            await handler(job)
        except Exception as e:
            self._send_reply(job, content=error_to_dict(e))
            self.log.exception("Exception in message handler:", exc_info=e)
        finally:
            self._publish_status("idle", job)

    @contextlib.contextmanager
    def _bind_socket(self, socket_id: SocketID, socket: zmq.Socket) -> Generator[None, Any, None]:
        """Bind a zmq.Socket storing a reference to the socket and the port
        details and closing the socket on leaving the context."""
        if socket_id in self._sockets:
            msg = f"{socket_id=} is already loaded"
            raise RuntimeError(msg)
        socket.linger = 500
        port_name = f"{socket_id}_port"
        if socket_id is not SocketID.iopub:
            # ref: https://github.com/ipython/ipykernel/issues/270
            socket.router_handover = 1
        port = bind_socket(socket=socket, transport=self.transport, ip=self.ip, port=getattr(self, port_name))  # pyright: ignore[reportArgumentType]
        setattr(self, port_name, port)
        self.log.debug("%s socket on port: %i", socket_id, port)
        self._sockets[socket_id] = socket
        try:
            yield
        finally:
            socket.close(linger=500)
            self._sockets.pop(socket_id)

    def iopub_send(
        self,
        msg_or_type: dict[str, Any] | str,
        content: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | None | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes] | None = None,
    ) -> None:
        """Send a message on the zmq iopub socket."""
        if socket := Caller.iopub_sockets.get(thread := threading.current_thread()):
            msg = self.session.send(
                stream=socket,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else self.job.get("msg"),  # pyright: ignore[reportArgumentType]
                ident=ident,
                buffers=buffers,
            )
            if msg:
                self.log.debug(
                    "iopub_send: (thread=%s) msg_type:'%s', content: %s", thread.name, msg["msg_type"], msg["content"]
                )
        else:
            self.control_thread_caller.call_no_context(
                self.iopub_send,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent if parent is not NoValue else None,
                ident=ident,
                buffers=buffers,
            )

    def _publish_status(self, status: Literal["busy", "idle"], job: Job) -> None:
        """send status (busy/idle) on IOPub"""
        self.iopub_send(
            msg_or_type="status",
            content={"execution_state": status},
            parent=job["msg"],  # type: ignore[call-arg]
            ident=self.topic("status"),
        )

    def _send_reply(self, job: Job, content: dict | None = None) -> None:
        """Send a reply to the job with the specified content."""
        content = content or {}
        if "status" not in content:
            content["status"] = "ok"
        msg = self.session.send(
            stream=job["socket"],
            msg_or_type=job["msg"]["header"]["msg_type"].replace("request", "reply"),
            content=content,
            parent=job["msg"]["header"],  # pyright: ignore[reportArgumentType]
            ident=job["ident"],
        )
        if msg:
            self.log.debug("*** _send_reply %s*** %s", job['socket_id'], msg)


    def _input_request(self, prompt: str, *, password=False) -> Any:
        job = self.job
        if not job["msg"].get("content", {}).get("allow_stdin", False):
            msg = "Stdin is not allowed in this context!"
            raise StdinNotImplementedError(msg)
        socket = self._sockets[SocketID.stdin]
        # Clear messages on the stdin socket
        while socket.get(SocketOption.EVENTS) & PollEvent.POLLIN:  # pyright: ignore[reportOperatorIssue]
            socket.recv_multipart(flags=Flag.DONTWAIT, copy=False)
        # Send the input request.
        assert self is not None
        self.session.send(
            stream=socket,
            msg_or_type="input_request",
            content={"prompt": prompt, "password": password},
            parent=job["msg"],  # pyright: ignore[reportArgumentType]
            ident=job["ident"],
        )
        # Poll for a reply.
        while not (socket.poll(100) & PollEvent.POLLIN):
            if self._last_interrupt_frame:
                raise KernelInterruptError
        return self.session.recv(socket)[1]["content"]["value"]  # pyright: ignore[reportOptionalSubscript]

    async def handle_message_request(self, job: Job, /) -> None:
        """The main handler for all shell and control messages.

        Args:
            job: The packed [message][async_kernel.typing.Message] for handling.
        """
        msg_type: MsgType = job["msg"]["header"]["msg_type"]
        if not (handler := self.message_handlers[job["socket_id"]].get(msg_type)):
            self.log.error("Unknown message type: %r", job["msg"]["header"])
            return
        func = wrap_handler(self._run_handler, handler)
        match self.get_run_mode(job):
            case RunMode.queue:
                await Caller().queue_call(func, job)
            case RunMode.thread:
                Caller.to_thread(func, job)
            case RunMode.task:
                Caller().call_soon(func, job)
            case RunMode.wait:
                await func(job)



    @staticmethod
    def get_run_mode(job: Job[Any]) -> RunMode:
        """Determine `RunMode` from the job."""
        if m := job.get("run_mode"):
            # Respect an existing mode
            return RunMode(m)
        msg_type = job["msg"]["header"]["msg_type"]
        mode = RunMode.wait
        content = job["msg"].get("content")
        if msg_type == MsgType.execute_request:
            if mode_ := CODE_MODE_MAPPINGS.get(content.get("code", "").strip().split("\n")[0].strip()):
                mode = mode_
            elif content.get("silent", True) or (job["socket_id"] is SocketID.control):
                mode = RunMode.task
            else:
                mode = RunMode.queue
        job["run_mode"] = mode
        return mode

    def topic(self, topic) -> bytes:
        """prefixed topic for IOPub messages"""
        return (f"kernel.{topic}").encode()

    async def kernel_info_request(self, job: Job) -> None:
        """Handle a kernel info request."""
        self._send_reply(job, self.kernel_info)

    async def comm_info_request(self, job: Job) -> None:
        """Handle a comm info request."""
        content = job["msg"]["content"]
        target_name = content.get("target_name", None)
        comms = {
            k: {"target_name": v.target_name}
            for (k, v) in tuple(self.comm_manager.comms.items())
            if v.target_name == target_name or target_name is None
        }
        self._send_reply(job, {"comms": comms})

    async def execute_request(self, job: Job[ExecuteContent]) -> None:
        """Process the execute request."""
        content = job["msg"]["content"]
        if (job["received_time"] < self._stop_on_error_time) and not content.get("silent", False):
            self.log.info("Aborting execute_request: %s", job)
            c_ = error_to_dict(RuntimeError("Aborting due to prior exception"))
            c_ |= {"execution_count": self.execution_count}
            self._publish_status("busy", job)
            self._send_reply(job, content=c_)
            self._publish_status("idle", job)
            return
        metadata = job["msg"].get("metadata") or {}
        if not (silent := content["silent"]):
            self._execution_count += 1
            self._execution_count_var.set(self._execution_count)
            self.shell.execute_request_timeout = metadata.get(MetadataKeys.timeout) or self.cell_execute_timeout
            self.iopub_send(
                msg_or_type="execute_input",
                content={"code": content["code"], "execution_count": self.execution_count},
                parent=job["msg"],
                ident=self.topic("execute_input"),
            )
        fut = (Caller.to_thread if self.get_run_mode(job) is RunMode.thread else Caller().call_soon)(
            self.shell.run_cell_async,
            raw_cell=content["code"],
            store_history=content.get("store_history", False),
            silent=silent,
            transformed_cell=self.shell.transform_cell(content["code"]),
            shell_futures=True,
            cell_id=metadata.get("cellId"),
        )
        if not silent:
            self._interrupts.add(fut.cancel)
            fut.add_done_callback(lambda fut: self._interrupts.discard(fut.cancel))
        result: ExecutionResult = await fut
        err = result.error_before_exec or result.error_in_exec if result else KernelInterruptError()
        if (err) and (
            (Tags.suppress_error in metadata.get("tags", ()))  # 1.
            or (isinstance(err, self.CancelledError) and (self.shell.execute_request_timeout is not None))  # 2.
        ):
            # Suppress the error due to either:
            # 1. tag
            # 2. timeout
            err = None
        reply_content = {
            "status": "error" if err else "ok",
            "execution_count": self.execution_count,
            "user_expressions": self.shell.user_expressions(content.get("user_expressions", {})),
        }
        if err:
            reply_content |= error_to_dict(error=err)
            if not silent and content.get("stop_on_error"):
                self._stop_on_error_time = time.monotonic()
                self.log.info("An error occurred in a non-silent execution request")
        self._send_reply(job, content=reply_content)

    async def interrupt_request(self, job: Job) -> None:
        """Handle an interrupt request."""
        self._interrupt_requested = True
        if sys.platform == "win32":
            signal.raise_signal(signal.SIGINT)
            time.sleep(0)
        else:
            os.kill(os.getpid(), signal.SIGINT)
        for interrupter in tuple(self._interrupts):
            interrupter()
        self._send_reply(job)

    async def complete_request(self, job: Job) -> None:
        """Handle a completion request."""
        parent = job["msg"]
        matches = await self.do_complete(parent["content"]["code"], parent["content"]["cursor_pos"])
        self._send_reply(job, matches)

    async def is_complete_request(self, job: Job) -> None:
        """Handle an is_complete request."""
        reply_content = await self.do_is_complete(job["msg"]["content"]["code"])
        self._send_reply(job, reply_content)

    async def inspect_request(self, job: Job) -> None:
        """Handle an inspect request."""
        content = job["msg"]["content"]
        reply_content = await self.do_inspect(
            content["code"],
            content["cursor_pos"],
            int(content.get("detail_level", 0)),
            set(content.get("omit_sections", [])),
        )
        self._send_reply(job, reply_content)

    async def history_request(self, job: Job) -> None:
        """Handle a history request."""
        reply_content = await self.do_history(**job["msg"]["content"])
        self._send_reply(job, reply_content)

    async def comm_open(self, job: Job) -> None:
        self.comm_manager.comm_open(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_msg(self, job: Job) -> None:
        self.comm_manager.comm_msg(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_close(self, job: Job) -> None:
        self.comm_manager.comm_close(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def control_shutdown_request(self, job: Job) -> None:
        """Handle a shutdown request."""
        await self.debugger.disconnect()
        self._send_reply(job, {"status": "ok", "restart": job["msg"]["content"].get("restart", False)})
        self.stop()

    async def debug_request(self, job: Job) -> None:
        """Handle a debug request."""
        content = await self.debugger.process_request(job["msg"]["content"])
        self._send_reply(job=job, content=content)

    async def do_complete(self, code, cursor_pos) -> dict[str, Any]:
        """Completions from IPython, using Jedi."""
        cursor_pos = cursor_pos if cursor_pos is not None else len(code)
        with _provisionalcompleter():
            completions = list(_rectify_completions(code, self.shell.Completer.completions(code, cursor_pos)))
        comps = [
            {
                "start": comp.start,
                "end": comp.end,
                "text": comp.text,
                "type": comp.type,
                "signature": comp.signature,
            }
            for comp in completions
        ]
        s, e = completions[0].start, completions[0].end if completions else (cursor_pos, cursor_pos)
        matches = [c.text for c in completions]
        return {
            "matches": matches,
            "cursor_end": e,
            "cursor_start": s,
            "metadata": {"_jupyter_types_experimental": comps},
        }

    async def do_is_complete(self, code) -> dict[str, Any]:
        """Handle an is_complete request."""
        status, indent_spaces = self.shell.input_transformer_manager.check_complete(code)
        r = {"status": status}
        if status == "incomplete":
            r["indent"] = " " * indent_spaces
        return r

    async def do_inspect(self, code, cursor_pos, detail_level=0, omit_sections=()) -> dict[str, Any]:
        """Handle code inspection."""
        name = token_at_cursor(code, cursor_pos)
        reply_content: dict[str, Any] = {"status": "ok"}
        reply_content["data"] = {}
        reply_content["metadata"] = {}
        try:
            bundle = self.shell.object_inspect_mime(name, detail_level=detail_level, omit_sections=omit_sections)
            reply_content["data"].update(bundle)
            if not self.shell.enable_html_pager:
                reply_content["data"].pop("text/html")
            reply_content["found"] = True
        except KeyError:
            reply_content["found"] = False
        return reply_content

    async def do_history(
        self,
        hist_access_type,
        output,
        raw,
        session=0,
        start=0,
        stop=None,
        n=None,
        pattern=None,
        unique=False,
    ) -> dict[str, list[Any]]:
        """Handle code history."""
        history_manager = self.shell.history_manager
        assert history_manager
        if hist_access_type == "tail":
            hist = history_manager.get_tail(n, raw=raw, output=output, include_latest=True)
        elif hist_access_type == "range":
            hist = history_manager.get_range(session, start, stop, raw=raw, output=output)
        elif hist_access_type == "search":
            hist = history_manager.search(pattern, raw=raw, output=output, n=n, unique=unique)
        else:
            hist = []
        return {"history": list(hist)}

    def excepthook(self, etype, evalue, tb) -> None:
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        traceback.print_exception(etype, evalue, tb, file=sys.__stderr__)

    def unraisablehook(self, unraisable: sys.UnraisableHookArgs, /) -> None:
        "Handle unraisable exceptions (during gc for instance)."
        exc_info = (
            unraisable.exc_type,
            unraisable.exc_value or unraisable.exc_type(unraisable.err_msg),
            unraisable.exc_traceback,
        )
        self.log.exception(unraisable.err_msg, exc_info=exc_info, extra={"object": unraisable.object})

    def raw_input(self, prompt="") -> Any:
        """Forward raw_input to frontends.

        Raises
        ------
        StdinNotImplementedError if active frontend doesn't support stdin.
        """
        return self._input_request(str(prompt), password=False)

    def getpass(self, prompt="") -> Any:
        """Forward getpass to frontends."""
        return self._input_request(prompt, password=True)
