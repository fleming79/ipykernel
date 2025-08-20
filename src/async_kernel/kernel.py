# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import asyncio
import atexit
import builtins
import contextlib
import errno
import functools
import getpass
import logging
import math
import os
import pathlib
import signal
import sys
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from contextlib import asynccontextmanager
from logging import Logger, LoggerAdapter
from pathlib import Path
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Literal, Self

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
    Content,
    ExecuteContent,
    HandlerType,
    Job,
    KernelConcurrencyMode,
    MsgType,
    NoValue,
    RunMode,
    SocketID,
    Tags,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator
    from types import CoroutineType, FrameType

    from anyio.abc import TaskStatus
    from IPython.core.interactiveshell import ExecutionResult

    from async_kernel.comm import CommManager


__all__ = ["Kernel", "KernelInterruptError"]


def error_to_content(error: BaseException, /) -> Content:
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
def _wrap_handler(
    runner: Callable[[HandlerType, Job]], handler: HandlerType
) -> Callable[[Job], CoroutineType[Any, Any, None]]:
    async def wrap_handler(job: Job) -> None:
        """A cache of run handlers.

        Args:
            runner: The function that calls and awaits the  handler

        Required by:

        - call[async_kernel.Caller.queue_call][] : The queue is on per-function (hash) basis.
        """
        await runner(handler, job)

    return wrap_handler


class KernelInterruptError(InterruptedError):
    "Raised to interrupt the kernel."

    # We subclass from InterruptedError so if the backend is a SelectorEventLoop it can catch the exception.
    # Other event loops don't appear to have this issue.


class Kernel(ConnectionFileMixin):
    """An asynchronous kernel with an anyio backend providing an IPython AsyncInteractiveShell with zmq sockets.

    Only one instance will be created/run at a time. The instance can be obtained with `Kernel()`.

    To start the kernel:


    === "Shell"

        At the command prompt.

        ``` shell
        async-kernel -f .
        ```

        See also:

        -

    === "Normal"
        ``` python
        from async_kernel.__main__ import main

        main()
        ```

    === "start (`classmethod`)"

        ``` python
        Kernel.start()
        ```

    === "Asynchronously inside anyio event loop"

        ``` python
        kernel = Kernel()
        async with kernel.start_in_context():
            await anyio.sleep_forever()
        ```
        ???+ tip

            This is a convenient way to start a kernel for debugging.

    """

    _instance: Self | None = None
    _initialised = False
    _interrupt_requested = False
    _last_interrupt_frame = None
    _stop_event = Instance(threading.Event, ())
    _stop_on_error_time: float = 0
    _interrupts: Container[set[Callable[[], object]]] = Set()
    _sockets: Dict[SocketID, zmq.Socket] = Dict()
    _execution_count = Int(0)
    anyio_backend = UseEnum(Backend)
    ""
    concurrency_mode = UseEnum(KernelConcurrencyMode)
    """The mode to use when getting the run mode for running the handler of a message request.
    
    See also:
        - [async_kernel.Kernel.handle_message_request][]
    """
    help_links = Tuple()
    ""
    quiet = CBool(True, help="Only send stdout/stderr to output stream")
    ""
    shell = Instance(AsyncInteractiveShell)
    ""
    session = Instance(Session)
    ""
    log = Instance(logging.LoggerAdapter)
    ""
    debugger = Instance(Debugger, ())
    ""
    comm_manager: Instance[CommManager] = Instance("async_kernel.comm.CommManager")
    ""
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
        if self._initialised:
            return  # Only initialize once
        self._initialised = True
        super().__init__(**kwargs)
        sys.excepthook = self.excepthook
        sys.unraisablehook = self.unraisablehook
        signal.signal(signal.SIGINT, self._signal_handler)
        if not os.environ.get("MPLBACKEND"):
            os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"

    @property
    def execution_count(self) -> int:
        "The execution count in context of the current coroutine, else the current value if there isn't one in context."
        return utils.get_execution_count()

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
        return AsyncInteractiveShell.instance(parent=self)

    @classmethod
    def stop(cls) -> None:
        """Stop the kernel.

        Once a kernel is stopped; that instance of the kernel cannot be restarted.
        Instead, a new kernel must be started.
        """
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
            AsyncInteractiveShell.clear_instance()
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

    def _input_request(self, prompt: str, *, password=False) -> Any:
        job = utils.get_job()
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
                                    run_mode=None,  #  pyright: ignore[reportArgumentType]. This value is set by `get_handler_and_run_mode`.
                                )
                            )
                        except Exception as e:
                            self.log.debug("Bad message on %s: %s", socket_id, e)
                            continue
                        await anyio.sleep(0)
                    await anyio.wait_readable(socket)
            except (zmq.ContextTerminated, self.CancelledError):
                return

    async def handle_message_request(self, job: Job, /) -> None:
        """The main handler for all shell and control messages.

        Args:
            job: The packed [message][async_kernel.typing.Message] for handling.
        """
        try:
            msg_type = MsgType(job["msg"]["header"]["msg_type"])
            socket_id = job["socket_id"]
            handler = self.get_handler(socket_id, msg_type)
        except (ValueError, TypeError):
            return
        run_mode = self.get_run_mode(socket_id, msg_type, job=job)
        self.log.debug("%s  %s run mode %s handler: %s", socket_id, msg_type, run_mode, handler)
        job["run_mode"] = run_mode
        runner = _wrap_handler(self.run_handler, handler)
        match run_mode:
            case RunMode.queue:
                await Caller().queue_call(runner, job)
            case RunMode.thread:
                Caller.to_thread(runner, job)
            case RunMode.task:
                Caller().call_soon(runner, job)
            case RunMode.blocking:
                await runner(job)

    def get_run_mode(
        self,
        socket_id: SocketID,
        msg_type: MsgType,
        *,
        concurrency_mode: KernelConcurrencyMode | NoValue = NoValue,  # pyright: ignore[reportInvalidTypeForm]
        job: Job | None = None,
    ) -> RunMode:
        """Determine the run mode for a given channel, message type and concurrency mode.

        The run mode determines how the kernel will execute the message.

        Args:
            socket_id: The socket ID the message was received on.
            msg_type: The type of the message.
            concurrency_mode: The concurrency mode of the kernel. Defaults to [kernel.concurrency_mode][async_kernel.Kernel.concurrency_mode]
            job: The job associated with the message, if any.

        Returns:
            The run mode for the message.

        Raises:
            ValueError: If a shutdown or debug request is received on the shell socket.
        """

        concurrency_mode = self.concurrency_mode if concurrency_mode is NoValue else concurrency_mode
        # TODO: Are any of these options worth including?
        # if mode_from_metadata := job["msg"]["metadata"].get("run_mode"):
        #     return RunMode( mode_from_metadata)
        # if mode_from_header := job["msg"]["header"].get("run_mode"):
        #     return RunMode( mode_from_header)
        match (concurrency_mode, socket_id, msg_type):
            case KernelConcurrencyMode.blocking, _, _:
                return RunMode.blocking
            case _, SocketID.control, MsgType.execute_request:
                return RunMode.task
            case _, _, MsgType.execute_request:
                if job:
                    if content := job["msg"].get("content", {}):
                        if (code := content.get("code")) and (mode_ := RunMode.get_mode(code)):
                            return mode_
                        if content.get("silent"):
                            return RunMode.task
                    if mode_ := set(utils.get_tags(job)).intersection(RunMode):
                        return RunMode(next(iter(mode_)))
                return RunMode.queue
            case _, SocketID.shell, MsgType.shutdown_request | MsgType.debug_request:
                msg = f"{msg_type=} not allowed on shell!"
                raise ValueError(msg)
            case _, _, MsgType.inspect_request | MsgType.complete_request | MsgType.is_complete_request:
                return RunMode.thread
            case _, _, MsgType.history_request:
                return RunMode.thread
            case _, _, MsgType.kernel_info_request | MsgType.comm_info_request | MsgType.comm_open | MsgType.comm_close:
                return RunMode.blocking
            case _:
                return RunMode.task

    def all_concurrency_run_modes(
        self,
    ) -> dict[
        Literal["SocketID", "KernelConcurrencyMode", "MsgType", "RunMode"],
        tuple[SocketID, KernelConcurrencyMode, MsgType, RunMode | None],
    ]:
        """Generates a dictionary containing all combinations of SocketID, KernelConcurrencyMode, and MsgType,
        along with their corresponding RunMode (if available)."""
        data: list[Any] = []
        for socket_id in [SocketID.shell, SocketID.control]:
            for concurrency_mode in KernelConcurrencyMode:
                for msg_type in MsgType:
                    try:
                        mode = self.get_run_mode(socket_id, msg_type, concurrency_mode=concurrency_mode)
                    except ValueError:
                        mode = None
                    data.append((socket_id, concurrency_mode, msg_type, mode))
        data_ = zip(*data, strict=True)
        return dict(zip(["SocketID", "KernelConcurrencyMode", "MsgType", "RunMode"], data_, strict=True))

    def get_handler(self, socket_id: SocketID, msg_type: MsgType) -> HandlerType:
        if not callable(f := getattr(self, msg_type, None)):
            msg = "A handler was not found for "
            raise TypeError(msg)
        return f  # pyright: ignore[reportReturnType]

    async def run_handler(self, handler: HandlerType, job: Job[dict]) -> None:
        """Runs the handler in the context of the job/message sending the reply content if it is provided.

        This method gets called for every valid request with the relevant handler.
        """

        def _send_reply(content: dict, /) -> None:
            """Send a reply to the job with the specified content."""
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
                self.log.debug("*** _send_reply %s*** %s", job["socket_id"], msg)

        utils._job_var.set(job)  # pyright: ignore[reportPrivateUsage]
        try:
            self.iopub_send(msg_or_type="status", content={"execution_state": "busy"}, ident=self.topic("status"))
            if (content := await handler(job)) is not None:
                _send_reply(content)
        except Exception as e:
            _send_reply(error_to_content(e))
            self.log.exception("Exception in message handler:", exc_info=e)
        finally:
            self.iopub_send(msg_or_type="status", content={"execution_state": "idle"}, ident=self.topic("status"))

    def iopub_send(
        self,
        msg_or_type: dict[str, Any] | str,
        content: Content | None = None,
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
                parent=parent if parent is not NoValue else utils.get_parent(),  # pyright: ignore[reportArgumentType]
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

    def topic(self, topic) -> bytes:
        """prefixed topic for IOPub messages"""
        return (f"kernel.{topic}").encode()

    async def kernel_info_request(self, job: Job[Content]) -> Content:
        """Handle a ke[rnel info request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-info)."""
        return self.kernel_info

    async def comm_info_request(self, job: Job[Content]) -> Content:
        """Handle a [comm info request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-info)."""
        c = job["msg"]["content"]
        target_name = c.get("target_name", None)
        comms = {
            k: {"target_name": v.target_name}
            for (k, v) in tuple(self.comm_manager.comms.items())
            if v.target_name == target_name or target_name is None
        }
        return {"comms": comms}

    async def execute_request(self, job: Job[ExecuteContent]) -> Content:
        """Handle a [execute request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute)."""
        c = job["msg"]["content"]
        if (
            job["run_mode"] is RunMode.queue
            and (job["received_time"] < self._stop_on_error_time)
            and not c.get("silent", False)
        ):
            self.log.info("Aborting execute_request: %s", job)
            return error_to_content(RuntimeError("Aborting due to prior exception")) | {
                "execution_count": self.execution_count
            }
        metadata = job["msg"].get("metadata") or {}
        if not (silent := c["silent"]):
            self._execution_count += 1
            utils._execution_count_var.set(self._execution_count)  # pyright: ignore[reportPrivateUsage]
            self.iopub_send(
                msg_or_type="execute_input",
                content={"code": c["code"], "execution_count": self.execution_count},
                parent=job["msg"],
                ident=self.topic("execute_input"),
            )
        fut = (Caller.to_thread if job["run_mode"] is RunMode.thread else Caller().call_soon)(
            self.shell.run_cell_async,
            raw_cell=c["code"],
            store_history=c.get("store_history", False),
            silent=silent,
            transformed_cell=self.shell.transform_cell(c["code"]),
            shell_futures=True,
            cell_id=metadata.get("cellId"),
        )
        if not silent:
            self._interrupts.add(fut.cancel)
            fut.add_done_callback(lambda fut: self._interrupts.discard(fut.cancel))
        try:
            result: ExecutionResult = await fut
            err = result.error_before_exec or result.error_in_exec if result else KernelInterruptError()
        except Exception as e:
            # A safeguard to catch exceptions not caught by the shell.
            err = e
        if (err) and (
            (Tags.suppress_error in metadata.get("tags", ()))  # 1.
            or (isinstance(err, self.CancelledError) and (utils.get_execute_request_timeout() is not None))  # 2.
        ):
            # Suppress the error due to either:
            # 1. tag
            # 2. timeout
            err = None
        content = {
            "status": "error" if err else "ok",
            "execution_count": self.execution_count,
            "user_expressions": self.shell.user_expressions(c.get("user_expressions", {})),
        }
        if err:
            content |= error_to_content(err)
            if (not silent) and c.get("stop_on_error"):
                try:
                    self._stop_on_error_time = math.inf
                    self.log.info("An error occurred in a non-silent execution request")
                    await anyio.sleep(0)
                finally:
                    self._stop_on_error_time = time.monotonic()
        return content

    async def complete_request(self, job: Job[Content]) -> Content:
        """Handle a [completion request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion)."""
        c = job["msg"]["content"]
        code: str = c["code"]
        cursor_pos = c.get("cursor_pos") or len(code)
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

    async def is_complete_request(self, job: Job[Content]) -> Content:
        """Handle a [is_complete request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness)."""
        status, indent_spaces = self.shell.input_transformer_manager.check_complete(job["msg"]["content"]["code"])
        content = {"status": status}
        if status == "incomplete":
            content["indent"] = " " * indent_spaces
        return content

    async def inspect_request(self, job: Job[Content]) -> Content:
        """Handle a [inspect request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection)."""
        c = job["msg"]["content"]
        detail_level = int(c.get("detail_level", 0))
        omit_sections = set(c.get("omit_sections", []))
        name = token_at_cursor(c["code"], c["cursor_pos"])
        content: dict[str, Any] = {"status": "ok"}
        content["data"] = {}
        content["metadata"] = {}
        try:
            bundle = self.shell.object_inspect_mime(name, detail_level=detail_level, omit_sections=omit_sections)
            content["data"].update(bundle)
            if not self.shell.enable_html_pager:
                content["data"].pop("text/html")
            content["found"] = True
        except KeyError:
            content["found"] = False
        return content

    async def history_request(self, job: Job[Content]) -> Content:
        """Handle a [history request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#history)."""
        c = job["msg"]["content"]
        history_manager = self.shell.history_manager
        assert history_manager
        if c.get("hist_access_type") == "tail":
            hist = history_manager.get_tail(c["n"], raw=c.get("raw"), output=c.get("output"), include_latest=True)
        elif c.get("hist_access_type") == "range":
            hist = history_manager.get_range(
                c.get("session"), c.get("start"), c.get("stop"), raw=c.get("raw"), output=c.get("output")
            )
        elif c.get("hist_access_type") == "search":
            hist = history_manager.search(
                c.get("pattern"), raw=c.get("raw"), output=c.get("output"), n=c.get("n"), unique=c.get("unique")
            )
        else:
            hist = []
        return {"history": list(hist)}

    async def comm_open(self, job: Job[Content]) -> None:
        """Handle a [comm open request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#opening-a-comm)."""
        self.comm_manager.comm_open(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_msg(self, job: Job[Content]) -> None:
        """Handle a [comm msg request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-messages)."""
        self.comm_manager.comm_msg(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def comm_close(self, job: Job[Content]) -> None:
        """Handle a [comm close request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#tearing-down-comms)."""
        self.comm_manager.comm_close(stream=job["socket"], ident=job["ident"], msg=job["msg"])  # pyright: ignore[reportArgumentType]

    async def interrupt_request(self, job: Job[Content]) -> Content:
        """Handle a [interrupt request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt) (control only)."""
        self._interrupt_requested = True
        if sys.platform == "win32":
            signal.raise_signal(signal.SIGINT)
            time.sleep(0)
        else:
            os.kill(os.getpid(), signal.SIGINT)
        for interrupter in tuple(self._interrupts):
            interrupter()
        return {}

    async def shutdown_request(self, job: Job[Content]) -> Content:
        """Handle a [shutdown request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-shutdown) (control only)."""
        await self.debugger.disconnect()
        Caller().call_no_context(self.stop)
        return {"status": "ok", "restart": job["msg"]["content"].get("restart", False)}

    async def debug_request(self, job: Job[Content]) -> Content:
        """Handle a [debug request](https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request) (control only)."""
        return await self.debugger.process_request(job["msg"]["content"])

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
