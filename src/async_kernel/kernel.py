# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import asyncio
import atexit
import builtins
import contextlib
import enum
import getpass
import logging
import sys
import threading
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NotRequired, Self, TypedDict

import anyio
import anyio.to_thread
import sniffio
import traitlets
import zmq
from IPython.core.completer import provisionalcompleter as _provisionalcompleter
from IPython.core.completer import rectify_completions as _rectify_completions
from IPython.core.error import StdinNotImplementedError
from IPython.utils.tokenutil import token_at_cursor
from jupyter_client.connect import ConnectionFileMixin
from jupyter_client.session import Session
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import Dict, Instance, default
from traitlets.utils.importstring import import_item
from zmq import Flag, PollEvent, SocketOption

from async_kernel import _version, utils
from async_kernel.asyncshell import AsyncInteractiveShell
from async_kernel.debugger import Debugger
from async_kernel.kernelspec import KernelName

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import CoroutineType

    from anyio.abc import TaskStatus
    from IPython.core.interactiveshell import ExecutionResult

    from async_kernel.comm import CommManager
    from async_kernel.iostream import OutStream


class MsgHeader(TypedDict):
    # https://jupyter-client.readthedocs.io/en/stable/messaging.html#message-header
    msg_id: str
    session: str
    username: str
    date: str
    msg_type: str
    version: str
    subshell_id: NotRequired[str | None]


class MsgType(TypedDict):
    header: MsgHeader
    parent_header: MsgHeader
    metadata: dict[str, Any]
    content: dict[str, dict]
    buffers: list[bytearray | bytes]


class MsgRequest(TypedDict):
    "A message associated with the socket and ident"

    socket_id: Literal[SocketID.control, SocketID.shell]
    socket: zmq.Socket
    ident: bytes | list[bytes]
    msg_type: str
    parent: dict


class SocketID(enum.StrEnum):
    heartbeat = "hb"
    shell = "shell"
    stdin = "stdin"
    control = "control"
    iopub = "iopub"


class Kernel(ConnectionFileMixin):
    """An async kernel with an anyio backend providing an IPython InteractiveShell with zmq.

    To start the kernel.

    Direct

    ``` python
    Kernel.start()
    ```

    Inside an already running asycio context.

    ``` python
    kernel = Kernel()
    async with kernel.start_in_context():
        await anyio.sleep_forever()
    ```

    """

    _instance: Self | None = None
    _io_modified = traitlets.Bool(False)
    _stopped = Instance(anyio.Event, ())
    _stop_event = Instance(threading.Event, ())
    _stop_on_error_time: float = 0
    _interrupt_events: traitlets.Container[set[threading.Event]] = traitlets.Set()
    _zmq_context = Instance(zmq.Context, ())
    _sockets: Dict[SocketID, zmq.Socket] = Dict()
    _shell_handlers = Dict()
    _control_handlers = Dict()
    _job: traitlets.Container[MsgRequest | dict] = Dict()  # type: ignore[assignment]
    _iopub_url = traitlets.Unicode("inproc://iopub")
    _iopub_sockets: traitlets.Dict[threading.Thread, zmq.Socket] = traitlets.Dict()
    debugger = Instance(Debugger, ())

    quiet = traitlets.Bool(True, help="Only send stdout/stderr to output stream").tag(config=True)
    outstream_class = traitlets.DottedObjectName(
        "async_kernel.iostream.OutStream",
        help="The importstring for the OutStream factory",
        allow_none=True,
    ).tag(
        config=True,
    )
    displayhook_class = traitlets.DottedObjectName(
        "async_kernel.displayhook.ZMQDisplayHook", help="The importstring for the DisplayHook factory"
    ).tag(config=True)

    session = Instance(Session)

    log = Instance(logging.LoggerAdapter)

    shell = Instance(AsyncInteractiveShell)
    shell_class = traitlets.Type(AsyncInteractiveShell)
    banner = traitlets.Unicode()
    help_links = traitlets.Dict()
    comm_manager: Instance[CommManager] = Instance("async_kernel.comm.CommManager")

    def __new__(cls, *, kernel_name=KernelName.asyncio, connection_file="", **kwargs) -> Self:  # noqa: ARG004
        #  There is only one instance.
        if not (instance := cls._instance):
            cls._instance = instance = super().__new__(cls)
        return instance

    def __init__(self, *, connection_file="", kernel_name=KernelName.asyncio, **kwargs):
        """Initialize the kernel."""
        if self._shell_handlers:
            return  # Only initialize once
        self.kernel_name = kernel_name
        self.connection_file = connection_file
        super().__init__(**kwargs)
        self._shell_handlers = {
            "kernel_info_request": self.kernel_info_request,
            "comm_info_request": self.comm_info_request,
            "execute_request": self.execute_request,
            "interrupt_request": self.interrupt_request,
            "complete_request": self.complete_request,
            "is_complete_request": self.is_complete_request,
            "inspect_request": self.inspect_request,
            "history_request": self.history_request,
            "comm_open": self.comm_open,
            "comm_msg": self.comm_msg,
            "comm_close": self.comm_close,
        }
        self._control_handlers = {
            "execute_request": self.control_execute_request,  # no task queue
            "shutdown_request": self.control_shutdown_request,
            "debug_request": self.debug_request,
        }
        sys.excepthook = self.excepthook

    @property
    def kernel_info(self):
        return {
            "protocol_version": _version.kernel_protocol_version,
            "implementation": _version.implementation,
            "implementation_version": _version.implementation_version,
            "language_info": _version.language_info,
            "banner": self.banner,
            "help_links": self.help_links,
            "debugger": not utils.LAUNCHED_BY_DEBUGPY,
        }

    @default("log")
    def _default_log(self):
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    @default("kernel_name")
    def _default_kernel_name(self):
        if sniffio.current_async_library() == "trio":
            return KernelName.trio
        if sys.version_info >= (3, 12) and asyncio.get_running_loop().get_task_factory() is asyncio.eager_task_factory:
            return KernelName.asyncio_eager
        return KernelName.asyncio

    @default("comm_manager")
    def _default_comm_manager(self):
        from async_kernel import comm  # noqa: PLC0415

        comm.set_comm()
        return comm.get_comm_manager()

    @default("session")
    def _default_session(self):
        return Session(parent=self)

    @default("shell")
    def _default_shell(self):
        return self.shell_class.instance(parent=self, kernel=self)

    async def _start_heartbeat(self, task_status: TaskStatus):
        # Reference: https://jupyter-client.readthedocs.io/en/stable/messaging.html#heartbeat-for-kernels

        def heartbeat():
            socket = self._zmq_context.socket(zmq.ROUTER)
            with self._bind_socket(SocketID.heartbeat, socket):
                ready_event.set()
                try:
                    zmq.proxy(socket, socket)
                except zmq.ContextTerminated:
                    return

        ready_event = threading.Event()
        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        utils.mark_thread_debugpy_ignore(heartbeat_thread, "heartbeat")
        heartbeat_thread.start()
        ready_event.wait(10)
        task_status.started()

    async def _start_stdin(self, task_status: TaskStatus):
        socket = self._zmq_context.socket(zmq.SocketType.ROUTER)
        socket.linger = 0
        with self._bind_socket(SocketID.stdin, socket):
            task_status.started()
            await anyio.sleep_forever()

    async def _receive_msg_loop(self, socket_id: Literal[SocketID.control, SocketID.shell], *, task_status: TaskStatus):
        """Receive messages from the socket, unpack them and pass them to be processed with process_message."""

        if (
            sys.platform == "win32"
            and sniffio.current_async_library() == "asyncio"
            and (policy := asyncio.get_event_loop_policy())
            and policy.__class__.__name__ == "WindowsProactorEventLoopPolicy"
        ):
            from anyio._core._asyncio_selector_thread import get_selector  # noqa: PLC0415

            selector = get_selector()
            utils.mark_thread_debugpy_ignore(selector._thread)

        process_message = self._process_control if socket_id is SocketID.control else self._process_shell
        socket = zmq.Socket(self._zmq_context, zmq.SocketType.ROUTER)
        with self.iopub_enabled_this_thread(slow_subscriber_sleep=0.0), self._bind_socket(socket_id, socket):
            async with utils.ThreadSafeCaller(log=self.log):
                try:
                    task_status.started()
                    while True:
                        while socket.get(SocketOption.EVENTS) & PollEvent.POLLIN:  # type: ignore[call-arg]
                            msg = socket.recv_multipart(flags=Flag.DONTWAIT, copy=False)
                            ident, msg_ = self.session.feed_identities(msg, copy=False)
                            parent = self.session.deserialize(msg_, content=True, copy=False)
                            msg_type = parent["header"]["msg_type"]
                            self.log.debug(
                                "*** _receive_msg_loop %s*** '%s' %s", socket_id, msg_type, parent["content"]
                            )
                            job = MsgRequest(
                                socket_id=socket_id,
                                socket=socket,
                                ident=ident,
                                parent=parent,
                                msg_type=msg_type,
                            )
                            await process_message(job)
                        await anyio.wait_readable(socket)
                except zmq.ContextTerminated:
                    return

    @contextlib.contextmanager
    def _bind_socket(self, socket_id: SocketID, socket: zmq.Socket):
        """Bind a zmq.Socket storing a reference to the socket and the port details.

        If `socket` is provided, a shadow copy of the socket is . The
        socket is closed once the context is left.
        """
        socket.linger = 500
        if socket_id in self._sockets:
            msg = f"{socket_id=} is already loaded"
            raise RuntimeError(msg)
        port_name = f"{socket_id}_port"
        port = utils.bind_socket(socket=socket, transport=self.transport, ip=self.ip, port=getattr(self, port_name))  # type: ignore[call-arg]
        setattr(self, port_name, port)
        self.log.debug("%s socket on port: %i", socket_id, port)
        self._sockets[socket_id] = socket
        try:
            yield
        finally:
            socket.close(linger=500)
            self._sockets.pop(socket_id)

    async def _start_iopub(self, task_status: TaskStatus):
        self._save_io()
        cls: type[OutStream] = import_item(self.outstream_class)
        if self.outstream_class:
            for name in ["stdout", "stderr"]:
                echo = getattr(sys, name)

                def flusher(string: str, name=name, echo=echo):
                    "Publish stdio or stderr when flush is called"
                    self.iopub_send(
                        msg_or_type="stream",
                        content={"name": name, "text": string},
                        ident=f"stream.{name}".encode(),
                    )
                    if not self.quiet and echo:
                        echo.write(string)
                        echo.flush()

                wrapper = cls(name=name, flusher=flusher)  # type: ignore[call-arg]
                setattr(sys, name, wrapper)
        task_status.started()
        self.comm_manager.kernel = self
        try:
            await anyio.sleep_forever()
        finally:
            self.comm_manager.kernel = None
            self._reset_io()

    async def _start_iopub_proxy(self, task_status: TaskStatus):
        """Provide an io proxy"""

        def pub_proxy():
            # We use an internal proxy to collect pub messages for distribution.
            # Each thread needs to open its own socket to publish to the internal proxy.
            # When thread-safe sockets become available, this could be changed...
            # which could come from any thread in this process.
            # Ref: https://zguide.zeromq.org/docs/chapter2/#Working-with-Messages (fig 14)
            frontend: zmq.Socket = self._zmq_context.socket(zmq.XSUB)
            frontend.bind(self._iopub_url)
            iopub_socket: zmq.Socket = self._zmq_context.socket(zmq.XPUB)
            with self._bind_socket(SocketID.iopub, iopub_socket):
                ready_event.set()
                try:
                    zmq.proxy(frontend, iopub_socket)
                except zmq.ContextTerminated:
                    frontend.close(linger=500)

        ready_event = threading.Event()
        iopub_thread = threading.Thread(target=pub_proxy, name="iopub proxy", daemon=True)
        utils.mark_thread_debugpy_ignore(iopub_thread, "iopub")
        iopub_thread.start()
        ready_event.wait(10)
        task_status.started()

    @contextlib.contextmanager
    def iopub_enabled_this_thread(self, *, slow_subscriber_sleep=0.4):
        """A contextmanager to provide a iopub socket on the current thread."""
        thread = threading.current_thread()
        if not (socket := self._iopub_sockets.get(thread)):
            self.log.info("Opening iopub socket for thread %s", thread.name)
            socket = self._zmq_context.socket(zmq.SocketType.PUB)
            socket.connect(self._iopub_url)
            self._iopub_sockets[thread] = socket
            if slow_subscriber_sleep:
                # https://pyzmq.readthedocs.io/en/latest/howto/logging.html#slow-joiner-problem
                # https://zguide.zeromq.org/docs/chapter5/#Slow-Subscriber-Detection-Suicidal-Snail-Pattern
                time.sleep(slow_subscriber_sleep)
            try:
                yield
            finally:
                socket.close(linger=500)
                self._iopub_sockets.pop(thread, None)
        else:
            yield

    def iopub_send(
        self,
        msg_or_type: dict[str, Any] | str,
        content: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        parent: dict[str, Any] | None = None,
        ident: bytes | list[bytes] | None = None,
        buffers: list[bytes] | None = None,
    ):
        """Send a message on the zmq iopub socket."""
        if socket := self._iopub_sockets.get(thread := threading.current_thread()):
            msg = self.session.send(
                stream=socket,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent if parent is not None else self._job.get("parent"),
                ident=ident,
                buffers=buffers,
            )
            if msg:
                self.log.debug(
                    "iopub_send: (thread=%s) msg_type:'%s', content: %s", thread.name, msg["msg_type"], msg["content"]
                )
        else:
            utils.ThreadSafeCaller.get_instance(self._control_thread).call_later(
                self.iopub_send,
                msg_or_type=msg_or_type,
                content=content,
                metadata=metadata,
                parent=parent,
                ident=ident,
                buffers=buffers,
            )
            self.log.debug(
                "Passing iopub to control thread from another thread (%s)."
                " Use in context of `iopub_enabled_this_thread` to provide a direct socket.",
                thread.name,
            )

    def _publish_status(self, status: Literal["busy", "idle"], job: MsgRequest):
        """send status (busy/idle) on IOPub"""
        self.iopub_send(
            msg_or_type="status",
            content={"execution_state": status},
            parent=job["parent"],
            ident=self._topic("status"),
        )

    def send_reply(self, job: MsgRequest, content: dict | None = None):
        content = content or {}
        if "status" not in content:
            content["status"] = "ok"
        msg = self.session.send(
            stream=job["socket"],
            msg_or_type=job["msg_type"].replace("request", "reply"),
            content=content,
            parent=job["parent"]["header"],
            ident=job["ident"],
        )
        if msg:
            self.log.debug("send_reply: '%s' %s", msg["msg_type"], msg["content"])

    def _send_error_reply(
        self, job: MsgRequest, *, ename="RuntimeError", evalue="", traceback: list[str] | None = None
    ):
        "Send a reply to the request"
        content = {
            "status": "error",
            "execution_count": self.shell.execution_count,
            "ename": ename,
            "evalue": evalue,
            "traceback": traceback or [],
        }
        self.send_reply(job, content)

    async def _start_control_loop(self, task_status: TaskStatus):
        def control():
            async def run_control_loop():
                # This code runs in a different thread having its own async event loop
                async with anyio.create_task_group() as tg:
                    await tg.start(self.debugger.start, self)
                    await tg.start(self._receive_msg_loop, SocketID.control)
                    ready_event.set()

            anyio.run(run_control_loop)

        ready_event = threading.Event()
        control_thread = threading.Thread(target=control, daemon=True)
        control_thread.start()
        ready_event.wait(10)
        control_thread.name = "Control"
        self._control_thread = control_thread
        task_status.started()

    async def _start_shell_loop(self, task_status: TaskStatus):
        async with anyio.create_task_group() as tg:
            await tg.start(self._shell_execute_request_loop)
            await tg.start(self._receive_msg_loop, SocketID.shell)
            task_status.started()

    async def _process_control(self, job: MsgRequest):
        # Inside control thread

        # Execute_requests
        msg_type = job["msg_type"]
        handler = self._control_handlers.get(msg_type) or self._shell_handlers.get(msg_type)
        if not handler:
            self.log.error("Unknown message type: %r", msg_type)
        else:
            if msg_type != "execute_request":
                self._publish_status("busy", job)
            try:
                await handler(job)
            except Exception as e:
                self._send_error_reply(job, ename=str(type(e).__name__), evalue=str(e))
                self.log.exception("Execute request exception for %s", job, exc_info=e)
            except KeyboardInterrupt:
                # Ctrl-c shouldn't crash the kernel here.
                self.log.error("KeyboardInterrupt caught in kernel.")
            finally:
                if msg_type != "execute_request":
                    self._publish_status("idle", job)

    async def _process_shell(self, job: MsgRequest):
        msg_type = job["msg_type"]
        if msg_type == "execute_request":
            await self.execute_request(job)
        else:
            handler = self._shell_handlers.get(msg_type)
            if handler is None:
                self.log.error("Unknown message type: %r", msg_type)
            else:
                try:
                    self._publish_status("busy", job)
                    await handler(job)
                except Exception as e:
                    self._send_error_reply(
                        job, ename=str(type(e).__name__), evalue=str(e), traceback=traceback.format_stack()
                    )
                    self.log.error("Exception in message handler:", exc_info=e)
                except KeyboardInterrupt:
                    # Ctrl-c shouldn't crash the self here.
                    self.log.error("KeyboardInterrupt caught in kernel.")
                finally:
                    self._publish_status("idle", job)

    async def _shell_execute_request_loop(self, *, task_status: TaskStatus):
        self._exec_send_stream, self._exec_receive_stream = anyio.create_memory_object_stream[tuple[float, MsgRequest]](
            max_buffer_size=1000
        )
        async with self._exec_receive_stream as receive_stream:
            task_status.started()
            async for received_time, job in receive_stream:
                try:
                    if received_time < self._stop_on_error_time:
                        self.log.info("Aborting execute_request: %s", job)
                        self._publish_status("busy", job)
                        self._send_error_reply(job, evalue="Aborting due to prior exception")
                        self._publish_status("idle", job)
                        continue
                    await self._execute_request(job)
                except BaseException as e:
                    self.log.exception("Execute request", exc_info=e)
                    self._send_error_reply(
                        job,
                        ename=str(type(e).__name__),
                        evalue=str(e),
                        traceback=traceback.format_stack(),
                    )

    async def _wrap_start_soon(self, func: Callable[..., CoroutineType], args: tuple):
        try:
            await func(*args)
        except Exception as e:
            self.log.exception("Coroutine execution failed", exc_info=e)

    def _topic(self, topic):
        """prefixed topic for IOPub messages"""
        return (f"kernel.{topic}").encode()

    def _input_request(self, prompt, *, password=False):
        # Flush output before making the request.
        if threading.current_thread() is not threading.main_thread():
            msg = "Input request is only allowed from the main thread (eg: not from the control thread)."
            raise RuntimeError(msg)
        # flush the stdin socket, to purge stale replies
        socket = self._sockets[SocketID.stdin]
        while True:
            try:
                socket.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    break
                raise
        # Send the input request.
        assert self is not None
        self.session.send(
            stream=socket,
            msg_or_type="input_request",
            content={"prompt": prompt, "password": password},
            parent=self._job["parent"],
            ident=self._job["ident"],
        )
        # Await a response.
        while True:
            try:
                # Use polling with select() so KeyboardInterrupts can get
                # through; doing a blocking recv() means stdin reads are
                # uninterruptible on Windows. We need a timeout because
                # zmq.select() is also uninterruptible, but at least this
                # way reads get noticed immediately and KeyboardInterrupts
                # get noticed fairly quickly by human response time standards.
                rlist, _, xlist = zmq.select([socket], [], [socket], 0.01)
                if rlist or xlist:
                    ident, reply = self.session.recv(socket)
                    if (ident, reply) != (None, None):
                        break
            except KeyboardInterrupt:
                # re-raise KeyboardInterrupt, to truncate traceback
                msg = "Interrupted by user"
                raise KeyboardInterrupt(msg) from None
            except Exception:
                self.log.warning("Invalid Message:", exc_info=True)
        try:
            value = reply["content"]["value"]  # type:ignore[index]
        except Exception:
            self.log.error("Bad input_reply: %s", self._job["parent"])
            value = ""
        if value == "\x04":
            # EOF
            raise EOFError
        return value

    def _forward_input(self, allow_stdin=False):
        """Forward raw_input and getpass to the current frontend.

        via input_request
        """
        self._allow_stdin = allow_stdin
        self._sys_raw_input = builtins.input
        builtins.input = self.raw_input
        self._save_getpass = getpass.getpass
        getpass.getpass = self.getpass

    def _restore_input(self):
        """Restore raw_input, getpass"""
        builtins.input = self._sys_raw_input
        getpass.getpass = self._save_getpass

    def _save_io(self):
        if not self._io_modified:
            self._original_io = sys.stdout, sys.stderr, sys.displayhook
            self._io_modified = True

    def _reset_io(self):
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

    async def kernel_info_request(self, job: MsgRequest):
        """Handle a kernel info request."""

        self.send_reply(job, self.kernel_info)

    async def comm_info_request(self, job: MsgRequest):
        """Handle a comm info request."""
        content = job["parent"]["content"]
        target_name = content.get("target_name", None)
        comms = {
            k: {"target_name": v.target_name}
            for (k, v) in tuple(self.comm_manager.comms.items())
            if v.target_name == target_name or target_name is None
        }
        self.send_reply(job, {"comms": comms})

    async def execute_request(self, job: MsgRequest):
        """Handle an execute_request.

        * *Non-silent*:
            - Added to a queue and executed sequntially in a separate task.
            - Respect an `interrupt_request`.
            - Stop on error results with all queued *non-silent* requests returning an error.
        * *Silent*:
            - Started in a separate task.
            - Ignore `interrupt_request`.
            - Stop on error is not relevant.
        """
        content = job["parent"]["content"]
        silent = content["silent"]
        if not silent:
            await self._exec_send_stream.send((time.monotonic(), job))
        else:
            utils.ThreadSafeCaller.get_instance().call_later(self._execute_request, 0, job)

    async def _execute_request(self, job: MsgRequest):
        """Perform the actual execute_request."""
        content = job["parent"]["content"]
        silent = content["silent"]
        stop_on_error = content.pop("stop_on_error", True)
        self._publish_status("busy", job)
        try:
            # Re-broadcast our input for the benefit of listening clients, and
            # start computing output
            if not silent:
                self._job = job
                self.iopub_send(
                    msg_or_type="execute_input",
                    content={"code": content["code"], "execution_count": self.shell.execution_count},
                    parent=job["parent"],
                    ident=self._topic("execute_input"),
                )
            # Call do_execute with the appropriate arguments
            reply_content = await self._do_execute(**content)
            if not silent and stop_on_error and reply_content.get("status") == "error":
                self._stop_on_error_time = time.monotonic()
                self.log.info("An error occurred in a non-silent execution request at %s", self._stop_on_error_time)
            # Send the reply.
            self.send_reply(job, reply_content)
        except Exception as e:
            self._send_error_reply(job, ename=e.__class__.__name__, evalue=str(e))
            self.log.exception("Execute request exception for %s", job, exc_info=e)
        finally:
            self._publish_status("idle", job)

    async def interrupt_request(self, job: MsgRequest):
        """Handle an interrupt request."""
        for event in self._interrupt_events:
            event.set()
        self.send_reply(job)

    async def complete_request(self, job: MsgRequest):
        """Handle a completion request."""
        parent = job["parent"]
        matches = await self.do_complete(parent["content"]["code"], parent["content"]["cursor_pos"])
        self.send_reply(job, matches)

    async def is_complete_request(self, job: MsgRequest):
        """Handle an is_complete request."""
        reply_content = await self.do_is_complete(job["parent"]["content"]["code"])
        self.send_reply(job, reply_content)

    async def inspect_request(self, job: MsgRequest):
        """Handle an inspect request."""
        content = job["parent"]["content"]
        reply_content = await self.do_inspect(
            content["code"],
            content["cursor_pos"],
            content.get("detail_level", 0),
            set(content.get("omit_sections", [])),
        )
        self.send_reply(job, reply_content)

    async def history_request(self, job: MsgRequest):
        """Handle a history request."""
        reply_content = await self.do_history(**job["parent"]["content"])
        self.send_reply(job, reply_content)

    async def comm_open(self, job: MsgRequest):
        self.comm_manager.comm_open(job["socket"], job["ident"], job["parent"])  # type: ignore[call-arg]

    async def comm_msg(self, job: MsgRequest):
        self.comm_manager.comm_msg(job["socket"], job["ident"], job["parent"])  # type: ignore[call-arg]

    async def comm_close(self, job: MsgRequest):
        self.comm_manager.comm_close(job["socket"], job["ident"], job["parent"])  # type: ignore[call-arg]

    async def control_execute_request(self, job: MsgRequest):
        content = job["parent"]["content"]
        # Override setting
        content["silent"] = True
        content["allow_stdin"] = False
        utils.ThreadSafeCaller.get_instance().call_later(self._execute_request, 0, job)

    async def control_shutdown_request(self, job: MsgRequest):
        """Handle a shutdown request."""
        reply_content = await self.do_shutdown(job["parent"]["content"]["restart"])
        self.send_reply(job, reply_content)

    async def debug_request(self, job: MsgRequest):
        """Handle a debug request."""
        content = await self.debugger.process_request(job["parent"]["content"])
        self.send_reply(job=job, content=content)

    async def _do_execute(
        self,
        code: str,
        silent: bool,
        store_history=True,
        user_expressions: dict | None = None,
        allow_stdin=False,
    ):
        """Handle code execution."""
        # ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute
        if not (shell := self.shell):
            msg = "shell is missing!"
            raise RuntimeError(msg)
        reply_content: dict[str, Any] = {}
        result: list[ExecutionResult] = []
        interrupt = threading.Event()
        if not silent:
            self._interrupt_events.add(interrupt)
            self._forward_input(allow_stdin)
        try:

            async def run() -> None:
                result_ = await shell.run_cell_async(
                    raw_cell=code,
                    store_history=store_history,
                    silent=silent,
                    transformed_cell=shell.transform_cell(code),
                )
                result.append(result_)
                interrupt.set()

            async with anyio.create_task_group() as tg:
                tg.start_soon(run)
                await anyio.to_thread.run_sync(interrupt.wait)
                if not result:
                    tg.cancel_scope.cancel()
        finally:
            self._interrupt_events.discard(interrupt)
            if not silent:
                self._restore_input()
        reply_content = {}
        if result and (res := result[0]):
            err = res.error_before_exec if res.error_before_exec is not None else res.error_in_exec
        else:
            err = KeyboardInterrupt("Interrupted by client request")
        if not err:
            reply_content["status"] = "ok"
        else:
            if traceback := shell._last_traceback:
                self.log.info("Exception in execute request")
            reply_content["status"] = "error"
            reply_content["traceback"] = traceback or []
            reply_content["ename"] = str(type(err).__name__)
            reply_content["evalue"] = str(err)
        reply_content["execution_count"] = shell.execution_count - 1
        reply_content["user_expressions"] = (
            shell.user_expressions(user_expressions) if not err and user_expressions else {}
        )
        return reply_content

    async def do_complete(self, code, cursor_pos):
        """Completions from IPython, using Jedi."""
        if cursor_pos is None:
            cursor_pos = len(code)
        with _provisionalcompleter():
            raw_completions = self.shell.Completer.completions(code, cursor_pos)
            completions = list(_rectify_completions(code, raw_completions))
            comps = []
            for comp in completions:
                comps.append({
                    "start": comp.start,
                    "end": comp.end,
                    "text": comp.text,
                    "type": comp.type,
                    "signature": comp.signature,
                })
        if completions:
            s = completions[0].start
            e = completions[0].end
            matches = [c.text for c in completions]
        else:
            s = cursor_pos
            e = cursor_pos
            matches = []
        return {
            "matches": matches,
            "cursor_end": e,
            "cursor_start": s,
            "metadata": {"_jupyter_types_experimental": comps},
            "status": "ok",
        }

    async def do_is_complete(self, code):
        """Handle an is_complete request."""
        status, indent_spaces = self.shell.input_transformer_manager.check_complete(code)
        r = {"status": status}
        if status == "incomplete":
            r["indent"] = " " * indent_spaces
        return r

    async def do_inspect(self, code, cursor_pos, detail_level=0, omit_sections=()):
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
    ):
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
        return {
            "status": "ok",
            "history": list(hist),
        }

    async def do_shutdown(self, restart):
        """Handle kernel shutdown."""
        self.stop()
        await self._stopped.wait()
        return {"status": "ok", "restart": restart}

    def do_clear(self):
        """Clear the kernel."""
        self.shell.reset(False)
        return {"status": "ok"}

    def excepthook(self, etype, evalue, tb):
        """Handle an exception."""
        # write uncaught traceback to 'real' stderr, not zmq-forwarder
        traceback.print_exception(etype, evalue, tb, file=sys.__stderr__)

    def raw_input(self, prompt=""):
        """Forward raw_input to frontends.

        Raises
        ------
        StdinNotImplementedError if active frontend doesn't support stdin.
        """
        if not self._allow_stdin:
            raise StdinNotImplementedError
        return self._input_request(str(prompt), password=False)

    def getpass(self, prompt=""):
        """Forward getpass to frontends."""
        if not self._allow_stdin:
            raise StdinNotImplementedError
        return self._input_request(prompt, password=True)

    @asynccontextmanager
    async def start_in_context(self):
        """Start the Kernel in an already running anyio event loop."""
        if self._sockets:
            msg = "Already started"
            raise RuntimeError(msg)
        if self.kernel_name is KernelName.asyncio_eager and sys.version_info >= (3, 12):
            loop = asyncio.get_running_loop()
            loop.set_task_factory(asyncio.eager_task_factory)
        if self.connection_file and Path(self.connection_file).exists():
            self.load_connection_file()
        try:
            async with anyio.create_task_group() as tg:
                try:
                    await tg.start(self._start_heartbeat)
                    await tg.start(self._start_stdin)
                    await tg.start(self._start_iopub_proxy)
                    await tg.start(self._start_control_loop)
                    await tg.start(self._start_shell_loop)
                    assert len(self._sockets) == len(SocketID)
                    time.sleep(0.5)  # sleep to give internal iopub sockets time to connect.
                    if not self.connection_file:
                        self.connection_file = str(Path(jupyter_runtime_dir()).joinpath(f"kernel-{uuid.uuid4()}.json"))
                    self.write_connection_file()
                    print(
                        f'To connect a client to this kernel, use:\n      --existing "{self.connection_file}"',
                    )
                    atexit.register(self.cleanup_connection_file)
                    await tg.start(self._start_iopub)
                    await tg.start(self._wait_stopped, tg)
                    yield self
                finally:
                    self.stop()
        finally:
            self._zmq_context.term()

    async def _wait_stopped(self, tg, *, task_status: TaskStatus):
        def wait_stopped():
            thread = threading.current_thread()
            utils.mark_thread_debugpy_ignore(thread)
            self._stop_event.wait()
            utils.mark_thread_debugpy_ignore(thread, unhide=True)

        task_status.started()
        await anyio.to_thread.run_sync(wait_stopped)
        tg.cancel_scope.cancel()

    @classmethod
    def start(cls, connection_file="", kernel_name=KernelName.asyncio) -> int:
        """Start the kernel."""

        async def _start() -> None:
            async with kernel.start_in_context():
                await anyio.sleep_forever()

        kernel = cls(kernel_name=kernel_name, connection_file=connection_file)
        try:
            anyio.run(_start, backend="trio" if kernel.kernel_name is KernelName.trio else "asyncio")
        finally:
            pass
        return 0

    @classmethod
    def stop(cls):
        """Stop the kernel."""
        if instance := cls._instance:
            cls._instance = None
            instance._stop_event.set()
