"""The IPythonAKernel kernel implementation"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import asyncio
import builtins
import contextlib
import gc
import getpass
import inspect
import logging
import math
import os
import queue
import sys
import threading
import time
import typing as t
import uuid
import warnings
from dataclasses import dataclass
from functools import partial
from signal import SIGINT, SIGTERM, Signals
from typing import TYPE_CHECKING

import anyio
import psutil
import zmq
import zmq_anyio
from anyio import Event, create_memory_object_stream, create_task_group, sleep, to_thread
from anyio.abc import TaskGroup
from anyio.from_thread import BlockingPortal
from IPython.core import release
from IPython.core.error import StdinNotImplementedError
from IPython.utils.tokenutil import token_at_cursor
from jupyter_client.session import Session
from traitlets import Any, Bool, Dict, Float, Instance, List, Type, Unicode, default, observe
from traitlets.config.configurable import SingletonConfigurable

from ipykernel._version import kernel_protocol_version
from ipykernel.compiler import XCachingCompiler
from ipykernel.iostream import OutStream
from ipykernel.zmqshell import ZMQInteractiveShell

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

    from ipykernel.comm import CommManager
    from ipykernel.debugger import Debugger

try:
    from IPython.core.completer import provisionalcompleter as _provisionalcompleter
    from IPython.core.completer import rectify_completions as _rectify_completions

    _use_experimental_60_completion = True
except ImportError:
    _use_experimental_60_completion = False

if sys.platform != "win32":
    from signal import SIGKILL
else:
    SIGKILL = "windown-SIGKILL-sentinel"


_AWAITABLE_MESSAGE: str = (
    "For consistency across implementations, it is recommended that `{func_name}`"
    " either be a coroutine function (`async def`) or return an awaitable object"
    " (like an `asyncio.Future`). It might become a requirement in the future."
    " Coroutine functions and awaitables have been supported since"
    " ipykernel 6.0 (2021). {target} does not seem to return an awaitable"
)


class IPythonAKernel(SingletonConfigurable):
    """The base kernel class."""

    _stop_on_error_time: float = time.monotonic()

    # ---------------------------------------------------------------------------
    # IPythonAKernel interface
    # ---------------------------------------------------------------------------

    # attribute to override with a GUI
    eventloop = Any(None)

    processes: dict[str, psutil.Process] = {}

    session: Instance[Session] = Instance(Session)
    profile_dir = Instance("IPython.core.profiledir.ProfileDir", allow_none=True)
    shell_socket: Instance[zmq_anyio.Socket] = Instance(zmq_anyio.Socket)

    implementation: str
    implementation_version: str

    _is_test = Bool(False)

    control_socket = Instance(zmq_anyio.Socket)
    control_tasks: t.Any = List()

    debug_shell_socket = Any()

    control_thread = Any()
    shell_channel_thread = Any()
    iopub_socket = Any()
    iopub_thread = Any()
    stdin_socket = Any()

    _send_exec_request: Dict[zmq_anyio.Socket, MemoryObjectSendStream] = Dict()
    _main_subshell_ready = Instance(Event, ())
    asyncio_event_loop = Instance(asyncio.AbstractEventLoop, allow_none=True, read_only=True)  # type:ignore[call-overload]
    _tg_main = Instance(TaskGroup)
    _portal = Instance(BlockingPortal)

    log: logging.Logger = Instance(logging.Logger, allow_none=True)  # type:ignore[assignment]

    ident = Unicode()

    @default("ident")
    def _default_ident(self):
        return str(uuid.uuid4())

    language_info = {
        "name": "python",
        "version": sys.version.split()[0],
        "mimetype": "text/x-python",
        "codemirror_mode": {"name": "ipython", "version": sys.version_info[0]},
        "pygments_lexer": "ipython%d" % 3,
        "nbconvert_exporter": "python",
        "file_extension": ".py",
    }

    # any links that should go in the help menu
    help_links = List([
        {
            "text": "Python Reference",
            "url": "https://docs.python.org/%i.%i" % sys.version_info[:2],
        },
        {
            "text": "IPython Reference",
            "url": "https://ipython.org/documentation.html",
        },
        {
            "text": "NumPy Reference",
            "url": "https://docs.scipy.org/doc/numpy/reference/",
        },
        {
            "text": "SciPy Reference",
            "url": "https://docs.scipy.org/doc/scipy/reference/",
        },
        {
            "text": "Matplotlib Reference",
            "url": "https://matplotlib.org/contents.html",
        },
        {
            "text": "SymPy Reference",
            "url": "http://docs.sympy.org/latest/index.html",
        },
        {
            "text": "pandas Reference",
            "url": "https://pandas.pydata.org/pandas-docs/stable/",
        },
    ]).tag(config=True)

    # Experimental option to break in non-user code.
    # The ipykernel source is in the call stack, so the user
    # has to manipulate the step-over and step-into in a wize way.
    debug_just_my_code = Bool(
        True,
        help="""Set to False if you want to debug python standard and dependent libraries.
        """,
    ).tag(config=True)

    # track associations with current request
    _allow_stdin = Bool(False)

    # Time to sleep after flushing the stdout/err buffers in each execute
    # cycle.  While this introduces a hard limit on the minimal latency of the
    # execute cycle, it helps prevent output synchronization problems for
    # clients.
    # Units are in seconds.  The minimum zmq latency on local host is probably
    # ~150 microseconds, set this to 500us for now.  We may need to increase it
    # a little if it's not enough after more interactive testing.
    _execute_sleep = Float(0.0005).tag(config=True)

    # Frequency of the kernel's event loop.
    # Units are in seconds, kernel subclasses for GUI toolkits may need to
    # adapt to milliseconds.
    _poll_interval = Float(0.01).tag(config=True)

    stop_on_error_timeout = Float(
        0.0,
        config=True,
        help="""time (in seconds) to wait for messages to arrive
        when aborting queued requests after an error.

        Requests that arrive within this window after an error
        will be cancelled.

        Increase in the event of unusually slow network
        causing significant delays,
        which can manifest as e.g. "Run all" in a notebook
        aborting some, but not all, messages after an error.
        """,
    )

    # If the shutdown was requested over the network, we leave here the
    # necessary reply message so it can be sent by our registered atexit
    # handler.  This ensures that the reply is only sent to clients truly at
    # the end of our shutdown process (which happens after the underlying
    # IPython shell's own shutdown).
    _shutdown_message = None

    # This is a dict of port number that the kernel is listening on. It is set
    # by record_ports and used by connect_request.
    _recorded_ports = Dict()

    shell = Instance(ZMQInteractiveShell)
    shell_class = Type(ZMQInteractiveShell)

    # use fully-qualified name to ensure lazy import and prevent the issue from
    # https://github.com/ipython/ipykernel/issues/1198
    debugger_class = Type("ipykernel.debugger.Debugger")

    compiler_class = Type(XCachingCompiler)

    debugpy_socket = Instance(zmq_anyio.Socket)

    user_module = Any()

    @observe("user_module")
    def _user_module_changed(self, change):
        if self.shell is not None:
            self.shell.user_module = change["new"]

    user_ns = Dict()

    @observe("user_ns")
    def _user_ns_changed(self, change):
        if self.trait_has_value("shell"):
            self.shell.user_ns = change["new"]
            self.shell.init_user_ns()
            self.shell.set_completer_frame()

    comm_manager: Instance[CommManager] = Instance("ipykernel.comm.CommManager")

    @default("comm_manager")
    def _default_comm_manager(self):
        import ipykernel.comm

        ipykernel.comm.set_comm()
        return ipykernel.comm.get_comm_manager()

    # IPythonAKernel info fields
    implementation = "ipython"
    implementation_version = release.version

    # A reference to the Python builtin 'raw_input' function.
    # (i.e., __builtin__.raw_input for Python 2.7, builtins.input for Python 3)
    _sys_raw_input = Any()
    _sys_eval_input = Any()

    msg_types = [
        "execute_request",
        "complete_request",
        "inspect_request",
        "history_request",
        "comm_info_request",
        "kernel_info_request",
        "connect_request",
        "is_complete_request",
        "interrupt_request",
    ]

    # control channel accepts all shell messages
    # and some of its own
    control_msg_types = [
        *msg_types,
        "shutdown_request",
        "debug_request",
        "create_subshell_request",
        "delete_subshell_request",
        "list_subshell_request",
    ]

    def __init__(self, **kwargs):
        """Initialize the kernel."""
        super().__init__(**kwargs)

        # IPythonAKernel application may swap stdout and stderr to OutStream,
        # which is the case in `IPKernelApp.init_io`, hence `sys.stdout`
        # can already by different from TextIO at initialization time.
        self._stdout: OutStream | t.TextIO = sys.stdout
        self._stderr: OutStream | t.TextIO = sys.stderr

        # Build dict of handlers for message types
        self.shell_handlers = {}
        for msg_type in self.msg_types:
            self.shell_handlers[msg_type] = getattr(self, msg_type)

        self.control_handlers = {}
        for msg_type in self.control_msg_types:
            self.control_handlers[msg_type] = getattr(self, msg_type)

        from ipykernel.debugger import _is_debugpy_available

        # Initialize the Debugger
        if _is_debugpy_available:
            self.debugger: Debugger = self.debugger_class(
                self.log,
                self.debugpy_socket,
                self._publish_debug_event,
                self.debug_shell_socket,
                self.session,
                self.debug_just_my_code,
            )
            self.control_tasks.append(self.process_debugpy)

        # Initialize the InteractiveShell subclass
        self.set_trait(
            "shell",
            self.shell_class.instance(
                parent=self,
                profile_dir=self.profile_dir,
                user_module=self.user_module,
                user_ns=self.user_ns,
                kernel=self,
                compiler_class=self.compiler_class,
            ),
        )
        self.shell.displayhook.session = self.session

        jupyter_session_name = os.environ.get("JPY_SESSION_NAME")
        if jupyter_session_name:
            self.shell.user_ns["__session__"] = jupyter_session_name

        self.shell.displayhook.pub_socket = self.iopub_socket
        self.shell.displayhook.topic = self._topic("execute_result")
        self.shell.display_pub.session = self.session
        self.shell.display_pub.pub_socket = self.iopub_socket
        self.shell.configurables.append(self.comm_manager)  # type:ignore[arg-type]
        for msg_type in ["comm_open", "comm_msg", "comm_close"]:
            self.shell_handlers[msg_type] = getattr(self.comm_manager, msg_type)
        self._new_threads_parent_header = {}
        self._initialize_thread_hooks()

        if hasattr(gc, "callbacks"):
            # while `gc.callbacks` exists since Python 3.3, pypy does not
            # implement it even as of 3.9.
            gc.callbacks.append(self._clean_thread_parent_frames)

        self.comm_manager.kernel = self

    async def process_control(self):
        try:
            while True:
                await self.process_control_message()
        except BaseException:
            if self.control_stop.is_set():
                return
            raise

    async def process_control_message(self, msg=None):
        """dispatch control requests"""
        assert self.control_socket is not None
        assert self.session is not None
        assert self.control_thread is None or threading.current_thread() == self.control_thread

        msg = msg or await self.control_socket.arecv_multipart().wait()
        idents, msg = self.session.feed_identities(msg)
        try:
            msg = self.session.deserialize(msg, content=True)
        except Exception:
            self.log.error("Invalid Control Message", exc_info=True)  # noqa: G201
            return

        self.log.debug("Control received: %s", msg)
        self._publish_status("busy", msg)
        header = msg["header"]
        msg_type = header["msg_type"]

        handler = self.control_handlers.get(msg_type, None)
        if handler is None:
            self.log.error("UNKNOWN CONTROL MESSAGE TYPE: %r", msg_type)
        else:
            try:
                result = handler(self.control_socket, idents, msg)
                if inspect.isawaitable(result):
                    await result
                else:
                    # If the handler is not awaitable, ensure it completes before proceeding
                    time.sleep(0.00001)  # Small delay to ensure sequential processing
            except Exception:
                self.log.error("Exception in control handler:", exc_info=True)  # noqa: G201

        if sys.stdout is not None:
            sys.stdout.flush()
        if sys.stderr is not None:
            sys.stderr.flush()
        self._publish_status("idle", msg)

    def should_handle(self, stream, msg, idents):
        """Check whether a shell-channel message should be handled

        Allows subclasses to prevent handling of certain messages (e.g. aborted requests).

        .. versionchanged:: 7
            Subclass should_handle _may_ be async.
            Base class implementation is not async.
        """
        return True

    async def shell_channel_thread_main(self):
        """Main loop for shell channel thread.

        Listen for incoming messages on kernel shell_socket.  For each message
        received, extract the subshell_id from the message header and forward the
        message to the correct subshell via ZMQ inproc pair socket.
        """
        assert self.shell_socket is not None
        assert self.session is not None
        assert self.shell_channel_thread is not None
        assert threading.current_thread() == self.shell_channel_thread

        async with self.shell_socket, create_task_group() as tg:
            try:
                while True:
                    msg = await self.shell_socket.arecv_multipart(copy=False).wait()
                    # deserialize only the header to get subshell_id
                    # Keep original message to send to subshell_id unmodified.
                    _, msg2 = self.session.feed_identities(msg, copy=False)
                    try:
                        msg3 = self.session.deserialize(msg2, content=False, copy=False)
                        subshell_id = msg3["header"].get("subshell_id")

                        # Find inproc pair socket to use to send message to correct subshell.
                        socket = self.shell_channel_thread.manager.get_shell_channel_socket(subshell_id)
                        assert socket is not None
                        if not socket.started.is_set():
                            await tg.start(socket.start)
                        await socket.asend_multipart(msg, copy=False).wait()
                    except Exception:
                        self.log.error("Invalid message", exc_info=True)  # noqa: G201
            except BaseException:
                if self.shell_stop.is_set():
                    return
                raise

    async def shell_main(self, subshell_id: str | None):
        """Main loop for a single subshell."""
        if self._supports_kernel_subshells:
            if subshell_id is None:
                assert threading.current_thread() == threading.main_thread()
            else:
                assert threading.current_thread() not in (
                    self.shell_channel_thread,
                    threading.main_thread(),
                )
            # Inproc pair socket that this subshell uses to talk to shell channel thread.
            socket = self.shell_channel_thread.manager.get_other_socket(subshell_id)
        else:
            assert subshell_id is None
            assert threading.current_thread() == threading.main_thread()
            socket = None
        socket = socket or self.shell_socket
        if socket not in self._send_exec_request:
            send_stream, receive_stream = create_memory_object_stream(max_buffer_size=math.inf)
            self._send_exec_request[socket] = send_stream
            try:
                async with create_task_group() as tg:
                    if not socket.started.is_set():
                        await tg.start(socket.start)
                    tg.start_soon(self._process_shell, socket)
                    tg.start_soon(self._execute_request_loop, receive_stream)
                    if not subshell_id:
                        # Main subshell
                        with contextlib.suppress(RuntimeError):
                            self.set_trait("asyncio_event_loop", asyncio.get_running_loop())
                        async with create_task_group() as tg_main:
                            tg_main.cancel_scope.shield = True
                            self._tg_main = tg_main
                            async with BlockingPortal() as portal:
                                # Provide a portal for general threadsafe access
                                self._portal = portal
                                self._main_subshell_ready.set()
                                await to_thread.run_sync(self.shell_stop.wait)
                                await portal.stop(True)
                            tg_main.cancel_scope.cancel()
                        tg.cancel_scope.cancel()
            except BaseException:
                if not self.shell_stop.is_set():
                    raise
            finally:
                # IPythonAKernel shutdown
                await self._at_shutdown()
                self._send_exec_request.pop(socket, None)
                await send_stream.aclose()
                await receive_stream.aclose()

    async def _execute_request_loop(self, receive_stream: MemoryObjectReceiveStream):
        async with receive_stream:
            async for received_time, socket, idents, msg in receive_stream:
                try:
                    if received_time < self._stop_on_error_time:
                        self.log.info("Aborting execute_request: %s", msg["header"]["msg_id"])
                        if session := self.session:
                            session.send(
                                stream=socket,
                                msg_or_type="execute_reply",
                                content={
                                    "status": "error",
                                    "execution_count": self.execution_count,
                                    "ename": "RuntimeError",
                                    "evalue": "An exception occurred whilst this execute request was queued!",
                                    "traceback": [],
                                },
                                parent=msg,
                                ident=idents,
                            )
                        continue
                    await self.execute_request(socket, idents, msg)
                except BaseException as e:
                    self.log.exception("Execute request", exc_info=e)
                finally:
                    self._publish_status("idle", msg)

    async def _process_shell(self, socket):
        # socket=None is valid if kernel subshells are not supported.
        await self._main_subshell_ready.wait()
        try:
            while True:
                await self.process_shell_message(socket=socket)
        except BaseException:
            if self.shell_stop.is_set():
                return
            raise

    async def process_shell_message(self, msg=None, socket=None):
        # If socket is None kernel subshells are not supported so use socket=shell_socket.
        # If msg is set, process that message.
        # If msg is None, await the next message to arrive on the socket.
        assert self.session is not None
        socket = socket or self.shell_socket
        if self._supports_kernel_subshells:
            assert threading.current_thread() not in (
                self.control_thread,
                self.shell_channel_thread,
            )
            assert socket is not None

        msg = msg or await socket.arecv_multipart(copy=False).wait()

        copy = not isinstance(msg[0], zmq.Message)
        idents, msg = self.session.feed_identities(msg, copy=copy)
        try:
            msg = self.session.deserialize(msg, content=True, copy=copy)
        except BaseException:
            self.log.error("Invalid Message", exc_info=True)  # noqa: G201
            return

        msg_type = msg["header"]["msg_type"]
        if msg_type != "execute_request":
            self._publish_status("busy", msg)

        # Print some info about this message and leave a '--->' marker, so it's
        # easier to trace visually the message chain when debugging.  Each
        # handler prints its message at the end.
        self.log.debug("\n*** MESSAGE TYPE:%s***", msg_type)
        self.log.debug("   Content: %s\n   --->\n   ", msg["content"])

        should_handle: bool | t.Awaitable[bool] = self.should_handle(socket, msg, idents)
        if inspect.isawaitable(should_handle):
            should_handle = await should_handle
        if not should_handle:
            self.log.debug("Not handling %s:%s", msg_type, msg["header"].get("msg_id"))
            return

        handler = self.shell_handlers.get(msg_type)
        if handler is None:
            self.log.error("Unknown message type: %r", msg_type)
        else:
            self.log.debug("%s: %s", msg_type, msg)
            try:
                self.pre_handler_hook()
            except Exception:
                self.log.debug("Unable to signal in pre_handler_hook:", exc_info=True)
            try:
                if msg_type == "execute_request":
                    send_stream = self._send_exec_request[socket]
                    await send_stream.send((time.monotonic(), socket, idents, msg))
                else:
                    result = handler(socket, idents, msg)
                    if inspect.isawaitable(result):
                        await result
            except Exception as e:
                self.log.error("Exception in message handler:", exc_info=e)
            except KeyboardInterrupt:
                # Ctrl-c shouldn't crash the kernel here.
                self.log.error("KeyboardInterrupt caught in kernel.")
            finally:
                try:
                    self.post_handler_hook()
                except Exception:
                    self.log.debug("Unable to signal in post_handler_hook:", exc_info=True)
        if msg_type != "execute_request":
            self._publish_status("idle", msg)

    async def control_main(self):
        assert self.control_socket is not None
        async with self.control_socket, create_task_group() as tg:
            for task in self.control_tasks:
                tg.start_soon(task)
            tg.start_soon(self.process_control)
            await to_thread.run_sync(self.control_stop.wait)
            tg.cancel_scope.cancel()

    def pre_handler_hook(self):
        """Hook to execute before calling message handler"""
        # ensure default_int_handler during handler call

    def post_handler_hook(self):
        """Hook to execute after calling message handler"""

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

    async def start(self, tg: anyio.abc.TaskGroup) -> None:
        """Process messages on shell and control channels"""
        self._tg_main = tg
        self.control_stop = threading.Event()
        if not self._is_test and self.control_socket is not None:
            if self.control_thread:
                self.control_thread.start_soon(self.control_main)
                self.control_thread.start()
            else:
                tg.start_soon(self.control_main)

        self.shell_interrupt: queue.Queue[bool] = queue.Queue()
        self.shell_is_awaiting = False
        self.shell_is_blocking = False
        self.shell_stop = threading.Event()

        tg.start_soon(self.shell_main, None)
        await self._main_subshell_ready.wait()
        if self.shell_channel_thread:
            # Assign tasks to and start shell channel thread.
            manager = self.shell_channel_thread.manager
            self.shell_channel_thread.start_soon(self.shell_channel_thread_main)
            self.shell_channel_thread.start_soon(
                partial(manager.listen_from_control, self.shell_main, self.shell_channel_thread)
            )
            self.shell_channel_thread.start_soon(manager.listen_from_subshells)
            self.shell_channel_thread.start()

    def stop(self):
        self.comm_manager.kernel = None
        if event := getattr(self, "debugpy_stop", None):
            event.set()
        self.shell_stop.set()
        self.control_stop.set()
        self._main_subshell_ready = Event()

    def record_ports(self, ports):
        """Record the ports that this kernel is using.

        The creator of the IPythonAKernel instance must call this methods if they
        want the :meth:`connect_request` method to return the port numbers.
        """
        self._recorded_ports = ports

    # ---------------------------------------------------------------------------
    # IPythonAKernel request handlers
    # ---------------------------------------------------------------------------

    def _publish_status(self, status: str, parent):
        """send status (busy/idle) on IOPub"""

        self.session.send(
            self.iopub_socket,
            "status",
            {"execution_state": status},
            parent=parent,
            ident=self._topic("status"),
        )

    def _publish_debug_event(self, event):
        self.session.send(
            self.iopub_socket,
            "debug_event",
            event,
            parent=self.parent_msg,
            ident=self._topic("debug_event"),
        )

    def _set_parent_ident(self, parent, ident):
        self._parent_msg = parent
        self._parent_ident = ident
        self.shell.set_parent(parent)

    @property
    def parent_msg(self):
        """The message of the most recent execution request.

        .. versionadded:: 7
        """
        try:
            return self._parent_msg
        except AttributeError:
            return {}

    @property
    def parent_ident(self):
        """The ident of the most recent execution request.

        .. versionadded:: 7
        """
        try:
            return self._parent_ident
        except AttributeError:
            return []

    def send_response(
        self,
        socket,
        msg_or_type,
        content=None,
        ident=None,
        buffers=None,
        track=False,
        header=None,
        metadata=None,
    ):
        """Send a response to the message we're currently processing.

        This accepts all the parameters of :meth:`jupyter_client.session.Session.send`
        except ``parent``.
        """
        return self.session.send(
            socket,
            msg_or_type,
            content=content,
            parent=self.parent_msg,
            ident=ident,
            buffers=buffers,
            track=track,
            header=header,
            metadata=metadata,
        )

    async def execute_request(self, socket, ident, parent):
        """handle an execute_request"""
        content = parent["content"]
        silent = content["silent"]
        stop_on_error = content.pop("stop_on_error", True)

        self._set_parent_ident(parent, ident)
        self._publish_status("busy", parent)

        # Re-broadcast our input for the benefit of listening clients, and
        # start computing output
        if not silent:
            self.execution_count += 1
            self.session.send(
                self.iopub_socket,
                "execute_input",
                {"code": content["code"], "execution_count": self.execution_count},
                parent=parent,
                ident=self._topic("execute_input"),
            )
        # Call do_execute with the appropriate arguments
        reply_content = await self.do_execute(**content)

        # Flush output before sending the reply.
        if sys.stdout is not None:
            sys.stdout.flush()
        if sys.stderr is not None:
            sys.stderr.flush()
        # FIXME: on rare occasions, the flush doesn't seem to make it to the
        # clients... This seems to mitigate the problem, but we definitely need
        # to better understand what's going on.
        if self._execute_sleep:
            time.sleep(self._execute_sleep)

        # Send the reply.
        reply_msg = self.session.send(socket, "execute_reply", content=reply_content, parent=parent, ident=ident)

        self.log.debug("%s", reply_msg)

        assert reply_msg is not None
        if reply_msg["content"]["status"] == "error":
            self.log.exception("Execution failed %s", reply_msg)
            if not silent and stop_on_error:
                self._stop_on_error_time = time.monotonic()
                self.log.info("Rejecting non-silent execute request")

    async def do_execute(
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
        self._forward_input(allow_stdin)
        reply_content: dict[str, t.Any] = {}
        try:

            @dataclass
            class Execution:
                interrupt: bool = False
                result: t.Any = None

            async def run(execution: Execution) -> None:
                execution.result = await shell.run_cell_async(
                    raw_cell=code,
                    store_history=store_history,
                    silent=silent,
                    transformed_cell=shell.transform_cell(code),
                )
                if not execution.interrupt:
                    self.shell_interrupt.put(False)

            res = None
            try:
                async with create_task_group() as tg:
                    execution = Execution()
                    self.shell_is_awaiting = True
                    tg.start_soon(run, execution)
                    execution.interrupt = await to_thread.run_sync(self.shell_interrupt.get)
                    self.shell_is_awaiting = False
                    if execution.interrupt:
                        tg.cancel_scope.cancel()
                    res = execution.result
            finally:
                shell.events.trigger("post_execute")
                if not silent:
                    shell.events.trigger("post_run_cell", res)
        finally:
            self._restore_input()

        if res is not None:
            err = res.error_before_exec if res.error_before_exec is not None else res.error_in_exec
        else:
            err = KeyboardInterrupt()
        if res is not None and res.success:
            reply_content["status"] = "ok"
        else:
            reply_content["status"] = "error"
            reply_content.update({
                "traceback": shell._last_traceback or [],
                "ename": str(type(err).__name__),
                "evalue": str(err),
            })

        # Return the execution counter so clients can display prompts
        reply_content["execution_count"] = shell.execution_count - 1
        if "traceback" in reply_content:
            self.log.info(
                "Exception in execute request:\n%s",
                "\n".join(reply_content["traceback"]),
            )
        # At this point, we can tell whether the main code execution succeeded
        # or not.  If it did, we proceed to evaluate user_expressions
        if reply_content["status"] == "ok":
            reply_content["user_expressions"] = shell.user_expressions(user_expressions or {})
        else:
            # If there was an error, don't even try to compute expressions
            reply_content["user_expressions"] = {}

        # Payloads should be retrieved regardless of outcome, so we can both
        # recover partial output (that could have been generated early in a
        # block, before an error) and always clear the payload system.
        reply_content["payload"] = shell.payload_manager.read_payload()
        # Be aggressive about clearing the payload because we don't want
        # it to sit in memory until the next execute_request comes in.
        shell.payload_manager.clear_payload()
        return reply_content

    async def is_complete_request(self, socket, ident, parent):
        """Handle an is_complete request."""
        content = parent["content"]
        code = content["code"]

        reply_content = await self.do_is_complete(code)
        reply_msg = self.session.send(socket, "is_complete_reply", reply_content, parent, ident)
        self.log.debug("%s", reply_msg)

    async def do_is_complete(self, code):
        """Handle an is_complete request."""
        status, indent_spaces = self.shell.input_transformer_manager.check_complete(code)
        r = {"status": status}
        if status == "incomplete":
            r["indent"] = " " * indent_spaces
        return r

    async def do_complete(self, code, cursor_pos):
        """
        Completions from IPython, using Jedi.
        """
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

    async def complete_request(self, socket, ident, parent):
        """Handle a completion request."""

        content = parent["content"]
        code = content["code"]
        cursor_pos = content["cursor_pos"]

        matches = await self.do_complete(code, cursor_pos)
        self.session.send(socket, "complete_reply", matches, parent, ident)

    async def inspect_request(self, socket, ident, parent):
        """Handle an inspect request."""

        content = parent["content"]
        reply_content = await self.do_inspect(
            content["code"],
            content["cursor_pos"],
            content.get("detail_level", 0),
            set(content.get("omit_sections", [])),
        )
        msg = self.session.send(socket, "inspect_reply", reply_content, parent, ident)
        self.log.debug("%s", msg)

    async def do_inspect(self, code, cursor_pos, detail_level=0, omit_sections=()):
        """Handle code inspection."""
        name = token_at_cursor(code, cursor_pos)

        reply_content: dict[str, t.Any] = {"status": "ok"}
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

    async def history_request(self, socket, ident, parent):
        """Handle a history request."""

        content = parent["content"]
        reply_content = await self.do_history(**content)
        msg = self.session.send(socket, "history_reply", reply_content, parent, ident)
        self.log.debug("%s", msg)

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

    async def connect_request(self, socket, ident, parent):
        """Handle a connect request."""

        content = self._recorded_ports.copy() if self._recorded_ports else {}
        content["status"] = "ok"
        msg = self.session.send(socket, "connect_reply", content, parent, ident)
        self.log.debug("%s", msg)

    @property
    def kernel_info(self):
        from ipykernel.debugger import _is_debugpy_available

        supported_features: list[str] = []
        if self._supports_kernel_subshells:
            supported_features.append("kernel subshells")
        if _is_debugpy_available:
            supported_features.append("debugger")

        return {
            "protocol_version": kernel_protocol_version,
            "implementation": self.implementation,
            "implementation_version": self.implementation_version,
            "language_info": self.language_info,
            "banner": self.shell.banner,
            "help_links": self.help_links,
            "supported_features": supported_features,
        }

    async def kernel_info_request(self, socket, ident, parent):
        """Handle a kernel info request."""

        content = {"status": "ok"}
        content.update(self.kernel_info)
        msg = self.session.send(socket, "kernel_info_reply", content, parent, ident)
        self.log.debug("%s", msg)

    async def comm_info_request(self, socket, ident, parent):
        """Handle a comm info request."""

        content = parent["content"]
        target_name = content.get("target_name", None)

        # Should this be moved to kernelbase?
        if hasattr(self, "comm_manager"):
            comms = {
                k: {"target_name": v.target_name}
                for (k, v) in self.comm_manager.comms.items()
                if v.target_name == target_name or target_name is None
            }
        else:
            comms = {}
        reply_content = {"comms": comms, "status": "ok"}
        msg = self.session.send(socket, "comm_info_reply", reply_content, parent, ident)
        self.log.debug("%s", msg)

    def _send_interrupt_children(self):
        if os.name == "nt":
            self.log.error("Interrupt message not supported on Windows")
        else:
            pid = os.getpid()
            pgid = os.getpgid(pid)
            # Prefer process-group over process
            # but only if the kernel is the leader of the process group
            if pgid and pgid == pid and hasattr(os, "killpg"):
                try:
                    os.killpg(pgid, SIGINT)
                except OSError:
                    os.kill(pid, SIGINT)
                    raise
            else:
                os.kill(pid, SIGINT)

    async def interrupt_request(self, socket, ident, parent):
        """Handle an interrupt request."""

        content: dict[str, t.Any] = {"status": "ok"}
        try:
            self._send_interrupt_children()
        except OSError as err:
            import traceback

            content = {
                "status": "error",
                "traceback": traceback.format_stack(),
                "ename": str(type(err).__name__),
                "evalue": str(err),
            }

        self.session.send(socket, "interrupt_reply", content, parent, ident=ident)
        return

    async def shutdown_request(self, socket, ident, parent):
        """Handle a shutdown request."""
        content = await self.do_shutdown(parent["content"]["restart"])
        self.session.send(socket, "shutdown_reply", content, parent, ident=ident)
        self.stop()

    async def do_shutdown(self, restart):
        """Handle kernel shutdown."""
        if self.shell:
            self.shell.exit_now = True
        return {"status": "ok", "restart": restart}

    def do_clear(self):
        """Clear the kernel."""
        if self.shell:
            self.shell.reset(False)
        return {"status": "ok"}

    async def debug_request(self, socket, ident, parent):
        """Handle a debug request."""

        content = parent["content"]
        reply_content = self.do_debug_request(content)
        if inspect.isawaitable(reply_content):
            reply_content = await reply_content
        reply_msg = self.session.send(socket, "debug_reply", reply_content, parent, ident)
        self.log.debug("%s", reply_msg)

    async def do_debug_request(self, msg):
        """Handle a debug request."""
        from ipykernel.debugger import _is_debugpy_available

        if _is_debugpy_available:
            return await self.debugger.process_request(msg)
        return None

    # ---------------------------------------------------------------------------
    # Subshell control message handlers
    # ---------------------------------------------------------------------------

    async def create_subshell_request(self, socket, ident, parent) -> None:
        if not self._supports_kernel_subshells:
            self.log.error("Subshells are not supported by this kernel")
            return

        # This should only be called in the control thread if it exists.
        # Request is passed to shell channel thread to process.
        other_socket = await self.shell_channel_thread.manager.get_control_other_socket(self.control_thread)
        await other_socket.asend_json({"type": "create"}).wait()
        reply = await other_socket.arecv_json().wait()

        self.session.send(socket, "create_subshell_reply", reply, parent, ident)

    async def delete_subshell_request(self, socket, ident, parent) -> None:
        if not self._supports_kernel_subshells:
            self.log.error("KERNEL SUBSHELLS NOT SUPPORTED")
            return

        try:
            content = parent["content"]
            subshell_id = content["subshell_id"]
        except Exception:
            self.log.error("Got bad msg from parent: %s", parent)
            return

        # This should only be called in the control thread if it exists.
        # Request is passed to shell channel thread to process.
        other_socket = await self.shell_channel_thread.manager.get_control_other_socket(self.control_thread)
        await other_socket.asend_json({"type": "delete", "subshell_id": subshell_id}).wait()
        reply = await other_socket.arecv_json().wait()

        self.session.send(socket, "delete_subshell_reply", reply, parent, ident)

    async def list_subshell_request(self, socket, ident, parent) -> None:
        if not self._supports_kernel_subshells:
            self.log.error("Subshells are not supported by this kernel")
            return

        # This should only be called in the control thread if it exists.
        # Request is passed to shell channel thread to process.
        other_socket = await self.shell_channel_thread.manager.get_control_other_socket(self.control_thread)
        await other_socket.asend_json({"type": "list"}).wait()
        reply = await other_socket.arecv_json().wait()

        self.session.send(socket, "list_subshell_reply", reply, parent, ident)

    # ---------------------------------------------------------------------------
    # Protected interface
    # ---------------------------------------------------------------------------

    def _topic(self, topic):
        """prefixed topic for IOPub messages"""
        return (f"kernel.{self.ident}.{topic}").encode()

    def _no_raw_input(self):
        """Raise StdinNotImplementedError if active frontend doesn't support
        stdin."""
        msg = "raw_input was called, but this frontend does not support stdin."
        raise StdinNotImplementedError(msg)

    def getpass(self, prompt="", stream=None):
        """Forward getpass to frontends

        Raises
        ------
        StdinNotImplementedError if active frontend doesn't support stdin.
        """
        if not self._allow_stdin:
            msg = "getpass was called, but this frontend does not support input requests."
            raise StdinNotImplementedError(msg)
        if stream is not None:
            import warnings

            warnings.warn(
                "The `stream` parameter of `getpass.getpass` will have no effect when using ipykernel",
                UserWarning,
                stacklevel=2,
            )
        return self._input_request(prompt, password=True)

    def raw_input(self, prompt=""):
        """Forward raw_input to frontends

        Raises
        ------
        StdinNotImplementedError if active frontend doesn't support stdin.
        """
        if not self._allow_stdin:
            msg = "raw_input was called, but this frontend does not support input requests."
            raise StdinNotImplementedError(msg)
        return self._input_request(str(prompt), password=False)

    def _input_request(self, prompt, *, password=False):
        # Flush output before making the request.
        if sys.stdout is not None:
            sys.stdout.flush()
        if sys.stderr is not None:
            sys.stderr.flush()

        # flush the stdin socket, to purge stale replies
        while True:
            try:
                self.stdin_socket.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    break
                raise

        # Send the input request.
        assert self.session is not None
        self.session.send(
            self.stdin_socket,
            "input_request",
            content={"prompt": prompt, "password": password},
            parent=self.parent_msg,
            ident=self.parent_ident,
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
                rlist, _, xlist = zmq.select([self.stdin_socket], [], [self.stdin_socket], 0.01)
                if rlist or xlist:
                    ident, reply = self.session.recv(self.stdin_socket)
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
            self.log.error("Bad input_reply: %s", self.parent_msg)
            value = ""
        if value == "\x04":
            # EOF
            raise EOFError
        return value

    def _signal_children(self, signum):
        """
        Send a signal to all our children

        Like `killpg`, but does not include the current process
        (or possible parents).
        """
        sig_rep = f"{Signals(signum)!r}"
        for p in self._process_children():
            self.log.debug("Sending %s to subprocess %s", sig_rep, p)
            try:
                if signum == SIGTERM:
                    p.terminate()
                elif signum == SIGKILL:
                    p.kill()
                else:
                    p.send_signal(signum)
            except psutil.NoSuchProcess:
                pass

    def _process_children(self):
        """Retrieve child processes in the kernel's process group

        Avoids:
        - including parents and self with killpg
        - including all children that may have forked-off a new group
        """
        kernel_process = psutil.Process()
        all_children = kernel_process.children(recursive=True)
        if os.name == "nt":
            return all_children
        kernel_pgid = os.getpgrp()
        process_group_children = []
        for child in all_children:
            try:
                child_pgid = os.getpgid(child.pid)
            except OSError:
                pass
            else:
                if child_pgid == kernel_pgid:
                    process_group_children.append(child)
        return process_group_children

    async def _progressively_terminate_all_children(self):
        sleeps = (0.01, 0.03, 0.1, 0.3, 1, 3, 10)
        if not self._process_children():
            self.log.debug("IPythonAKernel has no children.")
            return

        for signum in (SIGTERM, SIGKILL):
            for delay in sleeps:
                children = self._process_children()
                if not children:
                    self.log.debug("No more children, continuing shutdown routine.")
                    return
                # signals only children, not current process
                self._signal_children(signum)
                self.log.debug(
                    "Will sleep %s sec before checking for children and retrying. %s",
                    delay,
                    children,
                )
                await sleep(delay)

    async def _at_shutdown(self):
        """Actions taken at shutdown by the kernel, called by python's atexit."""
        try:
            # TODO: replace this with anyio equivalent
            await self._progressively_terminate_all_children()
        except Exception as e:
            self.log.exception("Exception during subprocesses termination %s", e)

        finally:
            if self._shutdown_message is not None and self.session:
                self.session.send(
                    self.iopub_socket,
                    self._shutdown_message,
                    ident=self._topic("shutdown"),
                )
                self.log.debug("%s", self._shutdown_message)

    @property
    def _supports_kernel_subshells(self):
        # TODO: Anyio equivalent - just use a main shell object instead...
        return self.shell_channel_thread is not None

    async def process_debugpy(self):
        async with self.debug_shell_socket, self.debugpy_socket, create_task_group() as tg:
            tg.start_soon(self.receive_debugpy_messages)
            tg.start_soon(self.poll_stopped_queue)
            self.debugpy_stop = threading.Event()
            await to_thread.run_sync(self.debugpy_stop.wait)
            tg.cancel_scope.cancel()

    async def receive_debugpy_messages(self):
        from ipykernel.debugger import _is_debugpy_available

        if not _is_debugpy_available:
            return

        while True:
            await self.receive_debugpy_message()

    async def receive_debugpy_message(self, msg=None):
        from ipykernel.debugger import _is_debugpy_available

        if not _is_debugpy_available:
            return

        if msg is None:
            assert self.debugpy_socket is not None
            msg = await self.debugpy_socket.arecv_multipart().wait()
        # The first frame is the socket id, we can drop it
        if msg and isinstance(data := msg[1], bytes):
            frame = data.decode("utf-8")
            self.log.debug("Debugpy received: %s", frame)
            self.debugger.tcp_client.receive_dap_frame(frame)

    async def poll_stopped_queue(self):
        """Poll the stopped queue."""
        while True:
            await self.debugger.handle_stopped_event()

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

    @property
    def execution_count(self):
        if self.shell:
            return self.shell.execution_count
        return None

    @execution_count.setter
    def execution_count(self, value):
        # Ignore the incrementing done by KernelBase, in favour of our shell's
        # execution counter.
        pass

    # async def execute_request(self, stream, ident, parent):
    #     """Override for cell output - cell reconciliation."""
    #     parent_header = extract_header(parent)
    #     # self._associate_new_top_level_threads_with(parent_header)
    #     await super().execute_request(stream, ident, parent)

    # def _associate_new_top_level_threads_with(self, parent_header):
    #     """Store the parent header to associate it with new top-level threads"""
    #     self._new_threads_parent_header = parent_header

    def _initialize_thread_hooks(self):
        """Store thread hierarchy and thread-parent_header associations."""
        stdout = self._stdout
        stderr = self._stderr
        kernel_thread_ident = threading.get_ident()
        kernel = self
        _threading_Thread_run = threading.Thread.run
        _threading_Thread__init__ = threading.Thread.__init__

        def run_closure(self: threading.Thread):
            """Wrap the `threading.Thread.start` to intercept thread identity.

            This is needed because there is no "start" hook yet, but there
            might be one in the future: https://bugs.python.org/issue14073

            This is a no-op if the `self._stdout` and `self._stderr` are not
            sub-classes of `OutStream`.
            """

            try:
                parent = self._ipykernel_parent_thread_ident
            except AttributeError:
                return
            for stream in [stdout, stderr]:
                if isinstance(stream, OutStream):
                    if parent == kernel_thread_ident:
                        stream._thread_to_parent_header[self.ident] = kernel._new_threads_parent_header
                    else:
                        stream._thread_to_parent[self.ident] = parent
            _threading_Thread_run(self)

        def init_closure(self: threading.Thread, *args, **kwargs):
            _threading_Thread__init__(self, *args, **kwargs)
            self._ipykernel_parent_thread_ident = threading.get_ident()

        threading.Thread.__init__ = init_closure  # type:ignore[method-assign]
        threading.Thread.run = run_closure  # type:ignore[method-assign]

    def _clean_thread_parent_frames(self, phase: t.Literal["start", "stop"], info: dict[str, t.Any]):
        """Clean parent frames of threads which are no longer running.
        This is meant to be invoked by garbage collector callback hook.

        The implementation enumerates the threads because there is no "exit" hook yet,
        but there might be one in the future: https://bugs.python.org/issue14073

        This is a no-op if the `self._stdout` and `self._stderr` are not
        sub-classes of `OutStream`.
        """
        # Only run before the garbage collector starts
        if phase != "start":
            return
        active_threads = {thread.ident for thread in threading.enumerate()}
        for stream in [self._stdout, self._stderr]:
            if isinstance(stream, OutStream):
                thread_to_parent_header = stream._thread_to_parent_header
                for identity in list(thread_to_parent_header.keys()):
                    if identity not in active_threads:
                        try:
                            del thread_to_parent_header[identity]
                        except KeyError:
                            pass
                thread_to_parent = stream._thread_to_parent
                for identity in list(thread_to_parent.keys()):
                    if identity not in active_threads:
                        try:
                            del thread_to_parent[identity]
                        except KeyError:
                            pass
