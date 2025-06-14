"""An Application for launching a kernel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import atexit
import errno
import logging
import os
import signal
import sys
import traceback
import typing as t
from contextlib import asynccontextmanager
from functools import partial
from io import FileIO, TextIOWrapper
from logging import StreamHandler
from pathlib import Path
from typing import Literal, cast

import anyio
import anyio.to_thread
import zmq
import zmq_anyio
from IPython.core.application import (
    BaseIPythonApplication,
    base_aliases,
    base_flags,  # type: ignore[import]
)
from IPython.core.profiledir import ProfileDir
from IPython.core.shellapp import InteractiveShellApp, shell_aliases, shell_flags
from jupyter_client.connect import ConnectionFileMixin
from jupyter_client.session import Session, session_aliases, session_flags
from jupyter_core.paths import jupyter_runtime_dir
from traitlets.traitlets import (
    Any,
    Bool,
    Dict,
    DottedObjectName,
    Instance,
    Integer,
    Type,
    Unicode,
    default,
)
from traitlets.utils import filefind
from traitlets.utils.importstring import import_item

from ipykernel.connect import get_connection_info, write_connection_file
from ipykernel.control import ControlThread
from ipykernel.heartbeat import Heartbeat
from ipykernel.iostream import IOPubThread
from ipykernel.kernelbase import IPythonAKernel
from ipykernel.shellchannel import ShellChannelThread
from ipykernel.thread import BaseThread
from ipykernel.zmqshell import ZMQInteractiveShell

# -----------------------------------------------------------------------------
# Flags and Aliases
# -----------------------------------------------------------------------------

kernel_aliases = dict(base_aliases)
kernel_aliases.update(
    {
        "ip": "IPKernelApp.ip",
        "hb": "IPKernelApp.hb_port",
        "shell": "IPKernelApp.shell_port",
        "iopub": "IPKernelApp.iopub_port",
        "stdin": "IPKernelApp.stdin_port",
        "control": "IPKernelApp.control_port",
        "f": "IPKernelApp.connection_file",
        "transport": "IPKernelApp.transport",
    }
)

kernel_flags = dict(base_flags)
kernel_flags.update(
    {
        "no-stdout": ({"IPKernelApp": {"no_stdout": True}}, "redirect stdout to the null device"),
        "no-stderr": ({"IPKernelApp": {"no_stderr": True}}, "redirect stderr to the null device"),
        "trio-loop": (
            {"InteractiveShell": {"trio_loop": False}},
            "Enable Trio as main event loop.",
        ),
    }
)

# inherit flags&aliases for any IPython shell apps
kernel_aliases.update(shell_aliases)
kernel_flags.update(shell_flags)

# inherit flags&aliases for Sessions
kernel_aliases.update(session_aliases)
kernel_flags.update(session_flags)

_ctrl_c_message = """\
NOTE: When using the `ipython kernel` entry point, Ctrl-C will not work.

To exit, you will have to explicitly quit this process, by either sending
"quit" from a client, or using Ctrl-\\ in UNIX-like environments.

To read more about this, see https://github.com/ipython/ipython/issues/2049

"""

# -----------------------------------------------------------------------------
# Application class for starting an IPython IPythonAKernel
# -----------------------------------------------------------------------------


class IPKernelApp(BaseIPythonApplication, InteractiveShellApp, ConnectionFileMixin):
    """The IPYKernel application class."""

    name = "ipython-kernel"
    aliases = Dict(kernel_aliases)  # type:ignore[assignment]
    flags = Dict(kernel_flags)  # type:ignore[assignment]
    classes = [IPythonAKernel, ZMQInteractiveShell, ProfileDir, Session]
    # the kernel class, as an importstring
    kernel_class = Type(
        cast("type[IPythonAKernel]", "ipykernel.kernelbase.IPythonAKernel"),
        klass=IPythonAKernel,
        help="""The IPythonAKernel subclass to be used.

    This should allow easy reuse of the IPKernelApp entry point
    to configure and launch kernels other than IPython's own.
    """,
    ).tag(config=True)
    kernel = Instance(IPythonAKernel)
    poller = Any()  # don't restrict this even though current pollers are all Threads
    heartbeat: Instance[Heartbeat | None] = Instance(Heartbeat, allow_none=True)  # type:ignore[assignment]

    context: Instance[zmq.Context[t.Any] | None] = Any()  # type:ignore[assignment]
    shell_socket = Any()
    control_socket = Any()
    debugpy_socket = Any()
    debug_shell_socket = Any()
    stdin_socket = Any()
    iopub_socket = Any()

    iopub_thread: Instance[IOPubThread | None] = Instance(IOPubThread, allow_none=True)  # type:ignore[assignment]
    control_thread: Instance[BaseThread | None] = Instance(BaseThread, allow_none=True)  # type:ignore[assignment]
    shell_channel_thread: Instance[BaseThread | None] = Instance(BaseThread, allow_none=True)  # type:ignore[assignment]

    session: Instance[Session]

    # Override superclass
    blocking_client = None  # type: ignore[assignment]
    _ports = Dict()

    _log_map: Dict[int, t.Any] = Dict()
    _io_modified = Bool(False)

    subcommands = {
        "install": (
            "ipykernel.kernelspec.InstallIPythonAKernelSpecApp",
            "Install the IPython kernel",
        ),
    }

    @property
    def user_ns(self):
        # If you want to set the namespace; set it on the kernel.
        return self.kernel.user_ns

    # connection info:
    connection_dir = Unicode()

    @default("connection_dir")
    def _default_connection_dir(self):
        return jupyter_runtime_dir()

    @property
    def abs_connection_file(self):
        if Path(self.connection_file).name == self.connection_file and self.connection_dir:
            return str(Path(str(self.connection_dir)) / self.connection_file)
        return self.connection_file

    # streams, etc.
    no_stdout = Bool(False, help="redirect stdout to the null device").tag(config=True)
    no_stderr = Bool(False, help="redirect stderr to the null device").tag(config=True)

    quiet = Bool(True, help="Only send stdout/stderr to output stream").tag(config=True)
    outstream_class = DottedObjectName(
        "ipykernel.iostream.OutStream",
        help="The importstring for the OutStream factory",
        allow_none=True,
    ).tag(config=True)
    displayhook_class = DottedObjectName(
        "ipykernel.displayhook.ZMQDisplayHook", help="The importstring for the DisplayHook factory"
    ).tag(config=True)

    capture_fd_output = Bool(
        True,
        help="""Attempt to capture and forward low-level output, e.g. produced by Extension libraries.
    """,
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

    def _try_bind_socket(self, s, port):
        iface = f"{self.transport}://{self.ip}"
        if self.transport == "tcp":
            if port <= 0:
                port = s.bind_to_random_port(iface)
            else:
                s.bind("tcp://%s:%i" % (self.ip, port))
        elif self.transport == "ipc":
            if port <= 0:
                port = 1
                path = "%s-%i" % (self.ip, port)
                while Path(path).exists():
                    port = port + 1
                    path = "%s-%i" % (self.ip, port)
            else:
                path = "%s-%i" % (self.ip, port)
            s.bind("ipc://%s" % path)
        return port

    def _bind_socket(self, s, port):
        try:
            win_in_use = errno.WSAEADDRINUSE  # type: ignore[attr-defined]
        except AttributeError:
            win_in_use = None

        # Try up to 100 times to bind a port when in conflict to avoid
        # infinite attempts in bad setups
        max_attempts = 1 if port else 100
        for attempt in range(max_attempts):
            try:
                return self._try_bind_socket(s, port)
            except zmq.ZMQError as ze:
                # Raise if we have any error not related to socket binding
                if ze.errno != errno.EADDRINUSE and ze.errno != win_in_use:
                    raise
                if attempt == max_attempts - 1:
                    raise
        return None

    def write_connection_file(self, **kwargs: t.Any) -> None:
        """write connection info to JSON file"""
        cf = self.abs_connection_file
        connection_info = {
            "ip": self.ip,
            "key": self.session.key,
            "transport": self.transport,
            "shell_port": self.shell_port,
            "stdin_port": self.stdin_port,
            "hb_port": self.hb_port,
            "iopub_port": self.iopub_port,
            "control_port": self.control_port,
            "kernel_name": self.kernel_name,
        }
        if Path(cf).exists():
            # If the file exists, merge our info into it. For example, if the
            # original file had port number 0, we update with the actual port
            # used.
            existing_connection_info = get_connection_info(cf, unpack=True)
            assert isinstance(existing_connection_info, dict)
            connection_info = dict(existing_connection_info, **connection_info)
            if connection_info == existing_connection_info:
                self.log.debug("Connection file %s with current information already exists", cf)
                return

        self.log.debug("Writing connection file: %s", cf)

        write_connection_file(cf, **connection_info)

    def cleanup_connection_file(self):
        """Clean up our connection file."""
        cf = self.abs_connection_file
        self.log.debug("Cleaning up connection file: %s", cf)
        try:
            Path(cf).unlink()
        except OSError:
            pass

        self.cleanup_ipc_files()

    def init_connection_file(self):
        """Initialize our connection file."""
        if not self.connection_file:
            self.connection_file = f"kernel-{os.getpid()}.json"
        try:
            self.connection_file = filefind(self.connection_file, [".", self.connection_dir])
        except OSError:
            self.log.debug("Connection file not found: %s", self.connection_file)
            # This means I own it, and I'll create it in this directory:
            Path(self.abs_connection_file).parent.mkdir(mode=0o700, exist_ok=True, parents=True)
            # Also, I will clean it up:
            atexit.register(self.cleanup_connection_file)
            return
        try:
            self.load_connection_file()
        except Exception:
            self.log.error(  # noqa: G201
                "Failed to load connection file: %r", self.connection_file, exc_info=True
            )
            self.exit(1)

    def init_sockets(self):
        """Create a context, a session, and the kernel sockets."""
        self.log.info("Starting the kernel at pid: %", os.getpid())
        assert self.context is None, "init_sockets cannot be called twice!"
        self.context = context = zmq.Context()
        atexit.register(self.close)

        self.shell_socket = zmq_anyio.Socket(context.socket(zmq.ROUTER))
        self.shell_socket.linger = 1000
        self.shell_port = self._bind_socket(self.shell_socket, self.shell_port)
        self.log.debug("shell ROUTER Channel on port: %i", self.shell_port)

        self.stdin_socket = context.socket(zmq.ROUTER)
        self.stdin_socket.linger = 1000
        self.stdin_port = self._bind_socket(self.stdin_socket, self.stdin_port)
        self.log.debug("stdin ROUTER Channel on port: %i", self.stdin_port)

        if hasattr(zmq, "ROUTER_HANDOVER"):
            # set router-handover to workaround zeromq reconnect problems
            # in certain rare circumstances
            # see ipython/ipykernel#270 and zeromq/libzmq#2892
            self.shell_socket.router_handover = self.stdin_socket.router_handover = 1

        self.init_control(context)
        self.init_iopub(context)

    def init_control(self, context):
        """Initialize the control channel."""
        self.control_socket = zmq_anyio.Socket(context.socket(zmq.ROUTER))
        self.control_socket.linger = 1000
        self.control_port = self._bind_socket(self.control_socket, self.control_port)
        self.log.debug("control ROUTER Channel on port: %i", self.control_port)

        self.debugpy_socket = zmq_anyio.Socket(context.socket(zmq.STREAM))
        self.debugpy_socket.linger = 1000

        self.debug_shell_socket = zmq_anyio.Socket(context.socket(zmq.DEALER))
        self.debug_shell_socket.linger = 1000
        last_endpoint = self.shell_socket.get_string(zmq.LAST_ENDPOINT)
        if last_endpoint:
            self.debug_shell_socket.connect(last_endpoint)

        if hasattr(zmq, "ROUTER_HANDOVER"):
            # set router-handover to workaround zeromq reconnect problems
            # in certain rare circumstances
            # see ipython/ipykernel#270 and zeromq/libzmq#2892
            self.control_socket.router_handover = 1

        self.control_thread = ControlThread(daemon=True)
        self.shell_channel_thread = ShellChannelThread(context, self.shell_socket, daemon=True)

    def init_iopub(self, context):
        """Initialize the iopub channel."""
        self.iopub_socket = zmq_anyio.Socket(context.socket(zmq.PUB))
        self.iopub_socket.linger = 1000
        self.iopub_port = self._bind_socket(self.iopub_socket, self.iopub_port)
        self.log.debug("iopub PUB Channel on port: %i", self.iopub_port)
        self.iopub_thread = IOPubThread(self.iopub_socket)
        self.iopub_thread.start()
        # backward-compat: wrap iopub socket API in background thread
        self.iopub_socket = self.iopub_thread.background_socket

    def init_heartbeat(self):
        """start the heart beating"""
        # heartbeat doesn't share context, because it mustn't be blocked
        # by the GIL, which is accessed by libzmq when freeing zero-copy messages
        hb_ctx = zmq.Context()
        self.heartbeat = Heartbeat(hb_ctx, (self.transport, self.ip, self.hb_port))
        self.hb_port = self.heartbeat.port
        self.log.debug("Heartbeat REP Channel on port: %i", self.hb_port)
        self.heartbeat.start()

    def close(self):
        """Close zmq sockets in an orderly fashion"""
        # un-capture IO before we start closing channels
        self.reset_io()
        self.log.info("Cleaning up sockets")
        if self.heartbeat:
            self.log.debug("Closing heartbeat channel")
            self.heartbeat.context.term()
        if self.iopub_thread is not None:
            self.log.debug("Closing iopub channel")
            self.iopub_thread.stop()
            self.iopub_thread.close()
        if self.control_thread is not None and self.control_thread.is_alive():
            self.log.debug("Closing control thread")
            self.control_thread.stop()
            self.control_thread.join()
        if self.shell_channel_thread is not None and self.shell_channel_thread.is_alive():
            self.log.debug("Closing shell channel thread")
            self.shell_channel_thread.stop()
            self.shell_channel_thread.join()

        if self.debugpy_socket and not self.debugpy_socket.closed:
            self.debugpy_socket.close()
        if self.debug_shell_socket and not self.debug_shell_socket.closed:
            self.debug_shell_socket.close()

        for channel in ("shell", "control", "stdin"):
            self.log.debug("Closing %s channel", channel)
            socket = getattr(self, channel + "_socket", None)
            if socket and not socket.closed:
                socket.close()
        self.log.debug("Terminating zmq context")
        if self.context:
            self.context.term()
        self.log.debug("Terminated zmq context")

    async def log_connection_info(self):
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
        # also raw print to the terminal if no parent_handle (`ipython kernel`)
        # unless log-level is CRITICAL (--quiet)
        if not self.parent_handle and int(self.log_level) < logging.CRITICAL:  # type:ignore[call-overload]
            print(_ctrl_c_message, file=sys.__stdout__)
            for line in lines:
                print(line, file=sys.__stdout__)

        self._ports = {
            "shell": self.shell_port,
            "iopub": self.iopub_port,
            "stdin": self.stdin_port,
            "hb": self.hb_port,
            "control": self.control_port,
        }

    async def init_io(self):
        """Redirect input streams and set a display hook."""
        self._save_io()
        if self.outstream_class:
            outstream_factory = import_item(str(self.outstream_class))

            e_stdout = None if self.quiet else sys.stdout
            e_stderr = None if self.quiet else sys.stderr

            if not self.capture_fd_output:
                outstream_factory = partial(outstream_factory)

            if sys.stdout is not None:
                sys.stdout.flush()
            sys.stdout = outstream_factory(self.session, self.iopub_thread, "stdout", echo=e_stdout)

            if sys.stderr is not None:
                sys.stderr.flush()
            sys.stderr = outstream_factory(self.session, self.iopub_thread, "stderr", echo=e_stderr)

            if hasattr(sys.stderr, "_original_stdstream_copy"):
                for handler in self.log.handlers:
                    if (
                        isinstance(handler, StreamHandler)
                        and (buffer := getattr(handler.stream, "buffer", None))
                        and (fileno := getattr(buffer, "fileno", None))
                        and fileno() == sys.stderr._original_stdstream_fd
                    ):
                        self.log.debug("Seeing logger to stderr, rerouting to raw filedescriptor.")
                        io_wrapper = TextIOWrapper(FileIO(sys.stderr._original_stdstream_copy, "w", closefd=False))
                        self._log_map[id(io_wrapper)] = handler.stream
                        handler.stream = io_wrapper
        if self.displayhook_class:
            displayhook_factory = import_item(str(self.displayhook_class))
            self.displayhook = displayhook_factory(self.session, self.iopub_socket)
            sys.displayhook = self.displayhook

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
        if hasattr(stderr, "_original_stdstream_copy"):
            for handler in self.log.handlers:
                if orig_stream := self._log_map.get(id(handler.stream)):
                    self.log.debug("Seeing modified logger, rerouting back to stderr")
                    handler.stream = orig_stream
            self._log_map = {}
        if self.outstream_class:
            outstream_factory = import_item(str(self.outstream_class))
            if isinstance(stderr, outstream_factory):
                stderr.close()
            if isinstance(stdout, outstream_factory):
                stdout.close()

    def sigint_handler(self, *args):
        if self.kernel.shell_is_awaiting:
            self.kernel.shell_interrupt.put(True)
        elif self.kernel.shell_is_blocking:
            raise KeyboardInterrupt

    async def init_signal(self):
        """Initialize the signal handler."""
        signal.signal(signal.SIGINT, self.sigint_handler)

    async def init_kernel(self):
        """Create the IPythonAKernel object itself"""
        kernel_factory = self.kernel_class.instance

        kernel = kernel_factory(
            parent=self,
            session=self.session,
            control_socket=self.control_socket,
            debugpy_socket=self.debugpy_socket,
            debug_shell_socket=self.debug_shell_socket,
            shell_socket=self.shell_socket,
            control_thread=self.control_thread,
            shell_channel_thread=self.shell_channel_thread,
            iopub_thread=self.iopub_thread,
            iopub_socket=self.iopub_socket,
            stdin_socket=self.stdin_socket,
            log=self.log,
            profile_dir=self.profile_dir,
        )
        kernel.record_ports({name + "_port": port for name, port in self._ports.items()})
        self.kernel = kernel

        # Allow the displayhook to get the execution count
        self.displayhook.get_execution_count = lambda: kernel.execution_count
    async def init_shell(self):
        """Initialize the shell channel."""
        self.init_path()
        shell = self.kernel.shell
        self.shell = shell
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
    async def start_with_context(self):  # TODO: A better name?
        """Start the application inside the current anyio event loop.

        ``` python

        app = cls.instance()
        app.initialize([])
        async app.start_here():
            await anyio.sleep_forever()
        # app has closed
        ```
        """
        async with anyio.create_task_group() as tg:
            # TODO: start these with taskgroup
            await self.init_pdb()
            self.init_connection_file()
            self.init_sockets()
            self.init_heartbeat()
            # writing/displaying connection info must be *after* init_sockets/heartbeat
            self.write_connection_file()
            # Log connection info after writing connection file, so that the connection
            # file is definitely available at the time someone reads the log.
            await self.log_connection_info()
            await self.init_io()
            await self.init_signal()

            await self.init_kernel()
            await self.init_shell()
            self.init_extensions()
            self.init_code()
            # flush stdout/stderr, so that anything written to these streams during
            # initialization do not get associated with the first execution request
            sys.stdout.flush()
            sys.stderr.flush()

            tg.start_soon(self.kernel.start, tg)

            await self.kernel._main_subshell_ready.wait()
            try:
                yield self
            finally:
                pass
                # do the cleanup here incase we want to restart

    @classmethod
    def start(cls, backend: Literal["trio", "asyncio", "asyncio_eager"] = "asyncio") -> None:
        """Start the application."""

        if backend == "asyncio" and sys.platform == "win32":
            import asyncio

            policy = asyncio.get_event_loop_policy()
            if policy.__class__.__name__ == "WindowsProactorEventLoopPolicy":
                from anyio._core._asyncio_selector_thread import get_selector

                selector = get_selector()
                selector._thread.pydev_do_not_trace = True

        async def _start() -> None:
            """ """
            app = cls.instance()
            async with app.start_with_context():
                # Block forever
                await anyio.Event().wait()

        anyio.run(_start, backend=backend)

    def stop(self) -> None:
        """Stop the kernel, thread-safe."""
        self.kernel.stop()


launch_new_instance = IPKernelApp.launch_instance
