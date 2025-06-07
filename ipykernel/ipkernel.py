"""The IPython kernel implementation"""

from __future__ import annotations

import builtins
import gc
import getpass
import os
import sys
import threading
import typing as t
from dataclasses import dataclass
from typing import TYPE_CHECKING

import comm
import zmq_anyio
from anyio import TASK_STATUS_IGNORED, create_task_group, to_thread
from comm.base_comm import CommManager
from IPython.core import release
from IPython.utils.tokenutil import token_at_cursor
from traitlets import Any, Bool, Dict, Instance, List, Type, default, observe
from typing_extensions import override

from ipykernel.compiler import XCachingCompiler
from ipykernel.iostream import OutStream
from ipykernel.kernelbase import Kernel as KernelBase
from ipykernel.zmqshell import ZMQInteractiveShell

if TYPE_CHECKING:
    from anyio.abc import TaskStatus

    from ipykernel.debugger import Debugger

try:
    from IPython.core.completer import provisionalcompleter as _provisionalcompleter
    from IPython.core.completer import rectify_completions as _rectify_completions

    _use_experimental_60_completion = True
except ImportError:
    _use_experimental_60_completion = False


class IPythonKernel(KernelBase):
    """The IPython Kernel class."""

    shell = Instance(ZMQInteractiveShell)
    shell_class = Type(ZMQInteractiveShell)
    comm_manager = Instance(CommManager)

    # use fully-qualified name to ensure lazy import and prevent the issue from
    # https://github.com/ipython/ipykernel/issues/1198
    debugger_class = Type("ipykernel.debugger.Debugger")

    compiler_class = Type(XCachingCompiler)

    use_experimental_completions = Bool(
        True,
        help="Set this flag to False to deactivate the use of experimental IPython completion APIs.",
    ).tag(config=True)

    debugpy_socket = Instance(zmq_anyio.Socket)

    user_module = Any()

    @observe("user_module")
    def _user_module_changed(self, change):
        if self.shell is not None:
            self.shell.user_module = change["new"]

    user_ns = Dict()

    @default("comm_manager")
    def _default_comm_manager(self):
        return comm.get_comm_manager()

    @observe("user_ns")
    def _user_ns_changed(self, change):
        if self.trait_has_value("shell"):
            self.shell.user_ns = change["new"]
            self.shell.init_user_ns()

    # A reference to the Python builtin 'raw_input' function.
    # (i.e., __builtin__.raw_input for Python 2.7, builtins.input for Python 3)
    _sys_raw_input = Any()
    _sys_eval_input = Any()

    def __init__(self, **kwargs):
        """Initialize the kernel."""
        super().__init__(**kwargs)

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

    # Kernel info fields
    implementation = "ipython"
    implementation_version = release.version
    language_info = {
        "name": "python",
        "version": sys.version.split()[0],
        "mimetype": "text/x-python",
        "codemirror_mode": {"name": "ipython", "version": sys.version_info[0]},
        "pygments_lexer": "ipython%d" % 3,
        "nbconvert_exporter": "python",
        "file_extension": ".py",
    }

    async def process_debugpy(self):
        async with self.debug_shell_socket, self.debugpy_socket, create_task_group() as tg:
            tg.start_soon(self.receive_debugpy_messages)
            tg.start_soon(self.poll_stopped_queue)
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

    @property
    def banner(self):
        if self.shell:
            return self.shell.banner
        return None

    async def poll_stopped_queue(self):
        """Poll the stopped queue."""
        while True:
            await self.debugger.handle_stopped_event()

    async def start(self, *, task_status: TaskStatus = TASK_STATUS_IGNORED) -> None:
        """Start the kernel."""
        if self.shell:
            self.shell.exit_now = False
        if self.debugpy_socket is None:
            self.log.warning("debugpy_socket undefined, debugging will not be enabled")
        else:
            self.debugpy_stop = threading.Event()
            self.control_tasks.append(self.process_debugpy)
        await super().start(task_status=task_status)

    def stop(self):
        super().stop()
        if self.debugpy_socket is not None:
            self.debugpy_stop.set()

    def _set_parent_ident(self, parent, ident):
        super()._set_parent_ident(parent, ident)
        self.shell.set_parent(parent)

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

    @override
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

    async def do_debug_request(self, msg):
        """Handle a debug request."""
        from ipykernel.debugger import _is_debugpy_available

        if _is_debugpy_available:
            return await self.debugger.process_request(msg)
        return None

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
        if self.shell:
            self.shell.exit_now = True
        return {"status": "ok", "restart": restart}

    async def do_is_complete(self, code):
        """Handle an is_complete request."""
        transformer_manager = getattr(self.shell, "input_transformer_manager", None)
        if transformer_manager is None:
            # input_splitter attribute is deprecated

            transformer_manager = self.shell.input_splitter
        status, indent_spaces = transformer_manager.check_complete(code)
        r = {"status": status}
        if status == "incomplete":
            r["indent"] = " " * indent_spaces
        return r

    def do_clear(self):
        """Clear the kernel."""
        if self.shell:
            self.shell.reset(False)
        return {"status": "ok"}

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
