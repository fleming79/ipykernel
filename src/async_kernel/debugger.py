"""Debugger implementation for the IPython kernel."""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
import typing as t
from pathlib import Path
from typing import TYPE_CHECKING, Generic, TypeVar

import anyio.abc
import traitlets
from IPython.core.inputtransformer2 import leading_empty_lines

from async_kernel import utils
from async_kernel.compiler import get_file_name, get_tmp_directory, get_tmp_hash_seed

if TYPE_CHECKING:
    from anyio.abc import TaskGroup, TaskStatus

    from async_kernel import Kernel

try:
    if "PYDEVD_IPYTHON_COMPATIBLE_DEBUGGING" not in os.environ:
        os.environ["PYDEVD_IPYTHON_COMPATIBLE_DEBUGGING"] = "1"

    # This import is required to provide _pydevd_bundle imports
    from debugpy.server import api  # noqa: F401

    _is_debugpy_available = True
except ImportError:
    _is_debugpy_available = False
except Exception as e:
    # We cannot import the module where the DebuggerInitializationError
    # is defined
    if e.__class__.__name__ == "DebuggerInitializationError":
        _is_debugpy_available = False
    else:
        raise e

T = TypeVar("T")


class PendingResult(Generic[T]):
    "A lightweight anyio non-compliant variant of a Future."

    def __init__(self) -> None:
        self._exception = None
        self._event_done = anyio.Event()

    async def wait(self) -> T:
        await self._event_done.wait()
        if self._exception:
            raise self._exception
        return self.result

    def set_result(self, value):
        if self._event_done.is_set():
            raise RuntimeError
        self.result = value
        self._event_done.set()

    def set_exception(self, exception: Exception):
        if self._event_done.is_set():
            raise RuntimeError
        self._exception = exception


class _FakeCode:
    """Fake code class."""

    def __init__(self, co_filename, co_name):
        """Init."""
        self.co_filename = co_filename
        self.co_name = co_name


class _FakeFrame:
    """Fake frame class."""

    def __init__(self, f_code, f_globals, f_locals):
        """Init."""
        self.f_code = f_code
        self.f_globals = f_globals
        self.f_locals = f_locals
        self.f_back = None


class _DummyPyDB:
    """Fake PyDb class."""

    def __init__(self):
        """Init."""
        from _pydevd_bundle.pydevd_api import PyDevdAPI  # type: ignore[attr-defined]  # noqa: PLC0415

        self.variable_presentation = PyDevdAPI.VariablePresentation()


class VariableExplorer(traitlets.HasTraits):
    """A variable explorer."""

    kernel: traitlets.Instance[Kernel] = traitlets.Instance("async_kernel.Kernel", ())

    def __init__(self):
        """Initialize the explorer."""
        from _pydevd_bundle.pydevd_suspended_frames import SuspendedFramesManager, _FramesTracker  # type: ignore[attr-defined]  # noqa: I001, PLC0415

        self.suspended_frame_manager = SuspendedFramesManager()
        self.py_db = _DummyPyDB()
        self.tracker = _FramesTracker(self.suspended_frame_manager, self.py_db)
        self.frame = None

    def track(self):
        """Start tracking."""
        from _pydevd_bundle import pydevd_frame_utils  # type: ignore[attr-defined]  # noqa: PLC0415

        var = self.kernel.shell.user_ns
        self.frame = _FakeFrame(_FakeCode("<module>", get_file_name("sys._getframe()")), var, var)
        self.tracker.track("thread1", pydevd_frame_utils.create_frames_list_from_frame(self.frame))

    def untrack_all(self):
        """Stop tracking."""
        self.tracker.untrack_all()

    def get_children_variables(self, variable_ref=None):
        """Get the child variables for a variable reference."""
        var_ref = variable_ref
        if not var_ref:
            var_ref = id(self.frame)
        variables = self.suspended_frame_manager.get_variable(var_ref)
        return [x.get_var_data() for x in variables.get_children_variables()]


class DebugpyClient(traitlets.HasTraits):
    """A client for debugpy."""

    HEADER = b"Content-Length: "
    SEPARATOR = b"\r\n\r\n"
    SEPARATOR_LENGTH = 4
    tcp_buffer = b""
    _pending_responses: traitlets.Dict[int, PendingResult] = traitlets.Dict()
    capabilities = traitlets.Dict()
    kernel: traitlets.Instance[Kernel] = traitlets.Instance("async_kernel.Kernel", ())
    _host_port = None
    _socketstream: anyio.abc.SocketStream | None = None

    def __init__(self, log, event_callback):
        """Initialize the client."""
        self.log = log
        self.event_callback = event_callback
        self._pack = self.kernel.session.pack
        self._unpack = self.kernel.session.unpack

    @property
    def connected(self):
        return bool(self._socketstream)

    async def _send_request(self, request: dict):
        if not (socketstream := self._socketstream):
            msg = "socketstream not connected"
            raise RuntimeError(msg)
        content = self._pack(request)
        content_length = str(len(content)).encode()
        buf = self.HEADER + content_length + self.SEPARATOR
        buf += content
        self.log.debug("DEBUGPYCLIENT: request %s", buf)
        await socketstream.send(buf)

    async def _wait_for_response(self, request: dict):
        self._pending_responses[request["seq"]] = pending = PendingResult()
        return await pending.wait()

    def put_tcp_frame(self, frame: bytes):
        """Put a tcp frame in the queue."""
        self.tcp_buffer += frame
        data = self.tcp_buffer.split(self.HEADER)
        if len(data) > 1:
            for buf in data[1:]:
                size, raw_msg = buf.split(self.SEPARATOR, maxsplit=1)
                size = int(size)
                if len(raw_msg) >= size:
                    msg: dict[str, t.Any] = self._unpack(raw_msg[:size])
                    self.log.debug("_put_message :%s %s", msg["type"], msg)
                    if msg["type"] == "event":
                        self.event_callback(msg)
                    else:
                        if pending_result := self._pending_responses.pop(msg["request_seq"], None):
                            pending_result.set_result(msg)
                else:
                    self.tcp_buffer = self.HEADER + buf
                    return
            self.tcp_buffer = b""

    def get_host_port(self):
        """Get the host debugpy port."""
        if not self._host_port:
            msg = "host port not available until debugpy is listening!"
            raise RuntimeError(msg)
        host, port = self._host_port
        return {"host": host, "port": port}

    async def connect_tcp_socket(self, *, task_status: TaskStatus):
        """Connect to the tcp socket."""

        if not self._host_port:
            import debugpy  # noqa: PLC0415

            self._host_port = debugpy.listen(0)
            thread = threading.current_thread()
            # This thread can't be stopped by the debugger when debugging
            thread.pydev_do_not_trace = True  # type: ignore[attr-defined]
            thread.is_pydev_daemon_thread = True  # type: ignore[attr-defined]
        try:
            self.log.debug("++ debugpy socketstream connecting ++")
            async with await anyio.connect_tcp(*self._host_port) as socketstream:
                self._socketstream = socketstream
                self.log.debug("++ debugpy socketstream connected ++")
                task_status.started()
                while True:
                    data = await socketstream.receive()
                    self.put_tcp_frame(data)
        except anyio.EndOfStream:
            self.log.debug("++ debugpy socketstream disconnected ++")
            return
        finally:
            self._socketstream = None

    async def send_dap_request(self, msg):
        """Send a dap request and return the response."""
        await self._send_request(msg)
        return await self._wait_for_response(msg)


class Debugger(traitlets.HasTraits):
    """The debugger class."""

    _seq = 0
    breakpoint_list = traitlets.Dict()
    stopped_threads = traitlets.Set()
    _removed_cleanup = traitlets.Dict()
    _initialize_reply = traitlets.Dict()
    just_my_code = traitlets.Bool(True)
    debugpy_initialized = traitlets.Bool()
    variable_explorer = traitlets.Instance(VariableExplorer, ())
    debugpy_client = traitlets.Instance(DebugpyClient)
    log = traitlets.Instance(logging.LoggerAdapter)
    kernel: Kernel
    taskgroup: TaskGroup
    init_event = traitlets.Instance(anyio.Event, ())
    forbidden_names = [
        "__name__",
        "__doc__",
        "__package__",
        "__loader__",
        "__spec__",
        "__annotations__",
        "__builtins__",
        "__builtin__",
        "__display__",
        "get_ipython",
        "debugpy",
        "exit",
        "quit",
        "In",
        "Out",
        "_oh",
        "_dh",
        "_",
        "__",
        "___",
    ]

    @traitlets.default("log")
    def _default_log(self):
        return logging.LoggerAdapter(logging.getLogger(self.__class__.__name__))

    def __init__(self):
        """Initialize the debugger."""
        self.debugpy_client = DebugpyClient(log=self.log, event_callback=self._handle_event)
        self.started_debug_handlers = {
            "dumpCell": self.do_dump_cell,
            "setBreakpoints": self.do_set_breakpoints,
            "source": self.do_source,
            "stackTrace": self.do_stack_trace,
            "variables": self.do_variables,
            "attach": self.do_attach,
            "configurationDone": self.do_configuration_done,
            "disconnect": self.do_disconnect,
        }
        self.static_debug_handlers = {
            "initialize": self.do_initialize,
            "debugInfo": self.do_debug_info,
            "inspectVariables": self.do_inspect_variables,
            "richInspectVariables": self.do_rich_inspect_variables,
            "modules": self.do_modules,
            "copyToGlobals": self.do_copy_to_globals,
        }

    async def start(self, kernel: Kernel, *, task_status: TaskStatus):
        # To be called by `kernel.start_in_context`.
        self.kernel = kernel
        async with anyio.create_task_group() as tg:
            self.taskgroup = tg
            task_status.started()
            await anyio.sleep_forever()

    async def _forward_message(self, msg):
        return await self.debugpy_client.send_dap_request(msg)

    def next_seq(self):
        "A monotonically decreasing negative number so as not to clash with the frontend seq."
        self._seq = self._seq - 1
        return self._seq

    def _handle_event(self, msg):
        if msg["event"] == "stopped":
            if msg["body"]["allThreadsStopped"]:
                self.taskgroup.start_soon(self.handle_stopped_event, msg)
                return
            self.stopped_threads.add(msg["body"]["threadId"])
        elif msg["event"] == "continued":
            if msg["body"]["allThreadsContinued"]:
                self.stopped_threads = set()
            else:
                self.stopped_threads.remove(msg["body"]["threadId"])
        elif msg["event"] == "initialized":
            self.init_event.set()

        self._publish_event(msg)

    def _publish_event(self, event: dict):
        self.kernel.iopub_send(
            msg_or_type="debug_event",
            content=event,
            ident=self.kernel._topic("debug_event"),
        )

    def _build_variables_response(self, request, variables):
        var_list = [var for var in variables if self._accept_variable(var["name"])]
        return {
            "seq": request["seq"],
            "type": "response",
            "request_seq": request["seq"],
            "success": True,
            "command": request["command"],
            "body": {"variables": var_list},
        }

    def _accept_stopped_thread(self, thread_name):
        forbid_list = ["IPythonHistorySavingThread"]
        return thread_name not in forbid_list

    async def handle_stopped_event(self, event):
        """Handle a stopped event."""
        req = {"seq": self.next_seq(), "type": "request", "command": "threads"}
        rep = await self._forward_message(req)
        for thread in rep["body"]["threads"]:
            if self._accept_stopped_thread(thread["name"]):
                self.stopped_threads.add(thread["id"])
            self._publish_event(event)

    def _accept_variable(self, variable_name):
        """Accept a variable by name."""
        return (
            variable_name not in self.forbidden_names
            and not bool(re.search(r"^_\d", variable_name))
            and not variable_name.startswith("_i")
        )

    def _to_response(self, msg: dict, *, success=True):
        return {
            "seq": self.next_seq(),
            "type": "response",
            "request_seq": msg["seq"],
            "success": success,
            "command": msg["command"],
        }

    async def process_request(self, message: dict[str, t.Any]):
        """Process a request."""
        command = message["command"]
        if handler := self.static_debug_handlers.get(command):
            return await handler(message)
        if not self.debugpy_client.connected:
            self.log.debug("Not ready - ignoring command: '%s'", command)
            return {}
        if handler := self.started_debug_handlers.get(command):
            with anyio.move_on_after(4):
                return await handler(message)
            return self._to_response(message, success=False)
        return await self._forward_message(message)

    ## Static handlers

    async def do_initialize(self, message):
        "Initialize debugpy server starting as required."
        if not self.debugpy_client.connected:
            if not self.debugpy_initialized:
                tmp_dir = get_tmp_directory()
                if not Path(tmp_dir).exists():
                    Path(tmp_dir).mkdir(parents=True)
                self.debugpy_initialized = True
            await self.taskgroup.start(self.debugpy_client.connect_tcp_socket)
            # Don't remove leading empty lines when debugging so the breakpoints are correctly positioned
            cleanup_transforms = self.kernel.shell.input_transformer_manager.cleanup_transforms
            if leading_empty_lines in cleanup_transforms:
                index = cleanup_transforms.index(leading_empty_lines)
                self._removed_cleanup[index] = cleanup_transforms.pop(index)
        return await self._forward_message(message)

    async def do_debug_info(self, message):
        """Handle a debug info message."""
        if not _is_debugpy_available or utils.LAUNCHED_BY_DEBUGPY:
            return {}
        breakpoint_list = []
        for key, value in self.breakpoint_list.items():
            breakpoint_list.append({"source": key, "breakpoints": value})
        return {
            "type": "response",
            "request_seq": message["seq"],
            "success": True,
            "command": message["command"],
            "body": {
                "isStarted": self.debugpy_client.connected,
                "hashMethod": "Murmur2",
                "hashSeed": get_tmp_hash_seed(),
                "tmpFilePrefix": get_tmp_directory() + os.sep,
                "tmpFileSuffix": ".py",
                "breakpoints": breakpoint_list,
                "stoppedThreads": list(self.stopped_threads),
                "richRendering": True,
                "exceptionPaths": ["Python Exceptions"],
                "copyToGlobals": True,
            },
        }

    async def do_inspect_variables(self, message):
        """Handle an inspect variables message."""
        self.variable_explorer.untrack_all()
        # looks like the implementation of untrack_all in ptvsd
        # destroys objects we nee din track. We have no choice but
        # reinstantiate the object
        self.variable_explorer = VariableExplorer()
        self.variable_explorer.track()
        variables = self.variable_explorer.get_children_variables()
        return self._build_variables_response(message, variables)

    async def do_rich_inspect_variables(self, message):
        """Handle a rich inspect variables message."""
        reply = {
            "type": "response",
            "sequence_seq": message["seq"],
            "success": False,
            "command": message["command"],
        }
        var_name = message["arguments"]["variableName"]
        valid_name = str.isidentifier(var_name)
        if not valid_name:
            reply["body"] = {"data": {}, "metadata": {}}
            if var_name == "special variables" or var_name == "function variables":
                reply["success"] = True
            return reply
        repr_data = {}
        repr_metadata = {}
        if not self.stopped_threads:
            # The code did not hit a breakpoint, we use the interpreter
            # to get the rich representation of the variable
            result = self.kernel.shell.user_expressions({var_name: var_name})[var_name]
            if result.get("status", "error") == "ok":
                repr_data = result.get("data", {})
                repr_metadata = result.get("metadata", {})
        else:
            # The code has stopped on a breakpoint, we use the setExpression
            # request to get the rich representation of the variable
            code = f"get_ipython().display_formatter.format({var_name})"
            frame_id = message["arguments"]["frameId"]
            reply = await self._forward_message(
                {
                    "type": "request",
                    "command": "evaluate",
                    "seq": self.next_seq(),
                    "arguments": {"expression": code, "frameId": frame_id, "context": "clipboard"},
                }
            )
            if reply["success"]:
                repr_data, repr_metadata = eval(reply["body"]["result"], {}, {})
        body = {
            "data": repr_data,
            "metadata": {k: v for k, v in repr_metadata.items() if k in repr_data},
        }
        reply["body"] = body
        reply["success"] = True
        return reply

    async def do_modules(self, message):
        """Handle a modules message."""
        modules = list(sys.modules.values())
        startModule = message.get("startModule", 0)
        moduleCount = message.get("moduleCount", len(modules))
        mods = []
        for i in range(startModule, moduleCount):
            module = modules[i]
            filename = getattr(getattr(module, "__spec__", None), "origin", None)
            if filename and filename.endswith(".py"):
                mods.append({"id": i, "name": module.__name__, "path": filename})
        return {"body": {"modules": mods, "totalModules": len(modules)}}

    async def do_copy_to_globals(self, message):
        dst_var_name = message["arguments"]["dstVariableName"]
        src_var_name = message["arguments"]["srcVariableName"]
        src_frame_id = message["arguments"]["srcFrameId"]
        expression = f"globals()['{dst_var_name}']"
        seq = message["seq"]
        return await self._forward_message(
            {
                "type": "request",
                "command": "setExpression",
                "seq": seq + 1,
                "arguments": {
                    "expression": expression,
                    "value": src_var_name,
                    "frameId": src_frame_id,
                },
            }
        )

    # Started handlers (requires debug_client connection)

    async def do_dump_cell(self, message):
        """Handle a dump cell message."""
        code = message["arguments"]["code"]
        file_name = get_file_name(code)

        with open(file_name, "w", encoding="utf-8") as f:
            f.write(code)

        return {
            "type": "response",
            "request_seq": message["seq"],
            "success": True,
            "command": message["command"],
            "body": {"sourcePath": file_name},
        }

    async def do_set_breakpoints(self, message):
        """Handle a set breakpoints message."""
        source = message["arguments"]["source"]["path"]
        self.breakpoint_list[source] = message["arguments"]["breakpoints"]
        message_response = await self._forward_message(message)
        # debugpy can set breakpoints on different lines than the ones requested,
        # so we want to record the breakpoints that were actually added
        if message_response.get("success"):
            self.breakpoint_list[source] = [
                {"line": breakpoint["line"]} for breakpoint in message_response["body"]["breakpoints"]
            ]
        return message_response

    async def do_source(self, message):
        """Handle a source message."""
        reply = {"type": "response", "request_seq": message["seq"], "command": message["command"]}
        source_path = message["arguments"]["source"]["path"]
        if Path(source_path).is_file():
            with open(source_path, encoding="utf-8") as f:
                reply["success"] = True
                reply["body"] = {"content": f.read()}
        else:
            reply["success"] = False
            reply["message"] = "source unavailable"
            reply["body"] = {}

        return reply

    async def do_stack_trace(self, message):
        """Handle a stack trace message."""
        reply = await self._forward_message(message)
        # The stackFrames array can have the following content:
        # { frames from the notebook}
        # ...
        # { 'id': xxx, 'name': '<module>', ... } <= this is the first frame of the code from the notebook
        # { frames from async_kernel }
        # ...
        # {'id': yyy, 'name': '<module>', ... } <= this is the first frame of async_kernel code
        # or only the frames from the notebook.
        # We want to remove all the frames from async_kernel when they are present.
        try:
            sf_list = reply["body"]["stackFrames"]
            module_idx = len(sf_list) - next(
                i for i, v in enumerate(reversed(sf_list), 1) if v["name"] == "<module>" and i != 1
            )
            reply["body"]["stackFrames"] = reply["body"]["stackFrames"][: module_idx + 1]
        except StopIteration:
            pass
        return reply

    async def do_variables(self, message):
        """Handle a variables message."""
        reply = {}
        if not self.stopped_threads:
            variables = self.variable_explorer.get_children_variables(message["arguments"]["variablesReference"])
            return self._build_variables_response(message, variables)

        reply = await self._forward_message(message)
        # TODO : check start and count arguments work as expected in debugpy
        reply["body"]["variables"] = [var for var in reply["body"]["variables"] if self._accept_variable(var["name"])]
        return reply

    async def do_attach(self, message):
        """Handle an attach message."""

        message["arguments"]["connect"] = self.debugpy_client.get_host_port()
        if self.just_my_code:
            message["arguments"]["debugOptions"] = ["justMyCode"]
        await self.debugpy_client._send_request(message)
        with anyio.move_on_after(10):
            await self.init_event.wait()
        await self._forward_message(
            {
                "type": "request",
                "seq": self.next_seq(),
                "command": "configurationDone",
            }
        )
        return await self.debugpy_client._wait_for_response(message)

    async def do_configuration_done(self, message):
        """Handle a configuration done message."""
        # This is only supposed to be called during initialize but can come at anytime. Ref: https://microsoft.github.io/debug-adapter-protocol/specification#Events_Initialized
        return {
            "seq": message["seq"],
            "type": "response",
            "request_seq": message["seq"],
            "success": True,
            "command": message["command"],
        }

    async def do_disconnect(self, message):
        response = await self._forward_message(message)
        # Restore the leading whitespace remove transform.
        cleanup_transforms = self.kernel.shell.input_transformer_manager.cleanup_transforms
        for index in sorted(self._removed_cleanup):
            func = self._removed_cleanup.pop(index)
            cleanup_transforms.insert(index, func)
        self.init_event = anyio.Event()
        self.breakpoint_list = {}
        return response
