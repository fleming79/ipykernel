"""Debugger implementation for the IPython kernel."""
from __future__ import annotations

import logging
import os
import re
import sys
import threading
import typing as t
from pathlib import Path
from typing import TYPE_CHECKING

import anyio.from_thread
import orjson
import traitlets
from IPython.core.inputtransformer2 import leading_empty_lines
from jupyter_client.jsonutil import json_default

from async_kernel import utils
from async_kernel.compiler import get_file_name, get_tmp_directory, get_tmp_hash_seed

if TYPE_CHECKING:
    from anyio.abc import TaskGroup, TaskStatus

    from async_kernel import Kernel
try:
    if "PYDEVD_IPYTHON_COMPATIBLE_DEBUGGING" not in os.environ:
        os.environ["PYDEVD_IPYTHON_COMPATIBLE_DEBUGGING"] = "1"

    # This import is required to have the next ones working...
    from debugpy.server import api  # noqa: F401

    from _pydevd_bundle import pydevd_frame_utils  # isort: skip
    from _pydevd_bundle.pydevd_suspended_frames import (  # isort: skip
        SuspendedFramesManager,
        _FramesTracker,
    )


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
        from _pydevd_bundle.pydevd_api import PyDevdAPI

        self.variable_presentation = PyDevdAPI.VariablePresentation()


class VariableExplorer(traitlets.HasTraits):
    """A variable explorer."""

    kernel: traitlets.Instance[Kernel] = traitlets.Instance("async_kernel.Kernel", ())

    def __init__(self):
        """Initialize the explorer."""
        self.suspended_frame_manager = SuspendedFramesManager()
        self.py_db = _DummyPyDB()
        self.tracker = _FramesTracker(self.suspended_frame_manager, self.py_db)
        self.frame = None

    def track(self):
        """Start tracking."""
        var = self.kernel.user_ns
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


class DebugpyMessageQueue:
    """A debugpy message queue."""

    HEADER = b"Content-Length: "
    SEPARATOR = b"\r\n\r\n"
    SEPARATOR_LENGTH = 4

    def __init__(self, event_callback, log):
        """Init the queue."""
        self.tcp_buffer = b""
        self.event_callback = event_callback
        self.send_stream, self.receive_stream = anyio.create_memory_object_stream()
        self.log = log

    def _put_message(self, raw_msg: bytes):
        msg: dict[str, t.Any] = orjson.loads(raw_msg)
        self.log.debug("_put_message :%s %s", msg["type"], msg)
        if msg["type"] == "event":
            self.event_callback(msg)
        else:
            self.send_stream.send_nowait(msg)

    def put_tcp_frame(self, frame: bytes):
        """Put a tcp frame in the queue."""
        self.tcp_buffer += frame
        data = self.tcp_buffer.split(self.HEADER)
        if len(data) > 1:
            for buf in data[1:]:
                size, raw_msg = buf.split(self.SEPARATOR, maxsplit=1)
                size = int(size)
                if len(raw_msg) >= size:
                    self._put_message(raw_msg[:size])
                else:
                    self.tcp_buffer = self.HEADER + buf
                    return
            self.tcp_buffer = b""


    async def get_message(self):
        """Get a message from the queue."""
        return await self.receive_stream.receive()


class DebugpyClient(traitlets.HasTraits):
    """A client for debugpy."""

    capabilities = traitlets.Dict()
    kernel: traitlets.Instance[Kernel] = traitlets.Instance("async_kernel.Kernel", ())
    initialize_reply = traitlets.Dict()
    init_event_seq = traitlets.Int(-1)
    _connected = traitlets.Bool()
    wait_for_attach = traitlets.Bool(True)
    _seq = 0

    def __init__(self, log, event_callback):
        """Initialize the client."""
        self.log = log
        self.event_callback = event_callback
        self.message_queue = DebugpyMessageQueue(self._forward_event, self.log)
        self.init_event = anyio.Event()

    async def start(self, task_status: TaskStatus):
        def start_debugpy():
            import debugpy
            return debugpy.listen(0)

        self._host_port = anyio.from_thread.run_sync(start_debugpy)
        async with await anyio.connect_tcp(*self._host_port) as socketstream:
            self.socketstream = socketstream
            thread = threading.current_thread()
            # This thread can't be stopped by the debugger when debugging
            thread.pydev_do_not_trace = True  # type: ignore[attr-defined]
            thread.is_pydev_daemon_thread = True  # type: ignore[attr-defined]
            task_status.started()
            await anyio.sleep_forever()

    def next_seq(self):
        "A monotonically decreasing negative number so as not to clash with the frontend seq."
        self._seq = self._seq - 1
        return self._seq

    def _forward_event(self, msg):
        if msg["event"] == "initialized":
            self.init_event.set()
            self.init_event_seq = msg["seq"]
        self.event_callback(msg)

    async def _send_request(self, msg):
        content = orjson.dumps(msg, default=json_default)
        content_length = str(len(content)).encode()
        buf = DebugpyMessageQueue.HEADER + content_length + DebugpyMessageQueue.SEPARATOR
        buf += content
        self.log.debug("DEBUGPYCLIENT: request %s", buf)
        await self.socketstream.send(buf)

    async def _wait_for_response(self):
        # Since events are never pushed to the message_queue
        # we can safely assume the next message in queue
        # will be an answer to the previous request
        return await self.message_queue.get_message()

    async def _handle_init_sequence(self):
        # 1] Waits for initialized event
        await self.init_event.wait()

        # 2] Sends configurationDone request
        configurationDone = {
            "type": "request",
            "seq": self.next_seq(),
            "command": "configurationDone",
        }
        await self._send_request(configurationDone)

        # 3]  Waits for configurationDone response
        await self._wait_for_response()

        # 4] Waits for attachResponse and returns it
        return await self._wait_for_response()

    def get_host_port(self):
        """Get the host debugpy port."""
        return self._host_port

    async def connect_tcp_socket(self, *, task_status: TaskStatus):
        """Connect to the tcp socket."""
        self._connected = True
        task_status.started()
        try:
            while True:
                data = await self.socketstream.receive()
                self.message_queue.put_tcp_frame(data)
        except anyio.EndOfStream:
            return
        finally:
            self._connected = False

    def disconnect_tcp_socket(self):
        """Disconnect from the tcp socket."""
        self._connected = False
        self.init_event = anyio.Event()
        self.wait_for_attach = True

    def receive_dap_frame(self, frame):
        """Receive a dap frame."""
        self.message_queue.put_tcp_frame(frame)

    async def send_dap_request(self, msg):
        """Send a dap request."""
        if msg["command"] == "initialize":
            if self.initialize_reply:
                return self.initialize_reply | {"request_seq": msg["seq"]}
            self.init_event_seq = msg["seq"]
        await self._send_request(msg)
        if self.wait_for_attach and msg["command"] == "attach":
            rep = await self._handle_init_sequence()
            self.wait_for_attach = False
            return rep
        reply = await self._wait_for_response()
        if msg["command"] == "initialize":
            self.initialize_reply = reply
        return reply


class Debugger(traitlets.HasTraits):
    """The debugger class."""

    breakpoint_list = traitlets.Dict()
    stopped_threads = traitlets.Set()
    _removed_cleanup = traitlets.Dict()
    is_started = traitlets.Bool()
    just_my_code = traitlets.Bool()
    debugpy_initialized = traitlets.Bool()
    variable_explorer = traitlets.Instance(VariableExplorer, ())
    debugpy_client = traitlets.Instance(DebugpyClient)
    log = traitlets.Instance(logging.LoggerAdapter)
    kernel: Kernel
    taskgroup: TaskGroup
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
            "dumpCell": self.dumpCell,
            "setBreakpoints": self.setBreakpoints,
            "source": self.source,
            "stackTrace": self.stackTrace,
            "variables": self.variables,
            "attach": self.attach,
            "configurationDone": self.configurationDone,
        }
        self.static_debug_handlers = {
            "debugInfo": self.debugInfo,
            "inspectVariables": self.inspectVariables,
            "richInspectVariables": self.richInspectVariables,
            "modules": self.modules,
            "copyToGlobals": self.copyToGlobals,
        }

    async def main_start(self, kernel: Kernel, *, task_status: TaskStatus):
        # To be called by `kernel.start_in_context`.
        self.kernel = kernel
        async with anyio.create_task_group() as tg:
            self.taskgroup = tg
            task_status.started()
            await anyio.sleep_forever()

    async def _forward_message(self, msg):
        return await self.debugpy_client.send_dap_request(msg)

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
        elif msg["event"] == "terminated":
            self._on_disconnect()
        self._publish_event(msg)

    def _publish_event(self, event: dict):
        self.kernel.iopub_send(
            msg_or_type="debug_event",
            content=event,
            ident=self.kernel._topic("debug_event"),
        )

    def _build_variables_response(self, request, variables):
        var_list = [var for var in variables if self.accept_variable(var["name"])]
        return {
            "seq": request["seq"],
            "type": "response",
            "request_seq": request["seq"],
            "success": True,
            "command": request["command"],
            "body": {"variables": var_list},
        }

    def _accept_stopped_thread(self, thread_name):
        # TODO: identify Thread-2, Thread-3 and Thread-4. These are NOT
        # Control, IOPub or Heartbeat threads
        forbid_list = ["IPythonHistorySavingThread", "Thread-2", "Thread-3", "Thread-4"]
        return thread_name not in forbid_list

    async def handle_stopped_event(self, event):
        """Handle a stopped event."""
        req = {"seq": self.debugpy_client.next_seq(), "type": "request", "command": "threads"}
        rep = await self._forward_message(req)
        for thread in rep["body"]["threads"]:
            if self._accept_stopped_thread(thread["name"]):
                self.stopped_threads.add(thread["id"])
            self._publish_event(event)

    async def start(self):
        """Start the debugger."""
        if not self.debugpy_initialized:
            await self.taskgroup.start(self.debugpy_client.start)
            await self.taskgroup.start(self.debugpy_client.connect_tcp_socket)
            tmp_dir = get_tmp_directory()
            if not Path(tmp_dir).exists():
                Path(tmp_dir).mkdir(parents=True)
            self.debugpy_initialized = True
        elif not self.debugpy_client._connected:
            await self.taskgroup.start(self.debugpy_client.connect_tcp_socket)

        # Don't remove leading empty lines when debugging so the breakpoints are correctly positioned
        cleanup_transforms = self.kernel.shell.input_transformer_manager.cleanup_transforms
        if leading_empty_lines in cleanup_transforms:
            index = cleanup_transforms.index(leading_empty_lines)
            self._removed_cleanup[index] = cleanup_transforms.pop(index)

        # self.debugpy_client.connect_tcp_socket()
        self.is_started = True
        return self.debugpy_initialized

    def stop(self):
        """Stop the debugger."""
        self.debugpy_client.disconnect_tcp_socket()

        # Restore remove cleanup transformers
        cleanup_transforms = self.kernel.shell.input_transformer_manager.cleanup_transforms
        for index in sorted(self._removed_cleanup):
            func = self._removed_cleanup.pop(index)
            cleanup_transforms.insert(index, func)

    async def dumpCell(self, message):
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

    async def setBreakpoints(self, message):
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

    async def source(self, message):
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

    async def stackTrace(self, message):
        """Handle a stack trace message."""
        reply = await self._forward_message(message)
        # The stackFrames array can have the following content:
        # { frames from the notebook}
        # ...
        # { 'id': xxx, 'name': '<module>', ... } <= this is the first frame of the code from the notebook
        # { frames from ipykernel }
        # ...
        # {'id': yyy, 'name': '<module>', ... } <= this is the first frame of ipykernel code
        # or only the frames from the notebook.
        # We want to remove all the frames from ipykernel when they are present.
        try:
            sf_list = reply["body"]["stackFrames"]
            module_idx = len(sf_list) - next(
                i for i, v in enumerate(reversed(sf_list), 1) if v["name"] == "<module>" and i != 1
            )
            reply["body"]["stackFrames"] = reply["body"]["stackFrames"][: module_idx + 1]
        except StopIteration:
            pass
        return reply

    def accept_variable(self, variable_name):
        """Accept a variable by name."""
        return (
            variable_name not in self.forbidden_names
            and not bool(re.search(r"^_\d", variable_name))
            and not variable_name.startswith("_i")
        )

    async def variables(self, message):
        """Handle a variables message."""
        reply = {}
        if not self.stopped_threads:
            variables = self.variable_explorer.get_children_variables(message["arguments"]["variablesReference"])
            return self._build_variables_response(message, variables)

        reply = await self._forward_message(message)
        # TODO : check start and count arguments work as expected in debugpy
        reply["body"]["variables"] = [var for var in reply["body"]["variables"] if self.accept_variable(var["name"])]
        return reply

    async def attach(self, message):
        """Handle an attach message."""
        host, port = self.debugpy_client.get_host_port()
        message["arguments"]["connect"] = {"host": host, "port": port}
        message["arguments"]["logToFile"] = True
        # Experimental option to break in non-user code.
        # The ipykernel source is in the call stack, so the user
        # has to manipulate the step-over and step-into in a wize way.
        # Set debugOptions for breakpoints in python standard library source.
        if not self.just_my_code:
            message["arguments"]["debugOptions"] = ["DebugStdLib"]
        return await self._forward_message(message)

    async def configurationDone(self, message):
        """Handle a configuration done message."""
        # This is only supposed to be called during initialize but can come at anytime. Ref: https://microsoft.github.io/debug-adapter-protocol/specification#Events_Initialized
        return {
            "seq": message["seq"],
            "type": "response",
            "request_seq": message["seq"],
            "success": True,
            "command": message["command"],
        }

    async def debugInfo(self, message):
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
                "isStarted": self.is_started,
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

    async def inspectVariables(self, message):
        """Handle an inspect variables message."""
        self.variable_explorer.untrack_all()
        # looks like the implementation of untrack_all in ptvsd
        # destroys objects we nee din track. We have no choice but
        # reinstantiate the object
        self.variable_explorer = VariableExplorer()
        self.variable_explorer.track()
        variables = self.variable_explorer.get_children_variables()
        return self._build_variables_response(message, variables)

    async def richInspectVariables(self, message):
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
            reply = await self._forward_message({
                "type": "request",
                "command": "evaluate",
                "seq": self.debugpy_client.next_seq(),
                "arguments": {"expression": code, "frameId": frame_id, "context": "clipboard"},
            })
            if reply["success"]:
                repr_data, repr_metadata = eval(reply["body"]["result"], {}, {})
        body = {
            "data": repr_data,
            "metadata": {k: v for k, v in repr_metadata.items() if k in repr_data},
        }
        reply["body"] = body
        reply["success"] = True
        return reply

    async def copyToGlobals(self, message):
        dst_var_name = message["arguments"]["dstVariableName"]
        src_var_name = message["arguments"]["srcVariableName"]
        src_frame_id = message["arguments"]["srcFrameId"]
        expression = f"globals()['{dst_var_name}']"
        seq = message["seq"]
        return await self._forward_message({
            "type": "request",
            "command": "setExpression",
            "seq": seq + 1,
            "arguments": {
                "expression": expression,
                "value": src_var_name,
                "frameId": src_frame_id,
            },
        })

    async def modules(self, message):
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

    async def process_request(self, message: dict[str, t.Any]):
        """Process a request."""
        reply = {}
        if message["command"] == "initialize" and not self.is_started:
            await self.start()
        if handler := self.static_debug_handlers.get(message["command"]):
            return await handler(message)
        if self.is_started:
            if handler := self.started_debug_handlers.get(message["command"]):
                return await handler(message)
            return await self._forward_message(message)
        if message["command"] == "disconnect":
            self.stop()
            self.breakpoint_list = {}
            self.stopped_threads = set()
            self.is_started = False
            self.log.info("The debugger has stopped")
        return reply
