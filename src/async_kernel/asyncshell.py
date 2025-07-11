# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import json
import pathlib
from typing import TYPE_CHECKING, Any

from IPython.core.displayhook import DisplayHook
from IPython.core.displaypub import DisplayPublisher
from IPython.core.error import StdinNotImplementedError
from IPython.core.interactiveshell import ExecutionResult, InteractiveShell, InteractiveShellABC
from IPython.core.magic import Magics, line_magic, magics_class
from IPython.core.usage import default_banner
from jupyter_client.jsonutil import json_default
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import CBool, CBytes, Dict, Instance, Type, default, observe
from typing_extensions import override

import async_kernel
from async_kernel import utils
from async_kernel.compiler import XCachingCompiler

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from async_kernel.kernel import Kernel
    from async_kernel.utils import P


__all__ = ["AsyncDisplayHook", "AsyncDisplayPublisher", "AsyncInteractiveShell"]


class AsyncDisplayHook(DisplayHook):
    """A displayhook subclass that publishes data using ZeroMQ. This is intended
    to work with an InteractiveShell instance. It sends a dict of different
    representations of the object."""

    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", ())
    content: Dict[str, Any] = Dict()

    def start_displayhook(self):
        """Start the display hook."""
        self.content = {}

    def write_output_prompt(self):
        """Write the output prompt."""
        self.content["execution_count"] = self.prompt_count

    def write_format_data(self, format_dict, md_dict=None):
        """Write format data to the message."""
        self.content["data"] = format_dict
        self.content["metadata"] = md_dict

    def finish_displayhook(self):
        """Finish up all displayhook activities."""
        if self.content:
            self.kernel.iopub_send("display_data", content=self.content)
            self.content = {}


class AsyncDisplayPublisher(DisplayPublisher):
    """A display publisher that publishes data using a ZeroMQ PUB socket."""

    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", ())
    topic = CBytes(b"display_data")

    @override
    def publish(  # type: ignore[override]
        self,
        data,
        metadata=None,
        *,
        transient=None,
        update=False,
        **kwargs,
    ) -> None:
        """Publish a display-data message

        Parameters
        ----------
        data : dict
            A mime-bundle dict, keyed by mime-type.
        metadata : dict, optional
            Metadata associated with the data.
        transient : dict, optional, keyword-only
            Transient data that may only be relevant during a live display,
            such as display_id.
            Transient data should not be persisted to documents.
        update : bool, optional, keyword-only
            If True, send an update_display_data message instead of display_data.

        Ref: https://jupyter-client.readthedocs.io/en/stable/messaging.html#update-display-data
        """
        self.kernel.iopub_send(
            msg_or_type="update_display_data" if update else "display_data",
            content={"data": data, "metadata": metadata or {}, "transient": transient or {}} | kwargs,
            ident=self.topic,
        )

    @override
    def clear_output(self, wait=False):
        """Clear output associated with the current execution (cell).

        Parameters
        ----------
        wait : bool (default: False)
            If True, the output will not be cleared immediately,
            instead waiting for the next display before clearing.
            This reduces bounce during repeated clear & display loops.

        """
        self.kernel.iopub_send(msg_or_type="clear_output", content={"wait": wait}, ident=self.topic)


class AsyncInteractiveShell(InteractiveShell):
    """A subclass of InteractiveShell for ZMQ."""

    displayhook_class = Type(AsyncDisplayHook)
    display_pub_class = Type(AsyncDisplayPublisher)
    displayhook: Instance[AsyncDisplayHook]
    display_pub: Instance[AsyncDisplayPublisher]
    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", ())
    compiler_class = Type(XCachingCompiler)
    compile: Instance[XCachingCompiler]

    @default("banner1")
    def _default_banner1(self):
        return default_banner

    # Override the traitlet in the parent class, because there's no point using
    # readline for the kernel. Can be removed when the readline code is moved
    # to the terminal frontend.
    readline_use = CBool(False)
    # autoindent has no meaning in a zmqshell, and attempting to enable it
    # will print a warning in the absence of readline.
    autoindent = CBool(False)

    def call_later(self, func: Callable[P, Any | Awaitable], delay=0.0, /, *args: P.args, **kwargs: P.kwargs):
        """Schedules a function or coroutine for execution in the current thread."""
        utils.ThreadSafeCaller.get_instance().call_later(func, delay, *args, **kwargs)

    def call_soon(self, func: Callable[P, Any | Awaitable], *args: P.args, **kwargs: P.kwargs):
        """Schedules a function or coroutine for execution in the current thread."""
        utils.ThreadSafeCaller.get_instance().call_later(func, 0, *args, **kwargs)

    @observe("exit_now")
    def _update_exit_now(self, change):
        """stop eventloop when exit_now fires"""
        if self.exit_now:
            self.kernel.stop()

    def ask_exit(self):
        try:
            response = self.kernel.raw_input("Are you sure you want to stop the kernel?\ny/[n]\n")
        except StdinNotImplementedError:
            pass
        else:
            if response != "y":
                return
        self.exit_now = True

    @override
    def init_user_ns(self):
        super().init_user_ns()
        self.user_ns.update({"call_later": self.call_later, "call_soon": self.call_soon})

    @override
    async def run_cell_async(
        self,
        raw_cell: str,
        store_history=False,
        silent=False,
        shell_futures=True,
        *,
        transformed_cell: str | None = None,
        preprocessing_exc_tuple: tuple | None = None,
        cell_id: str | None = None,
    ) -> ExecutionResult:
        if not silent:
            self._last_traceback = None
        result = None
        try:
            result = await super().run_cell_async(
                raw_cell=raw_cell,
                store_history=store_history,
                silent=silent,
                shell_futures=shell_futures,
                transformed_cell=transformed_cell,
                preprocessing_exc_tuple=preprocessing_exc_tuple,
                cell_id=cell_id,
            )
            return result  # noqa: RET504
        finally:
            self.events.trigger("post_execute")
            if not silent:
                self.events.trigger("post_run_cell", result)

    @override
    def _showtraceback(self, etype, evalue, stb):
        # For Keyboard interrupt, remove the kernel source code from the
        # traceback.
        ename = str(etype.__name__)
        if ename == "KernelInterruptError":
            stb.pop(-2)
        self.kernel.iopub_send(msg_or_type="error", content={"traceback": stb, "ename": ename, "evalue": str(evalue)})
        # store the formatted traceback
        self._last_traceback = stb

    def init_magics(self):
        """Initialize magics."""
        super().init_magics()
        self.register_magics(KernelMagics)

    @override
    def enable_gui(self, gui=None):
        pass


@magics_class
class KernelMagics(Magics):
    """Kernel magics."""

    @line_magic
    def connect_info(self, arg_s):
        """Print information for connecting other clients to this kernel."""

        kernel = async_kernel.Kernel()
        connection_file = pathlib.Path(kernel.connection_file)

        # if it's in the default dir, truncate to basename
        if jupyter_runtime_dir() == str(connection_file.parent):
            connection_file = connection_file.name

        info = kernel.get_connection_info()
        print(
            json.dumps(info, indent=2, default=json_default),
            f"Paste the above JSON into a file, and connect with:\n"
            f"    $> jupyter <app> --existing <file>\n"
            f"or, if you are local, you can connect with just:\n"
            f"    $> jupyter <app> --existing {connection_file}\n"
            f"or even just:\n"
            f"    $> jupyter <app> --existing\n"
            f"if this is the most recent Jupyter kernel you have started.",
        )


InteractiveShellABC.register(AsyncInteractiveShell)
