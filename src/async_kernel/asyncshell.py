# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import builtins
import json
import pathlib
import sys
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import IPython.core.release
from IPython.core.displayhook import DisplayHook
from IPython.core.displaypub import DisplayPublisher
from IPython.core.interactiveshell import ExecutionResult, InteractiveShell, InteractiveShellABC
from IPython.core.magic import Magics, line_magic, magics_class
from jupyter_client.jsonutil import json_default
from jupyter_core.paths import jupyter_runtime_dir
from traitlets import CBool, Dict, Instance, Type, default, observe
from typing_extensions import override

import async_kernel
from async_kernel.caller import Caller
from async_kernel.compiler import XCachingCompiler

if TYPE_CHECKING:
    from async_kernel.kernel import Kernel


__all__ = ["AsyncDisplayHook", "AsyncDisplayPublisher", "AsyncInteractiveShell"]


class AsyncDisplayHook(DisplayHook):
    """A displayhook subclass that publishes data using ZeroMQ. This is intended
    to work with an InteractiveShell instance. It sends a dict of different
    representations of the object."""

    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", ())
    content: Dict[str, Any] = Dict()

    @property
    @override
    def prompt_count(self):
        return self.kernel.execution_count

    @override
    def start_displayhook(self):
        """Start the display hook."""
        self.content = {}

    @override
    def write_output_prompt(self):
        """Write the output prompt."""
        self.content["execution_count"] = self.prompt_count

    @override
    def write_format_data(self, format_dict, md_dict=None):
        """Write format data to the message."""
        self.content["data"] = format_dict
        self.content["metadata"] = md_dict

    @override
    def finish_displayhook(self):
        """Finish up all displayhook activities."""
        if self.content:
            self.kernel.iopub_send("display_data", content=self.content)
            self.content = {}


class AsyncDisplayPublisher(DisplayPublisher):
    """A display publisher that publishes data using a ZeroMQ PUB socket."""

    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", ())
    topic: ClassVar = b"display_data"

    @override
    def publish(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        data,
        metadata=None,
        *,
        transient=None,
        update=False,
        **kwargs,
    ) -> None:
        """Publish a display-data message.

        Args:
            data: A mime-bundle dict, keyed by mime-type.
            metadata: Metadata associated with the data.
            transient: Transient data that may only be relevant during a live display, such as display_id.
                Transient data should not be persisted to documents.
            update: If True, send an update_display_data message instead of display_data.

        [Reference](https://jupyter-client.readthedocs.io/en/stable/messaging.html#update-display-data)
        """
        self.kernel.iopub_send(
            msg_or_type="update_display_data" if update else "display_data",
            content={"data": data, "metadata": metadata or {}, "transient": transient or {}} | kwargs,
            ident=self.topic,
        )

    @override
    def clear_output(self, wait=False):
        """Clear output associated with the current execution (cell).

        Args:
            wait: If True, the output will not be cleared immediately,
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
    user_ns_hidden = Dict()
    _main_mod_cache = Dict()

    @default("banner1")
    def _default_banner1(self):
        return (
            f"Python {sys.version}\n"
            f"Async kernel ({self.kernel.kernel_name})\n"
            f"IPython shell {IPython.core.release.version}\n"
        )

    # Override the traitlet in the parent class, because there's no point using
    # readline for the kernel. Can be removed when the readline code is moved
    # to the terminal frontend.
    readline_use = CBool(False)
    # autoindent has no meaning in a zmqshell, and attempting to enable it
    # will print a warning in the absence of readline.
    autoindent = CBool(False)

    @observe("exit_now")
    def _update_exit_now(self, _):
        """stop eventloop when exit_now fires"""
        if self.exit_now:
            self.kernel.stop()

    def ask_exit(self):
        if self.kernel.raw_input("Are you sure you want to stop the kernel?\ny/[n]\n") == "y":
            self.exit_now = True

    @override
    def init_create_namespaces(self, user_module=None, user_ns=None):
        return

    @override
    def save_sys_module_state(self):
        return

    @override
    def init_sys_modules(self):
        return

    @property
    @override
    def execution_count(self):
        return self.kernel.execution_count

    @execution_count.setter
    def execution_count(self, value):
        return

    @property
    @override
    def user_ns(self):
        if not hasattr(self, "_user_ns"):
            self.user_ns = {}
        return self._user_ns

    @user_ns.setter
    def user_ns(self, ns: dict):
        assert hasattr(ns, "clear")
        assert isinstance(ns, dict)
        self._user_ns = ns
        self.init_user_ns()

    @property
    @override
    def user_global_ns(self):
        return self.user_ns

    @property
    @override
    def ns_table(self):
        return {"user_global": self.user_ns, "user_local": self.user_ns, "builtin": builtins.__dict__}

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
        result = await super().run_cell_async(
            raw_cell=raw_cell,
            store_history=store_history,
            silent=silent,
            shell_futures=shell_futures,
            transformed_cell=transformed_cell,
            preprocessing_exc_tuple=preprocessing_exc_tuple,
            cell_id=cell_id,
        )
        self.events.trigger("post_execute")
        if not silent:
            self.events.trigger("post_run_cell", result)
        return result

    @override
    def _showtraceback(self, etype, evalue, stb):
        self.kernel.iopub_send(
            msg_or_type="error",
            content={"traceback": stb, "ename": str(etype.__name__), "evalue": str(evalue)},
        )

    @override
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
    def connect_info(self, _):
        """Print information for connecting other clients to this kernel."""

        kernel = async_kernel.Kernel()
        connection_file = pathlib.Path(kernel.connection_file)

        # if it's in the default dir, truncate to basename
        if jupyter_runtime_dir() == str(connection_file.parent):
            connection_file = connection_file.name

        info = kernel.get_connection_info()
        print(
            json.dumps(info, indent=2, default=json_default),
            "Paste the above JSON into a file, and connect with:\n"
            + "    $> jupyter <app> --existing <file>\n"
            + "or, if you are local, you can connect with just:\n"
            + f"    $> jupyter <app> --existing {connection_file}\n"
            + "or even just:\n"
            + "    $> jupyter <app> --existing\n"
            + "if this is the most recent Jupyter kernel you have started.",
        )

    @line_magic
    def callers(self, _):
        print("Active", "Protected", "\t", "Name")
        print("─" * 70)
        for caller in Caller.all_callers(active_only=False):
            symbol = "   ✓" if caller.active else "   ✗"
            current_thread: Literal["← current thread", ""] = "← current thread" if caller is Caller() else ""
            protected = "   🔐" if caller.protected else ""
            print(symbol, protected, "", caller.thread.name, current_thread, sep="\t")


InteractiveShellABC.register(AsyncInteractiveShell)
