# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from IPython.core.displayhook import DisplayHook
from IPython.core.displaypub import DisplayPublisher
from IPython.core.error import StdinNotImplementedError
from IPython.core.interactiveshell import InteractiveShell, InteractiveShellABC
from IPython.core.usage import default_banner
from jupyter_client.session import extract_header
from traitlets import CBool, CBytes, Dict, Instance, Type, default, observe
from typing_extensions import override

from async_kernel.compiler import XCachingCompiler

if TYPE_CHECKING:
    from async_kernel.kernel import Kernel

class AsyncDisplayHook(DisplayHook):
    """A displayhook subclass that publishes data using ZeroMQ. This is intended
    to work with an InteractiveShell instance. It sends a dict of different
    representations of the object."""

    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", ())
    content: Dict[str, Any] = Dict()

    def set_job(self, job):
        """Set the parent for outbound messages."""
        self.job = job

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
            self.kernel.pubio_send("display_data", content=self.content)
            self.content = {}


class AsyncDisplayPublisher(DisplayPublisher):
    """A display publisher that publishes data using a ZeroMQ PUB socket."""

    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", ())
    topic = CBytes(b"display_data")

    def set_parent(self, parent):
        """Set the parent for outbound messages."""
        self.parent_header = extract_header(parent)

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
        self.kernel.pubio_send(
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
        self.kernel.pubio_send(msg_or_type="clear_output", content={"wait": wait}, ident=self.topic)


class AsyncInteractiveShell(InteractiveShell):
    """A subclass of InteractiveShell for ZMQ."""

    displayhook_class = Type(AsyncDisplayHook)
    display_pub_class = Type(AsyncDisplayPublisher)
    displayhook: Instance[AsyncDisplayHook]
    display_pub: Instance[AsyncDisplayPublisher]
    kernel: Instance[Kernel] = Instance("async_kernel.Kernel", ())
    compiler_class = Type(XCachingCompiler)

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
    def run_cell(self, *args, **kwargs):
        """Run a cell."""
        self._last_traceback = None
        return super().run_cell(*args, **kwargs)

    @override
    def _showtraceback(self, etype, evalue, stb):
        # For Keyboard interrupt, remove the kernel source code from the
        # traceback.
        ename = str(etype.__name__)
        if ename == "KeyboardInterrupt":
            stb.pop(-2)
        self.kernel.pubio_send(msg_or_type="error", content={"traceback": stb, "ename": ename, "evalue": str(evalue)})
        # store the formatted traceback
        self._last_traceback = stb


InteractiveShellABC.register(AsyncInteractiveShell)
