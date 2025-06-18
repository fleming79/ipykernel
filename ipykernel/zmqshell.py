"""A ZMQ-based subclass of InteractiveShell.

This code is meant to ease the refactoring of the base InteractiveShell into
something with a cleaner architecture for 2-process use, without actually
breaking InteractiveShell itself.  So we're doing something a bit ugly, where
we subclass and override what we want to fix.  Once this is working well, we
can go back to the base class and refactor the code for a cleaner inheritance
implementation that doesn't rely on so much monkeypatching.

But this lets us maintain a fully working IPython as we develop the new
machinery.  This should thus be thought of as scaffolding.
"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import os
from typing import TYPE_CHECKING

from IPython.core.autocall import ZMQExitAutocall
from IPython.core.displaypub import DisplayPublisher
from IPython.core.interactiveshell import InteractiveShell, InteractiveShellABC
from IPython.core.usage import default_banner
from jupyter_client.session import extract_header
from traitlets import CBool, CBytes, Dict, Instance, Type, default, observe

from ipykernel.displayhook import ZMQShellDisplayHook

if TYPE_CHECKING:
    from ipykernel.kernelapp import MainKernel, Subkernel


# -----------------------------------------------------------------------------
# Functions and classes
# -----------------------------------------------------------------------------


class ZMQDisplayPublisher(DisplayPublisher):
    """A display publisher that publishes data using a ZeroMQ PUB socket."""

    main_kernel: Instance[MainKernel] = Instance("ipykernel.kernelapp.MainKernel", ())
    parent_header = Dict({})
    topic = CBytes(b"display_data")

    def set_parent(self, parent):
        """Set the parent for outbound messages."""
        self.parent_header = extract_header(parent)

    # Feb: 2025 IPython has a deprecated, `source` parameter, marked for removal that
    # triggers typing errors.
    def publish(  # type: ignore [override]
        self,
        data,
        metadata=None,
        *,
        transient=None,
        update=False,
        **kwargs,
    ):
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
        self.main_kernel.pubio_send(
            msg_or_type="update_display_data" if update else "display_data",
            content={"data": data, "metadata": metadata or {}, "transient": transient or {}} | kwargs,
            parent=self.parent_header,
            ident=self.topic,
        )

    def clear_output(self, wait=False):
        """Clear output associated with the current execution (cell).

        Parameters
        ----------
        wait : bool (default: False)
            If True, the output will not be cleared immediately,
            instead waiting for the next display before clearing.
            This reduces bounce during repeated clear & display loops.

        """
        self.main_kernel.pubio_send(
            msg_or_type="clear_output",
            content={"wait": wait},
            parent=self.parent_header,
            ident=self.topic,
        )


class ZMQInteractiveShell(InteractiveShell):
    """A subclass of InteractiveShell for ZMQ."""

    displayhook_class = Type(ZMQShellDisplayHook)
    display_pub_class = Type(ZMQDisplayPublisher)
    displayhook: Instance[ZMQShellDisplayHook]
    display_pub: Instance[ZMQDisplayPublisher]
    # data_pub_class = Any()  # type:ignore[assignment]
    kernel: Instance[Subkernel] = Instance("ipykernel.kernelapp.Subkernel")
    parent_header = Dict()

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

    exiter = Instance(ZMQExitAutocall)

    @default("exiter")
    def _default_exiter(self):
        return ZMQExitAutocall(self)

    @observe("exit_now")
    def _update_exit_now(self, change):
        """stop eventloop when exit_now fires"""
        if change["new"]:
            kernel = self.kernel
            if kernel is kernel.main_kernel:
                kernel.main_kernel.stop()
            else:
                kernel.main_kernel.close_subshell(kernel.ident)  # TODO

    keepkernel_on_exit = None

    def init_environment(self):
        """Configure the user's environment."""
        env = os.environ
        # These two ensure 'ls' produces nice coloring on BSD-derived systems
        env["TERM"] = "xterm-color"
        env["CLICOLOR"] = "1"
        # These two add terminal color in tools that support it.
        env["FORCE_COLOR"] = "1"
        env["CLICOLOR_FORCE"] = "1"
        # Since normal pagers don't work at all (over pexpect we don't have
        # single-key control of the subprocess), try to disable paging in
        # subprocesses as much as possible.
        env["PAGER"] = "cat"
        env["GIT_PAGER"] = "cat"

    def ask_exit(self):
        self.exit_now = True

    def run_cell(self, *args, **kwargs):
        """Run a cell."""
        self._last_traceback = None
        return super().run_cell(*args, **kwargs)

    def _showtraceback(self, etype, evalue, stb):
        # For Keyboard interrupt, remove the kernel source code from the
        # traceback.
        ename = str(etype.__name__)
        if ename == "KeyboardInterrupt":
            stb.pop(-2)
        self.kernel.main_kernel.pubio_send(
            msg_or_type="error",
            content={"traceback": stb, "ename": ename, "evalue": str(evalue)},
            parent=self.parent_header,
        )
        # store the formatted traceback
        self._last_traceback = stb

    def set_parent(self, parent):
        """Set the parent header for associating output with its triggering input"""
        self.parent_header = parent
        self.displayhook.set_parent(parent)
        self.display_pub.set_parent(parent)

    def get_parent(self):
        """Get the parent header."""
        return self.parent_header


InteractiveShellABC.register(ZMQInteractiveShell)
