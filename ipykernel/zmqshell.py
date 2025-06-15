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
import sys
import threading
from typing import TYPE_CHECKING

from IPython.core.autocall import ZMQExitAutocall
from IPython.core.displaypub import DisplayPublisher
from IPython.core.interactiveshell import InteractiveShell, InteractiveShellABC
from IPython.core.usage import default_banner
from jupyter_client.session import Session, extract_header
from traitlets import Any, CBool, CBytes, Dict, Instance, Type, default, observe

from ipykernel.displayhook import ZMQShellDisplayHook

if TYPE_CHECKING:
    from ipykernel.kernelbase import Kernel

# -----------------------------------------------------------------------------
# Functions and classes
# -----------------------------------------------------------------------------


class ZMQDisplayPublisher(DisplayPublisher):
    """A display publisher that publishes data using a ZeroMQ PUB socket."""

    session = Instance(Session, allow_none=True)
    pub_socket = Any(allow_none=True)
    parent_header = Dict({})
    topic = CBytes(b"display_data")

    # thread_local:
    # An attribute used to ensure the correct output message
    # is processed. See ipykernel Issue 113 for a discussion.
    _thread_local = Any()

    def set_parent(self, parent):
        """Set the parent for outbound messages."""
        self.parent_header = extract_header(parent)

    def _flush_streams(self):
        """flush IO Streams prior to display"""
        sys.stdout.flush()
        sys.stderr.flush()

    @default("_thread_local")
    def _default_thread_local(self):
        """Initialize our thread local storage"""
        return threading.local()

    @property
    def _hooks(self):
        if not hasattr(self._thread_local, "hooks"):
            # create new list for a new thread
            self._thread_local.hooks = []
        return self._thread_local.hooks

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
        """
        self._flush_streams()
        if metadata is None:
            metadata = {}
        if transient is None:
            transient = {}
        self._validate_data(data, metadata)
        content = {}
        content["data"] = data
        content["metadata"] = metadata
        content["transient"] = transient

        msg_type = "update_display_data" if update else "display_data"

        # Use 2-stage process to send a message,
        # in order to put it through the transform
        # hooks before potentially sending.
        assert self.session is not None
        msg = self.session.msg(msg_type, content, parent=self.parent_header)

        # Each transform either returns a new
        # message or None. If None is returned,
        # the message has been 'used' and we return.
        for hook in self._hooks:
            msg = hook(msg)
            if msg is None:
                return  # type:ignore[unreachable]

        self.session.send(
            self.pub_socket,
            msg,
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
        content = {"wait": wait}
        self._flush_streams()
        assert self.session is not None
        msg = self.session.msg("clear_output", content, parent=self.parent_header)

        # see publish() for details on how this works
        for hook in self._hooks:
            msg = hook(msg)
            if msg is None:
                return  # type:ignore[unreachable]

        self.session.send(
            self.pub_socket,
            msg,
            ident=self.topic,
        )

    def register_hook(self, hook):
        """
        Registers a hook with the thread-local storage.

        Parameters
        ----------
        hook : Any callable object

        Returns
        -------
        Either a publishable message, or `None`.
        The DisplayHook objects must return a message from
        the __call__ method if they still require the
        `session.send` method to be called after transformation.
        Returning `None` will halt that execution path, and
        session.send will not be called.
        """
        self._hooks.append(hook)

    def unregister_hook(self, hook):
        """
        Un-registers a hook with the thread-local storage.

        Parameters
        ----------
        hook : Any callable object which has previously been
            registered as a hook.

        Returns
        -------
        bool - `True` if the hook was removed, `False` if it wasn't
            found.
        """
        try:
            self._hooks.remove(hook)
            return True
        except ValueError:
            return False


class ZMQInteractiveShell(InteractiveShell):
    """A subclass of InteractiveShell for ZMQ."""

    displayhook_class = Type(ZMQShellDisplayHook)
    display_pub_class = Type(ZMQDisplayPublisher)
    displayhook: Instance[ZMQShellDisplayHook]
    display_pub: Instance[ZMQDisplayPublisher]
    # data_pub_class = Any()  # type:ignore[assignment]
    kernel: Instance[Kernel] = Instance("ipykernel.kernelbase.Kernel")
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
                kernel.main_kernel.close_subshell(kernel.ident)

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

    # def payloadpage_page(self, strg, start=0, screen_lines=0, pager_cmd=None):
    #     """Print a string, piping through a pager.

    #     This version ignores the screen_lines and pager_cmd arguments and uses
    #     IPython's payload system instead.

    #     Parameters
    #     ----------
    #     strg : str or mime-dict
    #         Text to page, or a mime-type keyed dict of already formatted data.
    #     start : int
    #         Starting line at which to place the display.
    #     """

    #     # Some routines may auto-compute start offsets incorrectly and pass a
    #     # negative value.  Offset to 0 for robustness.
    #     start = max(0, start)

    #     data = strg if isinstance(strg, dict) else {"text/plain": strg}

    #     payload = {"source": "page", "data": data, "start": start}
    #     assert self.payload_manager is not None
    #     self.payload_manager.write_payload(payload)

    # def init_hooks(self):
    #     """Initialize hooks."""
    #     super().init_hooks()
    #     self.set_hook("show_in_pager", page.as_hook(self.payloadpage_page), 99)

    # def ask_exit(self):
    #     """Engage the exit actions."""
    #     self.exit_now = not self.keepkernel_on_exit
    #     payload = {"source": "ask_exit", "keepkernel": self.keepkernel_on_exit}
    #     self.payload_manager.write_payload(payload)  # type:ignore[union-attr]

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

        exc_content = {
            "traceback": stb,
            "ename": ename,
            "evalue": str(evalue),
        }

        dh = self.displayhook
        # Send exception info over pub socket for other clients than the caller
        # to pick up
        topic = None
        if dh.topic:
            topic = dh.topic.replace(b"execute_result", b"error")

        dh.session.send(
            dh.pub_socket,
            "error",
            exc_content,
            dh.parent_header,
            ident=topic,
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
