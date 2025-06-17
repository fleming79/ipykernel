"""Replacements for sys.displayhook that publish over ZMQ."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import typing as t

from IPython.core.displayhook import DisplayHook
from jupyter_client.session import extract_header
from traitlets import Dict, Instance

if t.TYPE_CHECKING:
    from ipykernel.kernelapp import MainKernel


class ZMQShellDisplayHook(DisplayHook):
    """A displayhook subclass that publishes data using ZeroMQ. This is intended
    to work with an InteractiveShell instance. It sends a dict of different
    representations of the object."""

    main_kernel: Instance[MainKernel] = Instance("ipykernel.kernelapp.MainKernel", ())
    parent_header = Dict()
    msg: dict[str, t.Any] | None = None

    def set_parent(self, parent):
        """Set the parent for outbound messages."""
        self.parent_header = extract_header(parent)

    def start_displayhook(self):
        """Start the display hook."""
        self.msg = self.main_kernel.session.msg(
            msg_type="execute_result",
            content={
                "data": {},
                "metadata": {},
            },
            parent=self.parent_header,
        )

    def write_output_prompt(self):
        """Write the output prompt."""
        if self.msg:
            self.msg["content"]["execution_count"] = self.prompt_count

    def write_format_data(self, format_dict, md_dict=None):
        """Write format data to the message."""
        if self.msg:
            self.msg["content"]["data"] = format_dict
            self.msg["content"]["metadata"] = md_dict

    def finish_displayhook(self):
        """Finish up all displayhook activities."""
        if self.msg and self.msg["content"]["data"]:
            self.main_kernel.pubio_send(self.msg, parent=self.parent_header)
        self.msg = None
