"""Replacements for sys.displayhook that publish over ZMQ."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import typing as t

from IPython.core.displayhook import DisplayHook
from traitlets import Dict, Instance

if t.TYPE_CHECKING:
    from asynckernel.kernel import Kernel


class ZMQShellDisplayHook(DisplayHook):
    """A displayhook subclass that publishes data using ZeroMQ. This is intended
    to work with an InteractiveShell instance. It sends a dict of different
    representations of the object."""

    kernel: Instance[Kernel] = Instance("asynckernel.Kernel", ())
    content: Dict[str, t.Any] = Dict()

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
