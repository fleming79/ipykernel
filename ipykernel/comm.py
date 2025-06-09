"""Base class for a Comm"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import logging

import comm
import traitlets
from typing_extensions import override

from ipykernel.ipkernel import IPythonKernel
from ipykernel.kernelbase import Kernel

logger = logging.getLogger("ipykernel.comm")

__all__ = ["Comm"]


class Comm(comm.base_comm.BaseComm):
    """Comms optimized for IPythonKernel.

    Notes:
    -  Requires kernel to b set externally
    - This is set by CommManager, so if working with a
    """

    __slots__ = [
        "_close_callback",
        "_close_data",
        "_closed",
        "_msg_callback",
        "_open_data",
        "comm_id",
        "primary",
        "target_module",
        "target_name",
        "topic",
    ]
    kernel: IPythonKernel | None = None

    def publish_msg(self, msg_type, data=None, metadata=None, buffers=None, **keys):
        """Helper for sending a comm message on IOPub"""
        if not Kernel.initialized():
            return

        data = {} if data is None else data
        metadata = {} if metadata is None else metadata
        content = dict(data=data, comm_id=self.comm_id, **keys)

        if (kernel := self.kernel) is None:
            # Only send when the kernel is set
            return

        kernel.session.send(
            kernel.iopub_socket,
            msg_type,
            content,
            metadata=metadata,
            parent=kernel.parent_msg,
            ident=self.topic,
            buffers=buffers,
        )

    @override
    def handle_msg(self, msg: comm.base_comm.MessageType) -> None:
        """Handle a comm_msg message"""
        if self._msg_callback:
            self._msg_callback(msg)


"""Base class to manage comms"""


class CommManager(comm.base_comm.CommManager, traitlets.HasTraits):
    """A comm manager for IPythonKernel.

    When the kernel is set it will also set the kernel on all existing `Comm` instances.
    Notes:
    - The `Comm` will only send messages when the kernel is set.
    - The kernel is observed and must be set externally.
    - IPythonKernel, sets the kerenel this once it has been started.
    """

    kernel: traitlets.Instance[IPythonKernel | None] = traitlets.Instance(IPythonKernel, allow_none=True)  # type: ignore[assignment]
    comms: traitlets.Dict[str, comm.base_comm.BaseComm] = traitlets.Dict()
    targets: traitlets.Dict[str, comm.base_comm.CommTargetCallback] = traitlets.Dict()

    @traitlets.observe("kernel")
    def _observe_kernel(self, change: dict):
        kernel: IPythonKernel = change["new"]
        for c in self.comms.values():
            if isinstance(c, Comm):
                c.kernel = kernel

    @override
    def register_comm(self, comm: comm.base_comm.BaseComm) -> str:
        """Register a new comm"""
        if isinstance(comm, Comm) and (kernel := self.kernel):
            comm.kernel = kernel
        return super().register_comm(comm)


comm_manager = CommManager()


def get_comm_manager():
    return comm_manager


def set_comm():
    "Set the comm manager"
    comm.create_comm = Comm
    comm.get_comm_manager = get_comm_manager
