"""Base class for a Comm"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import comm.base_comm

from ipykernel.ipkernel import IPythonKernel
from ipykernel.kernelbase import Kernel

__all__ = ["BaseComm"]


# this is the class that will be created if we do comm.create_comm
class BaseComm(comm.base_comm.BaseComm):  # type:ignore[misc]``
    """The base class for comms."""

    kernel: IPythonKernel

    def publish_msg(self, msg_type, data=None, metadata=None, buffers=None, **keys):
        """Helper for sending a comm message on IOPub"""
        if not Kernel.initialized():
            return

        data = {} if data is None else data
        metadata = {} if metadata is None else metadata
        content = dict(data=data, comm_id=self.comm_id, **keys)

        assert self.kernel.session is not None
        self.kernel.session.send(
            self.kernel.iopub_socket,
            msg_type,
            content,
            metadata=metadata,
            parent=self.kernel.parent_msg,
            ident=self.topic,
            buffers=buffers,
        )
