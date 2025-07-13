"""Wrappers for forwarding stdout/stderr over zmq"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from io import TextIOBase
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Callable


class OutStream(TextIOBase):
    """A file like object that calls flusher with the string output when flush is called."""

    topic = None
    encoding = "UTF-8"

    def __init__(self, name: Literal["stderr", "stdout"], flusher: Callable[[str], None], *, isatty=False):
        """
        Parameters
        ----------
        session : object
            the session object
        name : str {'stderr', 'stdout'}
            the name of the standard stream to replace
        isatty : bool (default, False)
            Indication of whether this stream has terminal capabilities (e.g. can handle colors)

        """
        super().__init__()
        self.name = name
        self._isatty = bool(isatty)
        self._sender = flusher
        self._out = ""

    def flush(self):
        if send := self._out:
            self._out = ""
            self._sender(send)

    def isatty(self):
        return self._isatty

    def write(self, string: str) -> int:
        """Write to current stream after encoding if necessary

        Returns
        -------
        len : int
            number of items from input parameter written to stream.

        """
        self._out = string
        self.flush()
        return len(string)

    def writelines(self, sequence):
        """Write lines to the stream (separators are not added)."""
        self.write("".join(sequence))

    def writable(self):
        """Test whether the stream is writable."""
        return True
