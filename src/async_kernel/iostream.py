"""Wrappers for forwarding stdout/stderr over zmq"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from io import TextIOBase
from threading import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


class OutStream(TextIOBase):
    """A file like object that calls flusher with the string output when flush is called."""

    _write_lock = Lock()

    def __init__(self, flusher: Callable[[str], None]):
        """
        Parameters
        ----------
        flusher: Callable
            A callback responsible for sending the output.

        ref: https://docs.python.org/3/library/io.html#io.IOBase
        """
        super().__init__()
        self._flusher = flusher
        self._out = ""

    def isatty(self):
        return True

    def readable(self):
        return False

    def seekable(self):
        return False

    def writable(self):
        return True

    def flush(self):
        if out := self._out:
            self._out = ""
            self._flusher(out)

    def write(self, string: str) -> int:
        """Write to current stream after encoding if necessary

        Returns
        -------
        len : int
            number of items from input parameter written to stream.

        """
        with self._write_lock:
            self._out = string
            self.flush()
        return len(string)

    def writelines(self, sequence):
        """Write lines to the stream (separators are not added)."""
        self.write("".join(sequence))
