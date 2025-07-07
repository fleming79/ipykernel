"""Compiler helpers for the debugger."""

from __future__ import annotations

import os
import pathlib
import tempfile
from typing import ClassVar

from IPython.core.compilerop import CachingCompiler


def murmur2_x86(data, seed):
    """Get the murmur2 hash."""
    m = 0x5BD1E995
    data = [chr(d) for d in str.encode(data, "utf8")]
    length = len(data)
    h = seed ^ length
    rounded_end = length & 0xFFFFFFFC
    for i in range(0, rounded_end, 4):
        k = (
            (ord(data[i]) & 0xFF)
            | ((ord(data[i + 1]) & 0xFF) << 8)
            | ((ord(data[i + 2]) & 0xFF) << 16)
            | (ord(data[i + 3]) << 24)
        )
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> 24
        k = (k * m) & 0xFFFFFFFF

        h = (h * m) & 0xFFFFFFFF
        h ^= k

    val = length & 0x03
    k = 0
    if val == 3:
        k = (ord(data[rounded_end + 2]) & 0xFF) << 16
    if val in [2, 3]:
        k |= (ord(data[rounded_end + 1]) & 0xFF) << 8
    if val in [1, 2, 3]:
        k |= ord(data[rounded_end]) & 0xFF
        h ^= k
        h = (h * m) & 0xFFFFFFFF

    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15

    return h


class XCachingCompiler(CachingCompiler):
    """A custom caching compiler that writes the code to a tempfile corresponding to the hash of the code."""

    hash_method: ClassVar = "Murmur2"

    def __init__(self, *args, **kwargs):
        """Initialize the compiler."""
        super().__init__(*args, **kwargs)

    def get_code_name(self, raw_code, transformed_code, number):
        """Get the code name."""
        return str(self.get_file_name(raw_code))

    def get_file_name(self, code) -> pathlib.Path:
        """Get a file name."""
        name = murmur2_x86(code, self.hash_seed)
        return self.tmp_directory.joinpath(f"{name}{self.tmp_file_suffix}")

    @property
    def tmp_directory(self) -> pathlib.Path:
        """Get a temp directory."""
        if not (tempdir := getattr(self, "_tempdir", None)):
            self._tempdir = tempdir = pathlib.Path(tempfile.gettempdir(), f"async_kernel_{os.getpid()}").resolve()
        return tempdir

    @property
    def hash_seed(self):
        """Get a temp hash seed."""
        return 0xC70F6907

    @property
    def tmp_file_prefix(self):
        return f"{self.tmp_directory}{os.sep}"

    @property
    def tmp_file_suffix(self):
        return ".py"
