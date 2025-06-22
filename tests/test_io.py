"""Test IO capturing functionality"""

import io

import pytest

from async_kernel.iostream import OutStream


def test_io_api():
    """Test that wrapped stdout has the same API as a normal TextIO object"""

    def flusher(string: str):
        "" + string  # type: ignore[operator]

    stream = OutStream("stdout", flusher)

    assert stream.errors is None
    assert not stream.isatty()
    with pytest.raises(io.UnsupportedOperation):
        stream.detach()
    with pytest.raises(io.UnsupportedOperation):
        next(stream)
    with pytest.raises(io.UnsupportedOperation):
        stream.read()
    with pytest.raises(io.UnsupportedOperation):
        stream.readline()
    with pytest.raises(io.UnsupportedOperation):
        stream.seek(0)
    with pytest.raises(io.UnsupportedOperation):
        stream.tell()
    with pytest.raises(TypeError):
        stream.write(b" ")  # type: ignore[arg-type]


def test_io_isatty():
    stream = OutStream("stdout", lambda _: None, isatty=True)
    assert stream.isatty()
