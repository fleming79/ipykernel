"""Test IO capturing functionality"""

import io

import pytest

from ipykernel.iostream import OutStream

pytestmark = pytest.mark.anyio


async def test_io_api(kernel):
    """Test that wrapped stdout has the same API as a normal TextIO object"""
    stream = OutStream(kernel, "stdout")

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
        stream.write(b"")  # type: ignore[arg-type]


async def test_io_isatty(kernel):
    stream = OutStream(kernel, "stdout", isatty=True)
    assert stream.isatty()


