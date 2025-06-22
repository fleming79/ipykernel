"""The IPython kernel spec for Jupyter"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import enum
import json
import shutil
import stat
import sys
import tempfile
from pathlib import Path

from jupyter_client.kernelspec import KernelSpec, _is_valid_kernel_name

# path to kernelspec resources
RESOURCES = Path(__file__).parent.joinpath("resources")


class AsyncMode(enum.StrEnum):
    asyncio = "asyncio"
    trio = "trio"
    asyncio_eager = "asyncio_eager"


def write_kernel_spec(
    path: Path | str | None = None,
    *,
    kernel_name: str,
    async_mode=AsyncMode.asyncio,
    python_args=("python", "-m", "async_kernel.__main__:launch"),
) -> Path:
    """Write a kernel spec directory to `path`

    If `path` is not specified, a temporary directory is created.
    If `overrides` is given, the kernelspec JSON is updated before writing.

    The path to the kernelspec is always returned.
    """
    assert _is_valid_kernel_name(kernel_name)
    path = Path(path) if path else Path(tempfile.mkdtemp(suffix="_kernels")) / kernel_name

    # stage resources
    shutil.copytree(RESOURCES, path, dirs_exist_ok=True)

    # ensure path is writable
    mask = path.stat().st_mode
    if not mask & stat.S_IWUSR:
        path.chmod(mask | stat.S_IWUSR)

    spec = KernelSpec()
    spec.argv = [*python_args, "-f", "{connection_file}", "--async-mode", str(AsyncMode(async_mode))]
    spec.name = kernel_name
    spec.display_name = f"Python ({kernel_name})"
    spec.language = "python"
    spec.interrupt_mode = "message"
    spec.metadata = {}

    # write kernel.json
    with path.joinpath("kernel.json").open("w") as f:
        json.dump(spec.to_dict(), f, indent=1)
    return path



def write_all_kernelspec(base: Path, module_name: str, modes: tuple[AsyncMode, ...] = ()):
    python_args = ("python", "-m", f"{module_name}.__main__:launch")
    if not modes:
        modes = (AsyncMode.asyncio, AsyncMode.trio)
        if sys.version_info >= (3, 12):
            modes = (*modes, AsyncMode.asyncio_eager)
    if base.exists():
        shutil.rmtree(base)
    for async_mode in modes:
        kernel_name = str(async_mode)
        dest = base / kernel_name
        write_kernel_spec(dest, kernel_name=kernel_name, async_mode=async_mode, python_args=python_args)
