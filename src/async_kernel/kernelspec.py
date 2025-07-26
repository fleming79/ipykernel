"""The IPython kernel spec for Jupyter"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import enum
import json
import shutil
import sys
import tempfile
from pathlib import Path

from jupyter_client.kernelspec import KernelSpec, _is_valid_kernel_name

# path to kernelspec resources
RESOURCES = Path(__file__).parent.joinpath("resources")


class KernelName(enum.StrEnum):
    asyncio = "async"
    trio = "async-trio"
    if sys.version_info >= (3, 12):
        asyncio_eager = "async-eager"


def write_kernel_spec(
    path: Path | str | None = None,
    *,
    kernel_name=KernelName.asyncio,
    module_name="async_kernel",
) -> Path:
    """
    Write a kernel spec directory to `path`.

    If `path` is not specified, a temporary directory is created.
    If `overrides` is given, the kernelspec JSON is updated before writing.

    The path to the kernelspec is always returned.
    """
    assert _is_valid_kernel_name(kernel_name)
    path = Path(path) if path else Path(tempfile.mkdtemp(suffix="_kernels")) / kernel_name
    # stage resources
    shutil.copytree(RESOURCES, path, dirs_exist_ok=True)

    spec = KernelSpec()
    spec.argv = make_argv(module_name=module_name, kernel_name=kernel_name, fullpath=False)
    spec.name = kernel_name
    spec.display_name = f"Python ({kernel_name})"
    spec.language = "python"
    spec.interrupt_mode = "message"
    spec.metadata = {"debugger": True}

    # write kernel.json
    with path.joinpath("kernel.json").open("w") as f:
        json.dump(spec.to_dict(), f, indent=1)
    return path


def make_argv(
    module_name="async_kernel", connection_file="{connection_file}", kernel_name=KernelName.asyncio, fullpath=True
):
    """
    Constructs the argument vector (argv) for launching a Python kernel module.

    Args:
        module_name (str): The name of the Python module to run as the kernel. Defaults to "async_kernel".
        connection_file (str): The path to the connection file. Defaults to "{connection_file}".
        kernel_name (KernelName or str): The name of the kernel to use. Defaults to KernelName.asyncio.

    Returns:
        list: A list of command-line arguments to launch the kernel module.
    """
    python = sys.executable if fullpath else "python"
    return [python, "-m", module_name, "-f", connection_file, "--kernel_name", str(KernelName(kernel_name))]


def write_all_kernelspec(base: Path, *, module_name="async_kernel", kernel_names=tuple(KernelName)):
    """
    Writes Jupyter kernel specifications for the specified kernel names to the given base directory.

    Parameters:
        base (Path): The base directory where the kernel specifications will be written.
        module_name (str, optional): The name of the module to use in the kernel spec. Defaults to "async_kernel".
        kernel_names (tuple[KernelName, ...], optional): A tuple of kernel names to generate specs for.
            If not provided, defaults to (KernelName.asyncio, KernelName.trio), and adds
            KernelName.asyncio_eager if running on Python 3.12 or newer.

    Behavior:
        - For each kernel name, removes any existing kernel spec directory at the destination.
        - Writes a new kernel spec using `write_kernel_spec`.
    """
    for kernel_name in kernel_names:
        dest = base / kernel_name
        if dest.exists():
            shutil.rmtree(dest)
        write_kernel_spec(dest, kernel_name=kernel_name, module_name=module_name)
