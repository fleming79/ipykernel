"""The IPython kernel spec for Jupyter"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import enum
import importlib.util
import json
import shutil
import sys
import tempfile
from pathlib import Path

from jupyter_client.kernelspec import KernelSpec, _is_valid_kernel_name  # pyright: ignore[reportPrivateUsage]

# path to kernelspec resources
RESOURCES = Path(__file__).parent.joinpath("resources")


__all__ = ["Backend", "KernelName", "make_argv", "write_kernel_spec"]


class Backend(enum.StrEnum):
    asyncio = "asyncio"
    if importlib.util.find_spec("trio"):
        trio = "trio"


class KernelName(enum.StrEnum):
    asyncio = "async"
    trio = "async-trio"


def make_argv(
    *,
    connection_file="{connection_file}",
    kernel_name: KernelName | str = KernelName.asyncio,
    klass="async_kernel.Kernel",
    fullpath=True,
    **kwargs,
):
    """
    Constructs the argument vector (argv) for launching a Python kernel module.
    The backend is determined from the kernel_name. The default backend `asyncio` will
    be used unless the kernel_name contains 'trio' (case-insensitive). where a trio backend.

    Args:
        connection_file (str): The path to the connection file. Defaults to "{connection_file}".
        klass (str): The string import path to a Kernel.
        kernel_name (KernelName or str): The name of the kernel to use.
    kwargs:
        Additional settings to use on the instance of the Kernel.
        kwargs are converted to key/value pairs, keys will be prefixed with '--'.
        The kwargs should correspond to settings to set prior to starting. kwargs
        are set on the
        instance by eval on the value.
        keys that correspond to an attribute on the kernel instance are not used.

    Returns:
        list: A list of command-line arguments to launch the kernel module.
    """
    python = sys.executable if fullpath else "python"
    argv = [python, "-m", "async_kernel", "-f", connection_file]
    for k, v in ({"klass": klass, "kernel_name": kernel_name} | kwargs).items():
        argv.extend((f"--{k}", str(v)))
    return argv


def write_kernel_spec(
    path: Path | str | None = None,
    *,
    klass="async_kernel.Kernel",
    connection_file="{connection_file}",
    kernel_name: KernelName | str = KernelName.asyncio,
    fullpath=False,
    display_name="",
    **kwargs,
) -> Path:
    """
    Write a kernel spec directory to `path/kernel_name`.

    If `path` is not specified, a temporary directory is created.
    The path to the kernelspec is always returned.

    **kwargs:
        kwargs are passed to make_argv for additional runtime configuration of the kernel.
    """
    assert _is_valid_kernel_name(kernel_name)
    path = Path(path) if path else Path(tempfile.mkdtemp(suffix="_kernels")) / kernel_name
    # stage resources
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(RESOURCES, path, dirs_exist_ok=True)
    spec = KernelSpec()
    spec.argv = make_argv(
        klass=klass,
        connection_file=connection_file,
        kernel_name=kernel_name,
        fullpath=fullpath,
        **kwargs,
    )
    spec.name = kernel_name
    spec.display_name = display_name or f"Python ({kernel_name})"
    spec.language = "python"
    spec.interrupt_mode = "message"
    spec.metadata = {"debugger": True}

    # write kernel.json
    with path.joinpath("kernel.json").open("w") as f:
        json.dump(spec.to_dict(), f, indent=1)
    return path
