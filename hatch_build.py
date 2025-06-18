"""A custom hatch build hook for ipykernel."""

import shutil
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    """The IPykernel build hook."""

    def initialize(self, version, build_data):
        """Initialize the hook."""
        here = Path(__file__).parent.resolve()
        sys.path.insert(0, str(here))
        from ipykernel.kernelspec import AsyncMode, write_kernel_spec

        python_args = ("python", "-m", f"{self.metadata.name}.__main__:launch")
        modes = [AsyncMode.asyncio, AsyncMode.trio]
        if sys.version_info >= (3, 12):
            modes.append(AsyncMode.asyncio_eager)
        base = Path(here) / "data_kernelspec"
        if base.exists():
            shutil.rmtree(base)
        for async_mode in modes:
            if async_mode is AsyncMode.asyncio:
                kernel_name = self.metadata.name
            else:
                kernel_name = f"{self.metadata.name}-{async_mode}"
            dest = base / kernel_name
            write_kernel_spec(dest, kernel_name=kernel_name, async_mode=async_mode, python_args=python_args)
