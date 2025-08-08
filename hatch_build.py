"""A custom hatch build hook for async_kernel."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):  # pyright: ignore[reportUntypedBaseClass]
    """The async_kernel build hook."""

    def initialize(self, version, build_data):
        """Initialize the hook."""
        here = Path(__file__).parent.resolve()

        sys.path.insert(0, str(here / "src" / "async_kernel"))
        from kernelspec import KernelName, write_kernel_spec  # noqa: PLC0415

        write_kernel_spec(base=Path(here) / "data_kernelspec", kernel_name=KernelName.asyncio)
