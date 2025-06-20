"""A custom hatch build hook for asynckernel."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    """The asynckernel build hook."""

    def initialize(self, version, build_data):
        """Initialize the hook."""
        here = Path(__file__).parent.resolve()
        module_name = self.metadata.name
        sys.path.insert(0, str(here / "src" / module_name))
        from kernelspec import write_all_kernelspec  # type: ignore  # noqa: PGH003, PLC0415

        write_all_kernelspec(base=Path(here) / "data_kernelspec", module_name=module_name)
