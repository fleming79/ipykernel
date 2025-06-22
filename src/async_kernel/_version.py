"""
store the current version info of the server.
"""

from __future__ import annotations

import re
import sys

# Version string must appear intact for hatch versioning
__version__ = "0.0.0a1"

# Build up version_info tuple for backwards compatibility
pattern = r"(?P<major>\d+).(?P<minor>\d+).(?P<patch>\d+)(?P<rest>.*)"
match = re.match(pattern, __version__)
assert match is not None
parts: list[object] = [int(match[part]) for part in ["major", "minor", "patch"]]
if match["rest"]:
    parts.append(match["rest"])
version_info = tuple(parts)

kernel_protocol_version_info = (5, 4)
kernel_protocol_version = "{}.{}".format(*kernel_protocol_version_info)
implementation = "async_kernel"
implementation_version = " 0.1"

language_info = {
    "name": "python",
    "version": ".".join(map(str, sys.version_info)),
    "mimetype": "text/x-python",
    "codemirror_mode": {"name": "ipython", "version": 3},
    "pygments_lexer": "ipython3",
    "nbconvert_exporter": "python",
    "file_extension": ".py",
}
