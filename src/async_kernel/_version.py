# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

import sys

# Version string must appear intact for hatch versioning
__version__ = "0.1b0"

kernel_protocol_version_info = (5, 4)
kernel_protocol_version = "{}.{}".format(*kernel_protocol_version_info)

language_info = {
    "name": "python",
    "version": ".".join(map(str, sys.version_info)),
    "mimetype": "text/x-python",
    "codemirror_mode": {"name": "ipython", "version": 3},
    "pygments_lexer": "ipython3",
    "nbconvert_exporter": "python",
    "file_extension": ".py",
}
