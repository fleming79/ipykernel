from ipykernel._version import (
    __version__,
    kernel_protocol_version,
    kernel_protocol_version_info,
    version_info,
)
from ipykernel.connect import get_connection_file, get_connection_info, write_connection_file

try:
    import orjson  # type: ignore[import]
    from zmq.utils import jsonapi

    jsonapi.dumps = orjson.dumps
    jsonapi.loads = orjson.loads
except ImportError:
    pass
