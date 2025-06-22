"""The cli entry point for async_kernel."""

import argparse
import pathlib
import sys

from async_kernel.kernel import Kernel
from async_kernel.kernelspec import AsyncMode


def launch():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", dest="connection_file", default="")
    parser.add_argument(
        "--async-mode",
        dest="async_mode",
        default=AsyncMode.asyncio,
        help=f"options: {list(map(str, AsyncMode))}",
    )
    args = parser.parse_args()
    if not args.connection_file:
        parser.print_help()
        return
    print("starting kernel")
    try:
        Kernel.start(
            connection_file=str(pathlib.Path(args.connection_file).resolve()),
            async_mode=AsyncMode(args.async_mode),
        )
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    launch()
