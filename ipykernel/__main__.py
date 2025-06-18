"""The cli entry point for ipykernel."""

from ipykernel.kernelspec import AsyncMode


def launch():
    import argparse
    import pathlib
    import sys

    from ipykernel.kernelapp import Kernel

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


if __name__ == "__main__":
    launch()
