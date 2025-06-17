"""Entry point for launching an IPython kernel."""

if __name__ == "__main__":
    import argparse
    import sys

    from ipykernel.kernelapp import AsyncMode, MainKernel

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", dest="connection_file", default="")
    parser.add_argument("--async_mode", dest="async_mode", default=AsyncMode.asyncio)
    args = parser.parse_args()
    sys.exit(MainKernel.start(connection_file=args.connection_file, async_mode=AsyncMode(args.async_mode)))
