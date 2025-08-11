# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

"""The cli entry point for async_kernel."""

import argparse
import contextlib
import pathlib
import shutil
import sys
import traceback
from itertools import pairwise

import anyio
import traitlets

from async_kernel.kernel import Kernel
from async_kernel.kernelspec import Backend, KernelName, write_kernel_spec


def main(wait_exit_context=anyio.sleep_forever):
    "Main entry point to launch kernel or add/remove installed kernel specs."
    kernel_dir = pathlib.Path(sys.prefix) / "share/jupyter/kernels"
    parser = argparse.ArgumentParser(
        description="Kernel interface to start a kernel or add/remove a kernel spec."
        + f"The Jupyter Kernel directory is: f'{kernel_dir}'"
    )
    parser.add_argument(
        "-f",
        "--file",
        dest="connection_file",
        help="Start a Kernel with a connection file. To start a Kernel without a file use a period `.`.",
    )
    parser.add_argument(
        "-a",
        "--add",
        dest="add",
        help=f"Add a kernel spec. Default kernel names are: {list(map(str, KernelName))}.\n"
        + "To specify a 'trio' backend, include 'trio' in the name. Other options are also permitted. See: `write_kernel_spec` for detail.",
    )
    kernels = [] if not kernel_dir.exists() else [item.name for item in kernel_dir.iterdir() if item.is_dir()]
    parser.add_argument(
        "-r",
        "--remove",
        dest="remove",
        help=f"remove existing kernel specs. Installed kernels: {kernels}",
    )

    args, unknownargs = parser.parse_known_args()
    for k, v in pairwise(unknownargs):
        if k.startswith("--"):
            setattr(args, k.removeprefix("--"), v)
    if args.add:
        args.kernel_name = args.add
        for name in ["add", "remove"] + (["connection_file"] if args.connection_file is None else []):
            delattr(args, name)
        path = write_kernel_spec(path=kernel_dir / args.kernel_name, **vars(args))
        print(f"Added kernel spec {path!s}")
    elif args.remove:
        for name in args.remove.split(","):
            folder = kernel_dir / str(name)
            if folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
                print(f"Removed kernel spec: {name}")
            else:
                print(f"Kernel spec folder: '{name}' not found!")

    elif not args.connection_file:
        parser.print_help()
    else:
        klass = getattr(args, "klass", None)
        kernel_name: str = getattr(args, "kernel_name", None) or KernelName.asyncio
        cls: type[Kernel] = traitlets.import_item(klass) if klass else Kernel
        kernel = cls(kernel_name=kernel_name)
        for k, v in vars(args).items():
            if hasattr(kernel, k):
                if k == "connection_file" and v == ".":
                    continue
                try:
                    setattr(kernel, k, v)
                except Exception:
                    v = eval(v)  # noqa: PLW2901
                setattr(kernel, k, v)

        async def _start() -> None:
            print("Starting kernel")
            async with kernel.start_in_context():
                with contextlib.suppress(kernel.CancelledError):
                    await wait_exit_context()

        try:
            backend = Backend.trio if "trio" in kernel_name.lower() else Backend.asyncio
            anyio.run(_start, backend=backend)
        except KeyboardInterrupt:
            print("\nKernel stopped")
        except BaseException as e:
            traceback.print_exception(e, file=sys.stderr)
            if sys.__stderr__ is not sys.stderr:
                traceback.print_exception(e, file=sys.__stderr__)
            sys.exit(1)
        else:
            sys.exit(0)


if __name__ == "__main__":
    main()
