"""The cli entry point for async_kernel."""

import argparse
import pathlib
import shutil
import sys

from async_kernel.kernel import Kernel
from async_kernel.kernelspec import KernelName, write_all_kernelspec


def launch():
    kernel_dir = pathlib.Path(sys.prefix) / "share/jupyter/kernels"
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file",
        dest="connection_file",
        default="",
        help="Start using the connection file. To start without specify a file use a period `.`",
    )
    parser.add_argument(
        "-a",
        "--add",
        dest="add",
        default="",
        help=f"Add a kernel spec. Options: {list(map(str, KernelName))}",
    )
    parser.add_argument(
        "-r",
        "--remove",
        dest="remove",
        default="",
        help=f"remove an existing kernel. Installed kernels: {[item.name for item in kernel_dir.iterdir() if item.is_dir()]}",
    )
    parser.add_argument(
        "--async-mode",
        dest="kernel_name",
        default=KernelName.asyncio,
        help=f"options: {list(map(str, KernelName))}",
    )
    args = parser.parse_args()
    if args.add:
        kernel_names = tuple(KernelName(n.strip("'\"")) for n in args.add.split(","))
        write_all_kernelspec(kernel_dir, kernel_names=kernel_names)
        print(f"Added kernel spec {args.add}")
    elif args.remove:
        for kernel_name in args.remove.split(","):
            folder = kernel_dir / str(kernel_name)
            if folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
                print(f"Removed kernel spec: {kernel_name}")
            else:
                print(f"Kernel spec folder: '{kernel_name}' not found!")
    elif not args.connection_file:
        parser.print_help()
    else:
        print("Starting kernel")
        try:
            Kernel.start(
                connection_file=""
                if args.connection_file == "."
                else str(pathlib.Path(args.connection_file).resolve()),
                kernel_name=KernelName(args.kernel_name),
            )
            sys.exit(0)
        except Exception as e:
            print(e)
            sys.exit(1)


if __name__ == "__main__":
    launch()
