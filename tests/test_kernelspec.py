# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import json
import pathlib
import shutil

import pytest
from jupyter_client.kernelspec import KernelSpec

from async_kernel.kernelspec import RESOURCES, KernelName, write_kernel_spec


@pytest.mark.parametrize("kernel_name", list(KernelName))
def test_write_kernel_spec(kernel_name: KernelName):
    path = write_kernel_spec(kernel_name=kernel_name)
    if RESOURCES.exists():
        for fname in RESOURCES.iterdir():
            dst = path.joinpath(fname)
            assert pathlib.Path(dst).exists()
    kernel_json = path.joinpath("kernel.json")
    assert kernel_json.exists()
    with kernel_json.open("r") as f:
        data = json.load(f)
    KernelSpec(**data)
    shutil.rmtree(path)
