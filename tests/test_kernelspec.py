# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import json
import os
import pathlib
import shutil

import pytest
from jupyter_client.kernelspec import KernelSpec

from ipykernel.kernelspec import RESOURCES, AsyncMode, write_kernel_spec


@pytest.mark.parametrize('async_mode', list(AsyncMode))
def test_write_kernel_spec(async_mode:AsyncMode):
    path = write_kernel_spec(kernel_name="my-kernel", async_mode=async_mode)
    for fname in os.listdir(RESOURCES):
        dst =  path.joinpath(fname)
        assert pathlib.Path(dst).exists()
    kernel_json = path.joinpath( "kernel.json")
    assert kernel_json.exists()
    with kernel_json.open("r") as f:
        data = json.load(f)
    KernelSpec(**data)
    shutil.rmtree(path)

