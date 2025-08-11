# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import json
import shutil
import signal
import sys
import types
from typing import TYPE_CHECKING

import anyio
import pytest

import async_kernel.__main__ as main
from async_kernel.kernelspec import Backend, KernelName, make_argv
from tests import utils

if TYPE_CHECKING:
    import pathlib


@pytest.fixture
def fake_kernel_dir(tmp_path, monkeypatch):
    kernel_dir = tmp_path / "share/jupyter/kernels"
    kernel_dir.mkdir(parents=True)
    monkeypatch.setattr(main, "sys", types.SimpleNamespace(prefix=str(tmp_path)))
    monkeypatch.setattr(sys, "prefix", str(tmp_path))
    return kernel_dir


def test_prints_help_when_no_args(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog"])
    main.main()
    out = capsys.readouterr().out
    assert "usage:" in out


def test_add_kernel(monkeypatch, fake_kernel_dir: pathlib.Path, capsys):
    monkeypatch.setattr(
        sys, "argv", ["prog", "-a", "async-trio", "--display_name", "my kernel", "--klass", "my.custom.class"]
    )
    main.main()
    out = capsys.readouterr().out
    assert "Added kernel spec async-trio" in out
    kernel_dir = fake_kernel_dir.joinpath("async-trio")
    assert (kernel_dir).exists()
    with kernel_dir.joinpath("kernel.json").open("rb") as f:
        spec = json.load(f)
    assert spec == {
        "argv": [
            "python",
            "-m",
            "async_kernel",
            "-f",
            "{connection_file}",
            "--klass",
            "my.custom.class",
            "--kernel_name",
            "async-trio",
        ],
        "env": {},
        "display_name": "my kernel",
        "language": "python",
        "interrupt_mode": "message",
        "metadata": {"debugger": True},
    }


def test_remove_existing_kernel(monkeypatch, fake_kernel_dir, capsys):
    kernel_name = "asyncio"
    (fake_kernel_dir / kernel_name).mkdir()
    monkeypatch.setattr(sys, "argv", ["prog", "-r", kernel_name])
    monkeypatch.setattr(main, "KernelName", KernelName)
    monkeypatch.setattr(main, "shutil", shutil)
    main.main()
    out = capsys.readouterr().out
    assert f"Removed kernel spec: {kernel_name}" in out
    assert not (fake_kernel_dir / kernel_name).exists()


def test_remove_nonexistent_kernel(monkeypatch, fake_kernel_dir, capsys):
    kernel_name = "notfound"
    monkeypatch.setattr(sys, "argv", ["prog", "-r", kernel_name])
    monkeypatch.setattr(main, "KernelName", KernelName)
    monkeypatch.setattr(main, "shutil", shutil)
    main.main()
    out = capsys.readouterr().out
    assert f"Kernel spec folder: '{kernel_name}' not found!" in out


def test_start_kernel_success(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog", "-f", ".", "--kernel_name", "async", "--backend=asyncio"])
    started = False

    async def wait_exit():
        nonlocal started
        started = True

    with pytest.raises(SystemExit) as e:
        main.main(wait_exit)
    assert e.value.code == 0
    assert started
    out = capsys.readouterr().out
    assert "Starting kernel" in out
    utils.clear_kernel()


def test_start_kernel_failure(monkeypatch, capsys, mocker):
    # Replace cleanup_connection_file with None to cause an exception
    monkeypatch.setattr(sys, "argv", ["prog", "-f", ".", "--cleanup_connection_file", "None"])
    mocker.patch.object(sys, "__stderr__")
    with pytest.raises(SystemExit) as e:
        main.main()
    assert e.value.code == 1


async def test_subprocess_kernels_client(subprocess_kernels_client, kernel_name):
    # Start & Stop a kernel
    backend = Backend.trio if "trio" in kernel_name.lower() else Backend.asyncio
    _, reply = await utils.execute(
        subprocess_kernels_client,
        "kernel = get_ipython().kernel",
        user_expressions={"kernel_name": "kernel.kernel_name", "backend": "kernel.anyio_backend"},
    )
    assert kernel_name in reply["user_expressions"]["kernel_name"]["data"]["text/plain"]
    assert backend in reply["user_expressions"]["backend"]["data"]["text/plain"]


def test_main(monkeypatch, anyio_backend, kernel_name):
    # Start & Stop a kernel
    monkeypatch.setattr(sys, "argv", ["prog", "-f", ".", "--quiet", "False"])
    started = False

    async def wait_exit():
        nonlocal started
        started = True

    with pytest.raises(SystemExit):
        main.main(wait_exit)


@pytest.mark.skipif(sys.platform == "win32", reason="Can't simulate keyboard interrupt on windows.")
async def test_subprocess_kernel_keyboard_interrupt(tmp_path, anyio_backend):
    # This is the keyboard interrupt from a console app, not to be confused with 'interrupt_request'.
    connection_file = tmp_path / "connection_file.json"
    command = make_argv(connection_file=connection_file)
    process = await anyio.open_process(command)
    while not connection_file.exists():
        await anyio.sleep(0.1)
    # Simulate a keyboard interrupt from the console.
    process.send_signal(signal.SIGINT)
    while process.returncode is None:
        await anyio.sleep(0.1)
    assert process.returncode == 0
