import shutil
import sys
import types
from unittest import mock

import pytest

import async_kernel.__main__ as main


@pytest.fixture
def fake_kernel_dir(tmp_path, monkeypatch):
    kernel_dir = tmp_path / "share/jupyter/kernels"
    kernel_dir.mkdir(parents=True)
    monkeypatch.setattr(main, "sys", types.SimpleNamespace(prefix=str(tmp_path)))
    return kernel_dir


def test_prints_help_when_no_args(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog"])
    main.main()
    out = capsys.readouterr().out
    assert "usage:" in out


def test_add_kernel(monkeypatch, fake_kernel_dir, capsys):
    monkeypatch.setattr(sys, "argv", ["prog", "-a", "async-trio"])
    monkeypatch.setattr(main, "write_all_kernelspec", mock.Mock())
    monkeypatch.setattr(main, "KernelName", main.KernelName)
    main.main()
    out = capsys.readouterr().out
    assert "Added kernel spec async-trio" in out
    main.write_all_kernelspec.assert_called()


def test_remove_existing_kernel(monkeypatch, fake_kernel_dir, capsys):
    kernel_name = "asyncio"
    (fake_kernel_dir / kernel_name).mkdir()
    monkeypatch.setattr(sys, "argv", ["prog", "-r", kernel_name])
    monkeypatch.setattr(main, "KernelName", main.KernelName)
    monkeypatch.setattr(main, "shutil", shutil)
    main.main()
    out = capsys.readouterr().out
    assert f"Removed kernel spec: {kernel_name}" in out
    assert not (fake_kernel_dir / kernel_name).exists()


def test_remove_nonexistent_kernel(monkeypatch, fake_kernel_dir, capsys):
    kernel_name = "notfound"
    monkeypatch.setattr(sys, "argv", ["prog", "-r", kernel_name])
    monkeypatch.setattr(main, "KernelName", main.KernelName)
    monkeypatch.setattr(main, "shutil", shutil)
    main.main()
    out = capsys.readouterr().out
    assert f"Kernel spec folder: '{kernel_name}' not found!" in out


def test_start_kernel_success(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog", "-f", ".", "--async-mode", "async"])
    start_mock = mock.Mock()
    monkeypatch.setattr(main.Kernel, "start", start_mock)
    with pytest.raises(SystemExit) as e:
        main.main()
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert "Starting kernel" in out
    start_mock.assert_called()


def test_start_kernel_failure(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["prog", "-f", ".", "--async-mode", "async"])

    def fail_start(*a, **kw):
        msg = "fail!"
        raise RuntimeError(msg)

    monkeypatch.setattr(main.Kernel, "start", fail_start)
    with pytest.raises(SystemExit) as e:
        main.main()
    assert e.value.code == 1
    out = capsys.readouterr().out
    assert "fail!" in out
