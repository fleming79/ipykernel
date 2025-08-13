# Async kernel

[![image](https://img.shields.io/pypi/pyversions/async-kernel.svg)](https://pypi.python.org/pypi/async-kernel)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)

Async kernel is a Python [Jupyter kernel](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages) that runs in an [anyio](https://pypi.org/project/anyio/) event loop.

Async kernel is designed to run execute requests in tasks separate to the shell message loop. This means the kernel won't dead locks awaiting a response that is delivered via the shell.

Execute requests are queued for execution by default, but can also be run concurrently by including either `##task` or `##thread` at the top of the code. `##thread` will run the code in a separate `Caller` thread, that provides its own event loop.

## Highlights

- Concurrent cell execution in tasks or cells supported [^run-concurrent]
- Comms is not blocked during cell execution[^non-blocking-execution]
- Debugger included
- Configurable backend - "asyncio" (default) or "trio backend" [^config-backend]
- [IPython](https://pypi.org/project/ipython/) shell for magic, code completions, etc.
- No tornado - instead using anyio's [`wait_readable`](https://anyio.readthedocs.io/en/stable/api.html#anyio.wait_readable) to wait for incoming messages on zmq sockets

## Installation

```shell
pip install async-kernel
```

To add a kernel spec for `trio`.

```shell
pip install trio
async-kernel add async-trio
```

## Origin

Async-kernel started as fork of [IPyKernel](https://pypi.org/project/ipykernel/) commit [#8322a7684b004ee95f07b2f86f61e28146a5996d](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d).

```shell
async-kernel -a async-trio
```

[^run-concurrent]: Execute requests (code cells) are run passed to a queue for execution that is run in a different task to shell message handling. This means shell messages can be processed whilst execute requests are being performed. Code can also be scheduled for concurrent execution by adding `##task` or `##thread` at the top of the cell.

[^non-blocking-execution]: Shell messaging runs in a task separate to execute requests in the main thread. This means shell messages (including comms) can pass freely whilst an execute request is busy awaiting a result.

[^config-backend]: The default backend is 'asyncio'. To add a 'trio' backend, define a KernelSpec with a kernel name that includes trio in it.
