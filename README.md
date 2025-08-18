# Async kernel

[![image](https://img.shields.io/pypi/pyversions/async-kernel.svg)](https://pypi.python.org/pypi/async-kernel)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Built with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=plastic&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)

Async kernel is a Python [Jupyter kernel](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages) that runs in an [anyio](https://pypi.org/project/anyio/) event loop.


## Highlights
- Asynchronous
- Comms is not blocked during cell execution[^non-blocking-execution]
- Concurrent code execution [^run-concurrent]
- [Debugger client](https://jupyterlab.readthedocs.io/en/latest/user/debugger.html#debugger)
- Configurable backend - "asyncio" (default) or "trio backend" [^config-backend]
- [IPython](https://pypi.org/project/ipython/) shell for magic, code completions, and history
- No tornado - instead using anyio's [`wait_readable`](https://anyio.readthedocs.io/en/stable/api.html#anyio.wait_readable) to wait for incoming messages on zmq sockets


## Installation

```shell
pip install async-kernel
```

### Trio

To add a kernel spec for `trio`[^config-backend].

```shell
pip install trio
```

```shell
async-kernel -a async-trio
```

[![Link to demo](https://github.com/user-attachments/assets/9a4935ba-6af8-4c9f-bc67-b256be368811)](https://fleming79.github.io/async-kernel/simple_example/ "Show demo notebook.")


[^non-blocking-execution]: Shell messaging runs in a task separate to execute requests in the main thread. This means shell messages (including comms) can pass freely whilst an execute request is busy awaiting a result.

[^run-concurrent]: Code can also be scheduled for concurrent execution by adding `##task` or `##thread` at the top of the cell.

    Works with concurrent cell execution

    - [x] Jupyterlab
    - [ ] VS code - runs one cell at a time

[^config-backend]: The default backend is 'asyncio'. To add a 'trio' backend, define a KernelSpec with a kernel name that includes trio in it.
