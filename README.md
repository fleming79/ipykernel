# Async Kernel for Jupyter

<!-- [![Build Status](https://github.com/ipython/ipykernel/actions/workflows/ci.yml/badge.svg?query=branch%3Amain++)](https://github.com/ipython/ipykernel/actions/workflows/ci.yml/badge.svg?query=branch%3Amain++)
[![Documentation Status](https://readthedocs.org/projects/ipykernel/badge/?version=latest)](http://ipykernel.readthedocs.io/en/latest/?badge=latest) -->

Async-kernel is a python implementation of a [Jupyter kernel](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages). The kernel runs inside an [anyio](https://pypi.org/project/anyio/) event loop ([asyncio](https://docs.python.org/3/library/asyncio.html#module-asyncio) / [trio](https://pypi.org/project/trio/)) running requests in a modified [IPython](https://pypi.org/project/ipython/) InteractiveShell.

## Features

- [Execute-requests](#kerneljob) by default are run in a task (sequentially) without blocking shell messages.
- `stdout`(including print), `stderr` and `stdin`(input) map correctly to the execute request (see: [ContextVars](#contextvars)).
- Cell code can be run in threads or tasks by adding `##thread` or `##task` respectively as the first line in a cell (see [Execute mode](#execute-mode)).
- Provides a `Caller` class to execute code in tasks/threads with a thread safe Future providing access to the result.
- Uses the anyio function [`wait_readable`](https://anyio.readthedocs.io/en/stable/api.html#anyio.wait_readable) to await ZMQ socket messages.

### Execute mode

If you add `##<execute-mode>` to the top of cell, the kernel will modify how the cell is run. The following execute modes are supported.

- `##thread` - The code is run in a thread.
- `##task` - The code is run as a task.
- `##queue` (default behaviour) - The code is added to a queue and executed sequentially in a task.

### ContextVars

Execute request jobs are stored as a [ContextVar](https://docs.python.org/3/library/contextvars.html#module-contextvars) which is accessible on the kernel as the property `kernel.job`. Using a context variable makes it possible to perform concurrent execution enabling `stdio`, `stderr` and `stdin` to map back to the initial job (execute request).

#### Example: run a cell in a thread

This code will run the code in a thread.

```python
##thread

import time

time.sleep(100)
```

Irrespective of the Execute mode, any code run in a cell will respect cancellation, though in this example the cancellation will only occur after the `time.sleep` call has returned. Should this have been run in the `MainThread` the time.sleep would have been cancelled immediately by means of a signal.

## Kernel variants

The kernel name defines the anyio backend that is used. Currently there are three KernelNames implement.

1. async: An anyio 'asyncio' backend.
1. async-trio: An anyio 'trio' backend (requires trio - install manually).

### Enabling / disabling kernels

Kernels can be added/removed via the command line.

#### Add

```shell
async_kernel -add async-trio
```

#### Remove

async_kernel -remove async

# Development

## Installation from source

1. `git clone`
1. `cd ipykernel`
1. `uv sync"`
1. Activate the environment.

After that, all normal `ipython` commands will use this newly-installed version of the kernel.

## Running tests

```bash
pytest
```

## Running tests with coverage

```bash
pytest -vv -s --cov
```

# Origin

Async-kernel started as fork of [IPyKernel](https://pypi.org/project/ipykernel/) commit [#8322a7684b004ee95f07b2f86f61e28146a5996d](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d).
