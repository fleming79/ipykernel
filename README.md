# Async Kernel for Jupyter

<!-- [![Build Status](https://github.com/ipython/ipykernel/actions/workflows/ci.yml/badge.svg?query=branch%3Amain++)](https://github.com/ipython/ipykernel/actions/workflows/ci.yml/badge.svg?query=branch%3Amain++)
[![Documentation Status](https://readthedocs.org/projects/ipykernel/badge/?version=latest)](http://ipykernel.readthedocs.io/en/latest/?badge=latest) -->

Async-kernel is a python implementation of a [Jupyter kernel](https://docs.jupyter.org/en/latest/projects/kernels.html#kernels-programming-languages). The kernel runs inside an [anyio](https://pypi.org/project/anyio/) event loop ([asyncio](https://docs.python.org/3/library/asyncio.html#module-asyncio) / [trio](https://pypi.org/project/trio/)) running requests in a modified [IPython](https://pypi.org/project/ipython/) InteractiveShell.

## Features

- [Execute-requests](#kerneljob) are run inside tasks.
- Multiple namespaces are supported.
- Uses [ContextVars](#contextvars) for better concurrent execution and to enable multiple namespaces.
- A [header directive](#header-directives) inserted in the code enables the user to modify how the code is executed (in a task or thread) and what namespace to use.
- The `Caller` class provides methods to executed code in threads with different event loops and awaiting the result.
- Uses the anyio function [`wait_readable`](https://anyio.readthedocs.io/en/stable/api.html#anyio.wait_readable) to await ZMQ socket messages.

### Header directives

Async-kernel adds the concept of a header directive `#@<execute-mode>, <options>`. Code passed in *execute requests* that start with symbols `#@` in the first non-blank line will be
interpreted as a header directive.

The directive can be used to modify how the code is executed and the `namespace_id` to use.

### Execute Modes

Execute modes provided are:

- `thread`: The code is run in a thread.
- `task`: The code is run as a task.
- `queue` (default execute mode): The code is added to a queue and executed sequentially.

### Execute Options

- `namespace_id`: Specify the namespace where the code is executed. `shell.namespaces` is where the `namespace_id` is mapped to a namespace.

### ContextVars

Async-kernel uses [ContextVars](https://docs.python.org/3/library/contextvars.html#module-contextvars) to enable concurrent execution of code and mapping that execution back the request. For async-kernel this means the same kernel and shell can be used to perform concurrent execution in multiple contexts (tasks/and threads) while providing a mapping back to the intended request. This is used for `stdio` and `stderr` output (including print) and `stdin`.

#### `kernel.job`

[Execute](https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute) requests store the request in a `Job` which is accessible via the kernel property `kernel.job`.

#### `shell.namespace_id`

In async-kernel the `kernel.shell` maintains a mapping of `namespace_id`'s to dicts. The shell namespace_id is a ContextVar.

### Example

This code will run in a `Caller` thread 'My thread' in the shell namespace 'My Namespace'.

```python
# @thread, namespace=My namespace, thread_name=My thread

import time

time.sleep(100)
```

Irrespective of the header directive, any code run in a cell will respect cancellation, though in the example above, the cancellation will only occur after the `time.sleep` call has returned. Should this have been run in the `MainThread` the time.sleep would have been cancelled.

## Kernel variants

The kernel name defines the anyio backend that is used. Currently there are three KernelNames implement.

1. async: An anyio 'asyncio' backend.
1. async-trio: An anyio 'trio' backend (requires trio - install manually).
1. async-eager: Any anyio 'asyncio' backend configure with an [eager task factory](https://docs.python.org/3/library/asyncio-task.html#eager-task-factory) (requires Python>=3.12).

### Enabling / disabling kernels

Kernels can be added/removed via the command line. In Jupyter Lab, you can do this by prefixing the command with '!' and refreshing the browser after the command is run.

#### Add

```shell
async_kernel -add async-eager
```

#### Remove

async_kernel -remove async

## Installation from source

1. `git clone`
1. `cd ipykernel`
1. `pip install -e ".[test]"`

After that, all normal `ipython` commands will use this newly-installed version of the kernel.

## Running tests

Follow the instructions from `Installation from source`.

and then from the root directory

```bash
pytest
```

## Running tests with coverage

Follow the instructions from `Installation from source`.

and then from the root directory

```bash
pytest -vv -s --cov ipykernel --cov-branch --cov-report term-missing:skip-covered --durations 10
```

## About the IPython Development Team

The IPython Development Team is the set of all contributors to the IPython project.
This includes all of the IPython subprojects.

The core team that coordinates development on GitHub can be found here:
https://github.com/ipython/.

## Origin

Async-kernel started as fork of [IPyKernel](https://pypi.org/project/ipykernel/) commit [#8322a7684b004ee95f07b2f86f61e28146a5996d](https://github.com/ipython/ipykernel/commit/8322a7684b004ee95f07b2f86f61e28146a5996d).

## Our Copyright Policy

IPython uses a shared copyright model. Each contributor maintains copyright
over their contributions to IPython. But, it is important to note that these
contributions are typically only changes to the repositories. Thus, the IPython
source code, in its entirety is not the copyright of any single person or
institution. Instead, it is the collective copyright of the entire IPython
Development Team. If individual contributors want to maintain a record of what
changes/contributions they have specific copyright on, they should indicate
their copyright in the commit message of the change, when they commit the
change to one of the IPython repositories.

With this in mind, the following banner should be used in any source code file
to indicate the copyright and license terms:

```
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
```
