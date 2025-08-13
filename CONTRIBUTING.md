# Contributing

This project is in development. Create an issue to provide feedback.

## Development

## Installation from source

```shell
git clone https://github.com/fleming79/async-kernel.git
cd async-kernel
uv venv -p python@311
uv sync
# Activate the environment
```

## Running tests

```shell
pytest
```

## Running tests with coverage

We are aiming for 100% code coverage. Any new code should have meaningful tests
added to ensure reliability.

```shell
pytest -vv --cov
```

## Code Styling

`Async kernel` uses ruff for code formatting.
the pre-commit hook should take care of how it should look.
To install `pre-commit`, run the following::

```shell
pip install pre-commit
pre-commit install
```

You can invoke the pre-commit hook by hand at any time with::

```shell
pre-commit run
```

## Type checking

Type checking is performed using [basedpyright](https://docs.basedpyright.com/). It is installed automatically.

To run use

```shell
basedpyright
```

## Documentation

Documentation is provided my [Material for MkDocs ](https://squidfunk.github.io/mkdocs-material/).

To install dependencies:

```shell
uv sync --no-dev --frozen --group docs
```

To start the server locally:

```shell
mkdocs serve
```

## Releasing Async kernel

TODO
