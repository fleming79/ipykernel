# Contributing

This project is under active development. Feel free to create an issue to provide feedback.

## Development

## Installation from source

```shell
git clone https://github.com/fleming79/async-kernel.git
cd async-kernel
uv venv -p python@311
uv sync
# Activate the environment
```

### update packages

```shell
uv lock --upgrade
```

## Running tests

```shell
uv run pytest
```

## Running tests with coverage

We are aiming for 100% code coverage on CI (Linux). Any new code should also update tests to maintain coverage.

```shell
uv run pytest -vv --cov
```

## Code Styling

`Async kernel` uses ruff for code formatting. The pre-commit hook should take care of how it should look.

To install `pre-commit`, run the following:

```shell
pip install pre-commit
pre-commit install
```

You can invoke the pre-commit hook by hand at any time with:

```shell
pre-commit run
```

## Type checking

Type checking is performed using [basedpyright](https://docs.basedpyright.com/).

```shell
basedpyright
```

## Documentation

Documentation is provided my [Material for MkDocs ](https://squidfunk.github.io/mkdocs-material/). To start up a server for editing locally:

### Install

```shell
uv sync --group docs
uv run async-kernel -a async-docs --cell_execute_timeout 0.1
```

### Serve locally

```shell
mkdocs serve 
```

### API / Docstrings

API documentation is included using [mkdocstrings](https://mkdocstrings.github.io/).

Docstrings are written in docstring format [google-notypes](https://mkdocstrings.github.io/griffe/reference/docstrings/?h=google#google-style).
Typing information is included automatically by [griff](https://mkdocstrings.github.io/griffe).

#### See also

- [cross-referencing](https://mkdocstrings.github.io/usage/#cross-references)

### Notebooks

Notebooks are included in the documentation with the plugin [mkdocs-jupyter](https://github.com/danielfrg/mkdocs-jupyter).

#### Useful links

These links are not relevant for docstrings.

- [footnotes](https://squidfunk.github.io/mkdocs-material/reference/footnotes/#usage)
- [tooltips](https://squidfunk.github.io/mkdocs-material/reference/tooltips/#usage)

### Deploy manually

```shell
mkdocs gh-deploy --force
```

## Releasing Async kernel

TODO
