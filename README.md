# datalineup

Datalineup is a job scheduling and data processing system developed for web crawling needs at Khulnasoft Systems.

## Documentation

- [**Architecture Overview**](docs/architecture_overview.md)

## Installation

- [PyPI](https://pypi.org/project/datalineup-engine/): `datalineup-engine`.
- [Docker Hub](https://hub.docker.com/repository/docker/khulnasoft/datalineup): `khulnasoft/datalineup`.

## Development

Install [nox](https://nox.thea.codes/en/stable/) and [poetry](https://python-poetry.org/docs/).

- To run all tests: `nox`
- To format code: `nox -rs format`

You can also work from the shell with:

```console
$ # Install the project locally.
$ poetry install --all-extras
$ poetry shell
$ # Run the utilities.
$ py.test tests -xsvv
$ mypy src tests
```
