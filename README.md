# LAMP 

## Lightweight Application for Measuring Performance

LAMP is a collection of applications used to measure performance of the MBTA transit system.

## LAMP Applications:
* [Ingestion (Parquet Archiver)](python_src/src/lamp_py/ingestion/README.md)
* [Performance Manager (Rail Performance)](python_src/src/lamp_py/performance_manager/README.md)


# Architecture
[Link](https://miro.com/app/board/uXjVOzXKW9s=/?share_link_id=356679616715) to
Miro Diagram


# Developer Usage

## Dependencies

LAMP uses [asdf](https://asdf-vm.com/) to mange runtime versions using the
command line. Once installed, run the following in the root project directory:

```sh
# add project plugins
asdf plugin-add python
asdf plugin-add direnv
asdf plugin-add poetry
asdf plugin-add erlang
asdf plugin-add elixir

# install versions of plugins specified in .tool-versions
asdf install
```

`docker` and  `docker-compose` are required to run applications locally.


## AWS Credentials

LAMP applications require permissions to access AWS resources. 

To get started with AWS, install the
[AWS Command Line Interface](https://aws.amazon.com/cli/). Then, follow the instructions for 
[configuring the AWS cli](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds)
to associate a local machine with an AWS account. 

Finally, associate the AWS Account with the 
[Lamp Team User Group](https://github.com/mbta/devops/blob/627ab870f51b4bb9967f0f45efaee679e4a7d195/terraform/restricted/iam-user-groups.tf#L204-L213)
found in the MBTA devops terraform repository.

## Continuous Integraion

For the python applications, linting, type checking, static analysis, and tests can be run inside of the `python_src` directory with the following commands:

```sh
# black for Formatting
poetry run black .

# mypy for Type Checking
poetry run mypy .

# pylint for Static Analysis
poetry run pylint src tests

# pytest for Unit Tests
poetry run pytest
```

## Docker

The [Ingestion](python_src/src/lamp_py/ingestion/README.md) application should not be run locally because it would cause a race condition, with other application instances, when moving S3 objects in the AWS development environment.

The [Performance Manager](python_src/src/lamp_py/performance_manager/README.md) application can be run locally for development and debug purposes. Please refer to the application [README](python_src/src/lamp_py/performance_manager/README.md) for more information. 
