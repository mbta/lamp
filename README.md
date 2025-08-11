# Lightweight Application for Measuring Performance (LAMP)
LAMP is a collection of applications used to measure performance of the MBTA transit system.

## LAMP Applications:
* [Ingestion (Parquet Archiver)](src/lamp_py/ingestion/README.md)
* [Rail Performance Manager](src/lamp_py/performance_manager/README.md)
* [Bus Performance Manager](src/lamp_py/bus_performance_manager/README.md)

## Architecture

LAMP application architecture is managed and described using `Terraform` in the [MBTA Devops](https://github.com/mbta/devops) github repository. 

![Architecture Diagram](./architecture.jpg)

[Link](https://miro.com/app/board/uXjVOzXKW9s=/?share_link_id=356679616715) to Miro Diagram

# Getting Started with Local Development
## Prerequisites
Install these dependencies from `brew` and follow the installation output for any additional configuration.
- `colima`
    ```zsh
    brew install docker docker-compose colima
    colima start
    mkdir -p ${DOCKER_CONFIG:-"$HOME/.docker"}/cli-plugins
    ln -sfn /opt/homebrew/opt/docker-compose/bin/docker-compose ${DOCKER_CONFIG:-"$HOME/.docker"}/cli-plugins/docker-compose
    ```
- `asdf`
    ```zsh
    asdf plugin add python
    asdf plugin add direnv
    asdf plugin add poetry
    asdf install
    ```
    To configure `direnv`:
    1. Copy `.env.template` into `.env`
    2. [Hook](https://direnv.net/docs/hook.html) `direnv` into your shell
- `docker`
- `docker-compose`
- `postgresql`
- `unixodbc`

### GFTS-RT access (optional)

In the base installation, LAMP provides access to performancedata.mbta.com.
A fuller featureset is available by connecting to the MBTA's internal s3 storage.
For AWS access, also install
- `awscli`
    ```zsh
    aws configure
    ```
    will take you through an interactive CLI and store [your AWS Access Key credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/access-key-self-managed.html#Using_CreateAccessKey).
    Associate the AWS Account with the [Lamp Team User Group](https://github.com/mbta/devops/blob/627ab870f51b4bb9967f0f45efaee679e4a7d195/terraform/restricted/iam-user-groups.tf#L204-L213) found in the MBTA devops terraform repository.

## `poetry` configuration
```zsh
poetry env use 3.12.3
poetry env activate  
poetry lock
poetry install
```

## Check
If all this worked, you can run the following checks without errors.

1. Start up containers for local development
```zsh
# terminal 1
colima start -f
```
```zsh
# terminal 2
docker-compose up seed_metadata
```

2. Run tests
```zsh
poetry shell
pytest -s
```

### GFTS-RT (optional)

3. Query a date range from s3
```zsh
python runners/run_query_s3_with_date_range.py
```

## Setting up for development in Notebooks

Notebook and dev dependencies are installed separately from the app dependencies. 

To install these:

```
poetry install --with investigation
poetry install --with dev
```

To check `dev` installed correctly, run the [CI checks](#continuous-integration).

To check `investigation` installed correctly:

```sh 
poetry run marimo edit 
```

# Developer Usage (Detailed)

## Microsoft SQL

The LAMP ingestion application has a Microsoft SQL Server datasource. To query the Microsoft SQL Server, on Linux, two pre-requisites are required.

1. Install the [Mirosoft ODBC 18 driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).
2. Configure `openssl` to allow TLS V1.0 connections.

## Environmental Variables

Project environmental variables are stored in [.env](.env) and managed for command line usage with `direnv`.

Using `direnv`, whenever a shell moves into any project directory, the environmental variables defined in [.env](.env) are loaded automagically. 

Additionally, [docker-compose.yml](docker-compose.yml) is configured to use [.env](.env), so that running containerized applications will load the same environmental variables.

## Continuous Integration

To ensure code quality, linting, type checking, static analysis and unit tests are automatically run via github actions when pull requests are opened. 

CI for LAMP python applications can be run locally, in the root project directory, with the following `poetry` commands:
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

## Continuous Deployment

Images for LAMP applications are hosted by AWS on the Elastic Container Registry (ECR). Updates to application images are pushed to ECR via automated github actions. 

LAMP applications are hosted by AWS and run on Elastic Container Service (ECS) instances. Deployment of LAMP applications, to ECS instances, occur via automated github actions.

## Running Locally

LAMP uses `docker` and `docker-compose` to run local instances of applications for development purposes. Please refer to the `README` page of invidiual applications for instructions. 
