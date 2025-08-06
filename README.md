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

# Getting Started (Quick Start)
## Local Development

1) Install container runtime (colima at MBTA)

*Note*: Follow brew instructions for colima. Need to modify `~/.docker/config.json`

Follow these colima installation instructions: [Test](#tests)

2) Install asdf and dependencies
```
asdf plugin add python
asdf plugin add direnv
asdf plugin add poetry
asdf install
```
3. Copy `.env.template` into `.env`

4. Install python dependencies via poetry
```
poetry env use 3.12.3
poetry env activate  
poetry lock
poetry install
poetry self add poetry-plugin-shell
```

5. Start up containers for local development
```
[terminal 1] colima start -f
[terminal 2] docker-compose up seed_metadata
```

6. Run tests
```
poetry shell
pytest -s
```

## Local Development with AWS resources

There are many scripts in the `runners` repository that pull data from our S3 buckets to do semi-local development. These require AWS credential to be configured. See the [AWS Credentials](#aws-credentials) section below

Minimally, this will require the following steps:

1. Generate personal token on AWS Console
2. populate ~/.aws/config with:
```
[default]
region = us-east-1
```

3. populate ~/.aws/credentials with:
```
[default]
aws_access_key_id = <YOUR ID>
aws_secret_access_key = <YOUR ACCESS KEY>
```

4. To test this, run the following runner script that queries a date range from S3:
```
python runners/run_query_s3_with_date_range.py
```

## Setting up for development in Notebooks

Notebook and dev dependencies are installed separately from the app dependencies. 

To install these:

```
poetry install --with investigation
poetry install --with dev
```

To check `dev` installed correctly:
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

To check `investigation` installed correctly:

```sh 
poetry run marimo edit 
```

# Developer Usage (Detailed)

## Dependencies

LAMP uses [asdf](https://asdf-vm.com/) to mange runtime versions using the command line. Once installed, run the following in the root project directory:


```sh
# add project plugins
asdf plugin add python
asdf plugin add direnv
asdf plugin add poetry

# install versions of plugins specified in .tool-versions
asdf install
```

`poetry` is used by LAMP python applications to manage dependencies. 

`docker` and  `docker-compose` are required to run containerized versions of LAMP applications for local development.

## AWS Credentials

LAMP applications require permissions to access MBTA/CTD AWS resources. 

To get started with AWS, install the [AWS Command Line Interface](https://aws.amazon.com/cli/). Then, follow the instructions for [configuring the AWS cli](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds) to associate a local machine with an AWS account.  Finally, associate the AWS Account with the [Lamp Team User Group](https://github.com/mbta/devops/blob/627ab870f51b4bb9967f0f45efaee679e4a7d195/terraform/restricted/iam-user-groups.tf#L204-L213) found in the MBTA devops terraform repository.

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

## Tests

To run the tests, first install and setup Colima, Docker, and docker-compose:

```shell
brew install docker docker-compose colima
colima start
mkdir -p ${DOCKER_CONFIG:-"~/.docker"}/cli-plugins
ln -sfn /opt/homebrew/opt/docker-compose/bin/docker-compose ${DOCKER_CONFIG:-"~/.docker"}/cli-plugins/docker-compose
```

Then:

`pytest -s`

## Repository Design 

This repository contains all LAMP source code used to run, test and deploy LAMP applications.

Source code for LAMP python applications can be found in the [src/](src/)  directory. 
