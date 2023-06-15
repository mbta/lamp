# Lightweight Application for Measuring Performance (LAMP)
LAMP is a collection of applications used to measure performance of the MBTA transit system using historical data sets.

## Architecture

 The actions are used for our continuous integration flows and updates to our Asana project. The Elixir API is planned to be an endpoint for other teams at CTD to programmatically access some of the data that comes out of our pipelines. The pipelines themselves are deployed onto AWS Elastic Container Services where they are run continuously.
[Link](https://miro.com/app/board/uXjVOzXKW9s=/?share_link_id=356679616715) to Miro Diagram

## LAMP Applications:
* [Ingestion (Parquet Archiver)](python_src/src/lamp_py/ingestion/README.md)
* [Performance Manager (Rail Performance)](python_src/src/lamp_py/performance_manager/README.md)

# Developer Usage

## Repository Design 
This repository manages all of our source code as well as tools for testing and deploying our applications. Local environmental variables are stored in [.env](.env), and loaded via `direnv` whenever a shell moves into this directory or any of its subdirectories. Any code executed in this directory will have those environmental variables set. Additionally, our docker compose is configured to consume the .env file, so all variables found there will also extend to containers running the application. Our source code for our data pipelines lives in the `python_src` directory. Source code for the elixir api server application is found in `api`.

#### Python Data Pipeline Library (`/python_src`)
Multiple applications are run using a shared python library. The library is structured in a standard source and tests directory structure and uses poetry to manage dependencies. Pipelines can be run via poetry with `poetry run <pipeline_name>` commands defined in the [project toml file](python_src/pyproject.toml). The source code is split into directories based on use. There are subdirectories for each pipeline (ingestion and performance manager) and subdirectories for tools shared between pipelines (aws interactions, postgres connections, postgres schema definitions). In the top of the directory we define our dependencies and tooling in the pyproject toml file. Poetry creates a poetry.lock file that stores dependency trees. We have an alembic.ini file that configures Alembic, our data migration tool. Lastly, there is a Dockerfile that describes how to build the container that can run our pipelines. 

#### Docker and Docker Compose
At the root of the directory we have a docker compose files that can be used to:
* standup a local postgres server. `docker-compose up local_rds`
    * this database can be reset with `docker rm -f -v local_rds`
* startup the performance manager. `docker-compose up performance_manager`
* startup the api server. `docker-compose up api_server`
* seed the local rds with metadata `docker-compose run seed_metadata`

NOTE: We don't provide ingestion here as it depends on aws s3 buckets and will actually move files around underneath running applications.

## Dependencies

LAMP uses [asdf](https://asdf-vm.com/) to mange runtime versions using the command line. Once installed, run the following in the root project directory:

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

`docker` and  `docker-compose` can be used to run applications locally, and are required if you want to run the application against a local postgres database.

## AWS Credentials

LAMP applications require permissions to access AWS resources. 

To get started with AWS, install the [AWS Command Line Interface](https://aws.amazon.com/cli/). Then, follow the instructions for [configuring the AWS cli](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds) to associate a local machine with an AWS account.  Finally, associate the AWS Account with the [Lamp Team User Group](https://github.com/mbta/devops/blob/627ab870f51b4bb9967f0f45efaee679e4a7d195/terraform/restricted/iam-user-groups.tf#L204-L213) found in the MBTA devops terraform repository.

## Continuous Integration

We use a gitflow strategy for development that allows everyone to work in parallel with few merging conflict. A developer branches off of the `main` branch, completes their feature / chore / fix in their branch, and then merges it back into `main` after a rebase against main. To ensure code quality, we have linting, type checking, static analysis and testing scripts that are run on our python code that are run via github actions when pull requests are opened. They can be run locally in the `python_src/` directory with the following:
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

When a pull request is merged, another github action will create the docker image for our python pipelines and push them to our Elastic Container Repo. It will also deploy this new container to our `dev` environment Elastic Container Service where it will pick up where the last version left off.