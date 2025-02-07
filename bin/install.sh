#!/bin/zsh
set -e


# this runs through the steps in the README for local installation
# prerequisite - asdf installed
# brew install asdf

asdf plugin add python
asdf plugin add direnv
asdf plugin add poetry

brew install postgresql
asdf install
poetry install --without tableau

poetry run pytest  --ignore=src/lamp_py/mssql/test_connect.py --ignore=tests/ingestion_tm/test_ingest.py --ignore=tests/aws/test_s3_utils.py --ignore=tests/performance_manager/test_performance_manager.py