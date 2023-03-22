#!/bin/bash

# Get the list of changed subdirectories in python_sr/src/lamp_py
# 
# git command will give us a list of all directories containing changes within our source directory.
# * --dirstat - show the diff breakdown by directory
# * origin/main - compare the current branch to origin main
# * -- <dir> - only show diff inside of this directory
# * sample output:
#   10.0% python_src/src/lamp_py/ingestion/
#   20.0% python_src/src/lamp_py/aws/
#   30.0% python_src/src/lamp_py/performance_manager/
#
# awk command reads each line from previous command and parses out the subdir
# * -F'[/]' - deliminate each line by either `/`
# * '{ print $4 }' - print out the subdir.
#   * $0 is the entire line
#   * $1 is <percentage> python_src
#   * $2 is src
#   * $3 is lamp_py
#   * $4 is the subdir we're looking for 
CHANGED_SUBDIRS=$(git diff --dirstat origin/main -- "python_src/src/lamp_py" | \
                  awk -F'[/]' '{ print $4 }')

# Get the list of changed files for toml and lock files
#
# git command gives a list of all files changed
# * --name-only - only list the file name.
# * origin/main - compare the current branch to origin main
# * -- <filepath> <filepath> - only show diff for those files
CHANGED_FILES=$(git diff --name-only origin/main -- "python_src/pyproject.toml" "python_src/poetry.lock")

# Initialize test booleans to false
TEST_INGESTION=false
TEST_AWS=false
TEST_PM=false
TEST_POSTGRES=false

# Iterate through changed subdirs and set flags to true based on changes
for SUBDIR in $CHANGED_SUBDIRS; do
  if [[ "$SUBDIR" == "ingestion" ]]; then
    TEST_INGESTION=true
  elif [[ "$SUBDIR" == "aws" ]]; then
    TEST_INGESTION=true
    TEST_AWS=true
    TEST_PM=true
  elif [[ "$SUBDIR" == "postgres" ]]; then
    TEST_INGESTION=true
    TEST_PM=true
    TEST_POSTGRES=true
  elif [[ "$SUBDIR" == "performance_manager" ]]; then
    TEST_PM=true
  elif [[ "$SUBDIR" == "runtime_utils" ]]; then
    TEST_INGESTION=true
    TEST_PM=true
  else
    echo "Encountered unkonwn subdir $SUBDIR"
    exit 1
  fi
done

# If the lock file or pyproject dot toml file has changed, run all tests
if [ -n "$CHANGED_FILES" ]; then
  TEST_INGESTION=true
  TEST_AWS=true
  TEST_PM=true
  TEST_POSTGRES=true
fi

# Print out the test boolean values
echo "TEST_INGESTION=$TEST_INGESTION"
echo "TEST_AWS=$TEST_AWS"
echo "TEST_PM=$TEST_PM"
echo "TEST_POSTGRES=$TEST_POSTGRES"
