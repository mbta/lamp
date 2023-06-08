# Ingestion

Ingestion is an application to aggreagate GTFS-RT and GTFS data into parquet files for storage in AWS S3 buckets.

## GTFS to Parquet
Raw Real Time GTFS files are collected in an incoming S3 bucket and are
populated by [Delta](https://github.com/mbta/delt), a small service that logs
http files to the bucket configurably. A python based lambda collects all of the
files in the incoming bucket and organizes them into batches of similar types.
These batches are then processed by a second python based lambda that adds all
of the data in each real time gtfs file into a single parquet file. These
parquet files are then uploaded to a separate S3 bucket, with all processed
files moved to an archive.

The python module used for batching and conversions is located in
`/py_gtfs_rt_ingestion/`, along with its tests and the scripts that are
triggered in each of the lambda functions. This modules, its dependencies, and
the scripts are zipped up together before being deployed to the lambda
functions.

### Types of GTFS Files
* Real Time Alerts
* Real Time Bus Trip Updates
* Real Time Bus Vehicle Positions
* Real Time Trip Updates
* Real Time Vehicle Count
* Real Time Vehicle Positions
* Static Schedule Data

Information on the parquet table format for these file types can be found
[here](parquet_schemas.md).

## Parquet to Relational Database
Parquet files are analyzed by the Performance Manager application to compare
static schedule data with the real time positioning of vehicles in the field.
The application is run inside of python3.9 image described in a Dockerfile,
executing code found in the performance_manager directory.

## py_gtfs_rt_ingestion 
Dependency management in this module is handled by [poetry](python-poetry.org),
which is installed via asdf. It will create a virtual env with all of the
projects dependencies in it with `poetry install`.

There are two scripts at the top level of this directory used by the lambda
functions and a third helper script.

* `batch_files.py` - create batches of files to process out of an incoming bucket.
  These batches can either be output as a list of lambda event dicts or the
  ingestion lambda can be triggered directly.
* `ingest.py` - create a parquet table out of the entries of a list of gtfs
  files that are stored locally or on s3. s3 files are moved to an archive
  bucket on successful conversion and an error bucket on failure. parquet tables
  are saved to an outgoing bucket.
* `dev_test_setup.py` - setup the s3 dev import, archive, error, and output
  buckets to run end to end testing.





