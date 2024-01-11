# Tableau Publisher

The Tableau Publisher is an application that takes data created by the Rail Performance Manager application as parquet files and publishes them to the ITD Managed Tableau Instance as hyper files. 

## Application Operation

The application itself is run via a cloudwatch event that is set to trigger on a cronlike schedule.

On each run, it iterates through a list of jobs that generate hyper files and uploads them to the ITD Tableau server, where they can be used to generate dashboards and reports for external users. To generate the job reads a parquet file that has been created by upstream LAMP applications and converts it to a hyper file using the [Tableau Hyper API](https://www.tableau.com/developer/tools/hyper-api). The file is generated on local storage, and then uploaded to the ITD Managed Tableau server using the [Tableau Server Client](https://tableau.github.io/server-client-python/), a python library wrapping the [Tableau REST API](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm).

### Upstream Applications

To simplify the conversion from parquet to hyper, the schemas for both are defined within this module. We also store the hardcoded S3 filepaths. Because of this, components of this library are used by other applications when writing the parquet files.

## Developer Note

The Tableau Hyper API is not currently supported on Apple Silicon. This means that local execution on Mac OSX with arm64 processors will not work without emulation. In light of that, imports from this directory will trigger `ModuleNotFound` exceptions if running on the wrong system. To avoid that, the `__init__.py` file includes a wrapper around components that are consumed by other applications. These functions will log an error when run without the desired dependencies.

### Installation without Tableau dependencies

In `pyproject.toml`, there is an additional dependency group that contains the tableau dependencies. It is not marked optional, so these modules will be installed with `poetry install`. If you are on an arm64 architecture, you can avoid installing the tableau dependencies with `poetry install --without tableau`. This behavior is encoded in the `.envrcy`, `docker-compose.yml`, and `Dockerfile` files in this repository, so you should get the desired behavior without additional arguments.
