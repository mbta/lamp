import pyarrow
import polars as pl

from lamp_py.ingestion.utils import explode_table_column, flatten_table_schema
from lamp_py.runtime_utils.remote_files_spare import (
    springboard_spare_vehicles,
    tableau_spare_vehicles,
)
from lamp_py.tableau.conversions.convert_types import convert_to_tableau_compatible_schema, get_default_tableau_schema_from_s3
from lamp_py.tableau.spare.default_converter import default_converter, default_converter_from_s3

from lamp_py.tableau.spare.wip_1_autogen_schema_printer import spare_resources

from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob

def flatten_spare_vehicle(table: pyarrow.Table) -> pyarrow.Table:
    """
    Apply transforms to spare vehicle.parquet
    """
    table = table.drop(["metadata.providerId", "metadata.owner", "emissionsRate", "createdAt", "updatedAt"])
    df = pl.from_arrow(table)
    accessibilty_rows = flatten_table_schema(
        explode_table_column(flatten_table_schema(table), "accessibilityFeatures")
    )  # .drop(['accessibilityFeatures'])

    return pl.concat([df, pl.from_arrow(accessibilty_rows)], how="align").to_arrow().drop("accessibilityFeatures")


SPARE_TABLEAU_PROJECT = "GTFS-RT"
# overrides1 = {"emissionsRate": pyarrow.int64()}
excludes1 = {"accessibilityFeatures": ""}
HyperSpareVehicles = FilteredHyperJob(
    remote_input_location=springboard_spare_vehicles,
    remote_output_location=tableau_spare_vehicles,
    rollup_num_days=None,
    processed_schema=get_default_tableau_schema_from_s3(
        springboard_spare_vehicles,
        preprocess=flatten_spare_vehicle,
        # overrides=overrides1,
        excludes=excludes1,
    ),
    parquet_preprocess=flatten_spare_vehicle,
    dataframe_filter=None,
    parquet_filter=None,
    tableau_project_name=SPARE_TABLEAU_PROJECT,
)

spare_job_list = []
spare_job_list_singles = [
    "admins",
    "appointmentTypes",
    "appointments",
]

# generically create jobs
for resource, (springboard_input, tableau_output) in spare_resources.items():

    spare_job_list.append(
        FilteredHyperJob(
            remote_input_location=springboard_input,
            remote_output_location=tableau_output,
            rollup_num_days=None,
            processed_schema=default_converter_from_s3(
                springboard_input
            ),
            parquet_preprocess=None,
            dataframe_filter=default_converter,
            parquet_filter=None,
            tableau_project_name=SPARE_TABLEAU_PROJECT,
        )
    )
