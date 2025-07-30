from lamp_py.runtime_utils.remote_files_spare import (
    springboard_spare_vehicles,
    tableau_spare_vehicles,
)
from lamp_py.tableau.conversions.convert_types import get_default_tableau_schema_from_s3
from lamp_py.tableau.conversions.spare.convert_spare_vehicle import flatten_spare_vehicle
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
import pyarrow

SPARE_TABLEAU_PROJECT = "SPARE"
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


# if __name__ == '__main__':
#     processed_schema=get_default_tableau_schema_from_s3(
#         springboard_spare_vehicles,
#         preprocess=flatten_spare_vehicle,
#     )
#     print(processed_schema)
#     print("here!")
