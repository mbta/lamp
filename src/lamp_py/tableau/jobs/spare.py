from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob


import polars as pl
from lamp_py.runtime_utils.remote_files import (
    S3Location,
    S3_SPRINGBOARD,
    S3_ARCHIVE,
)
# Spare data, published by Odin

# temporary, clean up after uploading any files
SPARE_TABLEAU_PROJECT = "Glides"

# list_of_files = ["vehicles.parquet"]

# for file in list_of_files:
file = "vehicles.parquet"
s3_input_path = S3Location(bucket=S3_SPRINGBOARD, prefix=f"odin/data/spare/{file}")
s3_output_path = S3Location(
    bucket=S3_ARCHIVE, prefix=f"lamp/spare/tableau_publisher/{file}"
)


vehicles_schema = (
    pl.DataFrame(
        schema={
            "id": pl.String(),
            "identifier": pl.String(),
            "ownerUserId": pl.String(),
            "ownerType": pl.String(),
            "make": pl.String(),
            "model": pl.String(),
            "color": pl.String(),
            "licensePlate": pl.String(),
            "passengerSeats": pl.Int32(),
            # "accessibilityFeatures": pl.List(
            # pl.Struct(
            # {
            # "type": pl.String(),
            # "count": pl.Int32(),
            # "seatCost": pl.Int32(),
            # "requireFirstInLastOut": pl.Boolean(),
            # }
            # )
            # ),
            "status": pl.String(),
            "metadata": pl.String(),
            "emissionsRate": pl.Int64(),
            "vehicleTypeId": pl.String(),
            "capacityType": pl.String(),
            "createdAt": pl.Int64(),
            "updatedAt": pl.Int64(),
            "photoUrl": pl.String(),
        }
    )
    .to_arrow()
    .schema
)

spare_job = FilteredHyperJob(
    remote_input_location=s3_input_path,
    remote_output_location=s3_output_path,
    processed_schema=vehicles_schema,
    tableau_project_name=SPARE_TABLEAU_PROJECT,
    rollup_num_days=60,
    parquet_filter=None,
    dataframe_filter=None,
)
