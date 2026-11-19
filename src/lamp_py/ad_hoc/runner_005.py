import tempfile
from datetime import datetime

import polars as pl
from pyarrow.parquet import read_metadata

from lamp_py.aws.s3 import download_file, replace_remote_parquet
from lamp_py.ingestion.glides import VehicleTripAssignmentTable
from lamp_py.runtime_utils.lamp_exception import LampInvalidProcessingError
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import glides_vehicle_assignment


def runner(dry_run: bool = True) -> None:
    """Add staging dataset to Glides springboard data."""
    ad_hoc_logger = ProcessLogger(process_name="ad_hoc_runner", dry_run=str(dry_run))
    ad_hoc_logger.log_start()

    incident_datetime = datetime.fromisoformat("2026-04-27T02:55:00")

    with tempfile.TemporaryDirectory() as tmpdir:
        download_file(
            "mbta-ctd-dataplatform-staging-springboard/" + glides_vehicle_assignment.prefix, f"{tmpdir}/staging.parquet"
        )
        staging_file = pl.scan_parquet(f"{tmpdir}/staging.parquet")

        download_file(glides_vehicle_assignment.s3_uri, f"{tmpdir}/existing.parquet")
        existing_file = pl.scan_parquet(f"{tmpdir}/existing.parquet")

        new_frame = pl.concat(
            [
                staging_file.filter(pl.col("time") <= pl.lit(incident_datetime)),
                existing_file.filter(pl.col("time") > pl.lit(incident_datetime)),
            ]
        )

        VehicleTripAssignmentTable.validate(new_frame, cast=True, eager=False).sink_parquet(
            f"{tmpdir}/new_glides_vehicle_assignment.parquet"
        )

        if not dry_run:
            result = replace_remote_parquet(
                f"{tmpdir}/new_glides_vehicle_assignment.parquet", glides_vehicle_assignment.s3_uri
            )
            if not result:
                raise LampInvalidProcessingError
        else:
            existing_rows = read_metadata(glides_vehicle_assignment.s3_uri).num_rows
            new_rows = read_metadata(f"{tmpdir}/new_glides_vehicle_assignment.parquet").num_rows
            ad_hoc_logger.add_metadata(existing_rows=existing_rows, new_rows=new_rows)

    ad_hoc_logger.log_complete()
