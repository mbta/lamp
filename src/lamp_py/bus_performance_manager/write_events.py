from datetime import date, timedelta
import os
import tempfile
from typing import Optional

import pyarrow.parquet as pq

from lamp_py.bus_performance_manager.event_files import event_files_to_load, service_date_from_filename
from lamp_py.bus_performance_manager.events_metrics import run_bus_performance_pipeline
from lamp_py.runtime_utils.lamp_exception import LampExpectedNotFoundError, LampInvalidProcessingError
from lamp_py.runtime_utils.remote_files import bus_events, bus_operator_mapping
from lamp_py.runtime_utils.remote_files import VERSION_KEY
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import get_last_modified_object, object_exists, upload_file
from lamp_py.tableau.jobs.bus_performance import BUS_RECENT_NDAYS
from lamp_py.utils.date_range_builder import build_data_range_paths


# pylint: disable=R0914
def write_bus_metrics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    **debug_flags: dict[str, bool],
) -> None:
    """
    Write bus-performance parquet files to S3 for latest service dates needing to be processed
    If optional start_date or end_date are provided, re-processes the resulting date range
    disregarding last-processed check
    If optional write_local_only is provided, does not write to S3 bucket - only writes to local temp

    Inputs:
        start_date: Optional | beginning date of bus metrics to process
        end_date: Optional | end date of bus metrics to process
        write_local_only: Optional | if true, does not write to S3, only to local disk

    Outputs:
        None

    """
    logger = ProcessLogger("write_bus_metrics")
    logger.log_start()

    event_files = event_files_to_load(start_date, end_date)
    logger.add_metadata(service_date_count=len(event_files))

    for service_date in event_files.keys():
        gtfs_files = event_files[service_date]["gtfs_rt"]
        tm_files = event_files[service_date]["transit_master_stop_crossing"]
        tm_files_work_pieces = event_files[service_date]["transit_master_daily_work_piece"]

        day_logger = ProcessLogger(
            "write_bus_metrics_day",
            service_date=service_date,
            gtfs_file_count=len(gtfs_files),
            tm_file_count=len(tm_files),
            tm_work_piece=len(tm_files_work_pieces),
        )
        day_logger.log_start()

        # need gtfs_rt and tm files to run process
        if len(gtfs_files) == 0:
            day_logger.log_failure(FileNotFoundError(f"No RT_VEHICLE_POSITION files found for {service_date}"))
            continue

        if len(tm_files) == 0:
            day_logger.log_warning(FileNotFoundError(f"No TransitMaster files found for {service_date}"))
            continue

        if len(tm_files_work_pieces) == 0:
            day_logger.log_warning(FileNotFoundError(f"No Daily Work Piece files found for {service_date}"))
            continue

        # do bus events
        try:
            events_df, operator_id_mapping = run_bus_performance_pipeline(
                service_date, gtfs_files, tm_files, tm_files_work_pieces, **debug_flags
            )

            day_logger.add_metadata(bus_performance_rows=events_df.shape[0])

            output_filepath_bus_metrics = f"{service_date.strftime('%Y%m%d')}.parquet"
            output_filepath_operator_mapping = f"operator_map_pii_{service_date.strftime('%Y%m%d')}.parquet"

            if debug_flags.get("write_local_only"):
                events_df.write_parquet(os.path.join("/tmp/", output_filepath_bus_metrics), use_pyarrow=True)
                operator_id_mapping.write_parquet(
                    os.path.join("/tmp/", output_filepath_operator_mapping), use_pyarrow=True
                )
            else:
                with tempfile.TemporaryDirectory() as tempdir:
                    events_df.write_parquet(os.path.join(tempdir, output_filepath_bus_metrics), use_pyarrow=True)
                    operator_id_mapping.write_parquet(
                        os.path.join(tempdir, output_filepath_operator_mapping), use_pyarrow=True
                    )

                    upload_file(
                        file_name=os.path.join(tempdir, output_filepath_bus_metrics),
                        object_path=os.path.join(bus_events.s3_uri, output_filepath_bus_metrics),
                        extra_args={"Metadata": {VERSION_KEY: bus_events.version}},
                    )
                    upload_file(
                        file_name=os.path.join(tempdir, output_filepath_operator_mapping),
                        object_path=os.path.join(bus_operator_mapping.s3_uri, output_filepath_operator_mapping),
                        extra_args={"Metadata": {VERSION_KEY: bus_operator_mapping.version}},
                    )
        except LampExpectedNotFoundError as exception:
            # service_date not found = ExpectedNotFound
            day_logger.add_metadata(skipped_day=exception)
            continue
        except LampInvalidProcessingError as exception:
            # num service date > 1 = InvalidProcessing (this should never happen)
            day_logger.log_failure(exception)
        except Exception as exception:
            day_logger.log_failure(exception)

        day_logger.log_complete()

    logger.log_complete()


def regenerate_bus_metrics_recent(num_days: int = BUS_RECENT_NDAYS, **debug_flags: dict[str, bool]) -> None:
    """
    Check if latest updated schema is the same for all files in a recent num_days
    range. If not, regenerate the num_days range (== BUS_RECENT date range)
    so BUS_RECENT tableau events has all columns needed

    Check if any days in the num_days range are missing - if so, regenerate

    input:
        num_days: number of days to regenerate with write_bus_metrics
    """
    regenerate_bus_metrics_logger = ProcessLogger("regenerate_bus_metrics_recent")
    regenerate_bus_metrics_logger.log_start()

    # get date without time so comparisons will match for the entire day
    latest_event_file = get_last_modified_object(
        bucket_name=bus_events.bucket,
        file_prefix=bus_events.prefix,
        version=bus_events.version,
    )

    if latest_event_file:
        today = service_date_from_filename(latest_event_file["s3_obj_path"])

        # check if schema match - regenerate if no match
        if today:
            start_day = today - timedelta(days=num_days)
            latest_path = os.path.join(bus_events.s3_uri, f"{today.strftime('%Y%m%d')}.parquet")
            prior_path = os.path.join(bus_events.s3_uri, f"{start_day.strftime('%Y%m%d')}.parquet")

            regenerate_days = False

            prior_schema = pq.read_schema(prior_path)
            latest_schema = pq.read_schema(latest_path)

            # if the two schemas don't match, assume that changes have been made,
            # and regenerate all latest days
            if latest_schema != prior_schema:
                write_bus_metrics(start_date=start_day, end_date=today, **debug_flags)
                regenerate_days = True
            regenerate_bus_metrics_logger.add_metadata(
                regenerated=regenerate_days, start_date=prior_path, end_date=latest_path
            )

            # check if any file paths are missing - regenerate all if any in the expected range are missing
            expected_bus_events_paths = build_data_range_paths(
                bus_events.s3_uri + "/{yy}{mm:02d}{dd:02d}.parquet", start_date=start_day, end_date=today
            )
            expected_bus_operator_mapping_paths = build_data_range_paths(
                bus_operator_mapping.s3_uri + "/operator_mapping_pii_{yy}{mm:02d}{dd:02d}.parquet",
                start_date=start_day,
                end_date=today,
            )

            for day in zip(expected_bus_events_paths, expected_bus_operator_mapping_paths):
                if not object_exists(day[0]) or not object_exists(day[1]):
                    regenerate_bus_metrics_logger.add_metadata(regenerate_missing_day=day[0])
                    missing_date = service_date_from_filename(day[0])
                    write_bus_metrics(start_date=missing_date, end_date=missing_date, **debug_flags)

    regenerate_bus_metrics_logger.log_complete()
