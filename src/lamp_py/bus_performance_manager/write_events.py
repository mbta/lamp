import os
import tempfile

from lamp_py.bus_performance_manager.event_files import event_files_to_load
from lamp_py.bus_performance_manager.events_metrics import bus_performance_metrics
from lamp_py.runtime_utils.lamp_exception import LampExpectedNotFoundError, LampInvalidProcessingError
from lamp_py.runtime_utils.remote_files import bus_events
from lamp_py.runtime_utils.remote_files import VERSION_KEY
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import upload_file


def write_bus_metrics() -> None:
    """
    Write bus-performance parquet files to S3 for service dates neeing to be processed
    """
    logger = ProcessLogger("write_bus_metrics")
    logger.log_start()

    event_files = event_files_to_load()
    logger.add_metadata(service_date_count=len(event_files))

    for service_date in event_files.keys():
        gtfs_files = event_files[service_date]["gtfs_rt"]
        tm_files = event_files[service_date]["transit_master"]

        day_logger = ProcessLogger(
            "write_bus_metrics_day",
            service_date=service_date,
            gtfs_file_count=len(gtfs_files),
            tm_file_count=len(tm_files),
        )
        day_logger.log_start()

        # need gtfs_rt files to run process
        if len(gtfs_files) == 0:
            day_logger.log_failure(FileNotFoundError(f"No RT_VEHICLE_POSITION files found for {service_date}"))
            continue

        try:
            events_df = bus_performance_metrics(service_date, gtfs_files, tm_files)
            day_logger.add_metadata(bus_performance_rows=events_df.shape[0])

            with tempfile.TemporaryDirectory() as tempdir:
                write_file = f"{service_date.strftime('%Y%m%d')}.parquet"
                events_df.write_parquet(os.path.join(tempdir, write_file), use_pyarrow=True)

                upload_file(
                    file_name=os.path.join(tempdir, write_file),
                    object_path=os.path.join(bus_events.s3_uri, write_file),
                    extra_args={"Metadata": {VERSION_KEY: bus_events.version}},
                )

            # if any day succeeds, flip true - triggers upload to tableau
            successful_metrics = True

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
