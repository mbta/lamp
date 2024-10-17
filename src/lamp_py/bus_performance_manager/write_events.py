import os

from lamp_py.bus_performance_manager.event_files import event_files_to_load
from lamp_py.bus_performance_manager.events_metrics import bus_performance_metrics
from lamp_py.runtime_utils.remote_files import bus_events
from lamp_py.runtime_utils.process_logger import ProcessLogger


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
            write_path = os.path.join(bus_events.s3_uri, f"{service_date}_bus-performance-v1.parquet")
            events_df.write_parquet(write_path, use_pyarrow=True)
            day_logger.log_complete()
        except Exception as exception:
            day_logger.log_failure(exception)

    logger.log_complete()
