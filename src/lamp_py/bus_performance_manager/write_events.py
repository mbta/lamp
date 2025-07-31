from datetime import date, datetime, timedelta
import os
import tempfile
import zoneinfo

import pyarrow.parquet as pq

from lamp_py.bus_performance_manager.event_files import event_files_to_load
from lamp_py.bus_performance_manager.events_metrics import bus_performance_metrics
from lamp_py.runtime_utils.lamp_exception import LampExpectedNotFoundError, LampInvalidProcessingError
from lamp_py.runtime_utils.remote_files import bus_events
from lamp_py.runtime_utils.remote_files import VERSION_KEY
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import upload_file
from lamp_py.tableau.jobs.bus_performance import BUS_RECENT_NDAYS


def write_bus_metrics(
    start_date: date | None = None,
    end_date: date | None = None,
    write_local_only: bool = False,
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

            write_file = f"{service_date.strftime('%Y%m%d')}.parquet"

            if write_local_only:
                events_df.write_parquet(os.path.join("/tmp/", write_file), use_pyarrow=True)
            else:
                with tempfile.TemporaryDirectory() as tempdir:
                    events_df.write_parquet(os.path.join(tempdir, write_file), use_pyarrow=True)

                    upload_file(
                        file_name=os.path.join(tempdir, write_file),
                        object_path=os.path.join(bus_events.s3_uri, write_file),
                        extra_args={"Metadata": {VERSION_KEY: bus_events.version}},
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


def regenerate_bus_metrics_recent(num_days: int = BUS_RECENT_NDAYS) -> None:
    """
    Check if latest updated schema is the same for all files in a recent num_days
    range. If not, regenerate the num_days range (== BUS_RECENT date range)
    so BUS_RECENT tableau events has all columns needed

    input:
        num_days: number of days to regenerate with write_bus_metrics
    """

    # get date without time so comparisons will match for the entire day
    # cast this to eastern because datetime.now() UTC will be 4/5 hours ahead,
    # and thus the bus_recent events for "today" would not exist yet.
    today_eastern = datetime.now(tz=zoneinfo.ZoneInfo("US/Eastern")).date()
    start_day = today_eastern - timedelta(days=num_days)
    latest_path = os.path.join(bus_events.s3_uri, f"{today_eastern.strftime('%Y%m%d')}.parquet")
    prior_path = os.path.join(bus_events.s3_uri, f"{start_day.strftime('%Y%m%d')}.parquet")

    regenerate_bus_metrics_logger = ProcessLogger("regenerate_bus_metrics_recent")
    regenerate_bus_metrics_logger.log_start()

    regenerate_days = False

    latest_schema = pq.read_schema(latest_path)
    prior_schema = pq.read_schema(prior_path)

    # if the two schemas don't match, assume that changes have been made,
    # and regenerate all latest days as long as the new schema is strictly a superset of the
    # old one i.e. new columns added, none removed
    # if the schema is not a subset, downstream Tableau joining will fail and
    # the developer will see the error there
    if latest_schema != prior_schema and set(prior_schema).issubset(set(latest_schema)):
        write_bus_metrics(start_date=start_day, end_date=today_eastern)
        regenerate_days = True
    regenerate_bus_metrics_logger.add_metadata(regenerated=regenerate_days)
    regenerate_bus_metrics_logger.log_complete()
