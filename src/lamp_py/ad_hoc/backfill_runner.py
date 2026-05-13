# pylint: disable=too-many-positional-arguments,too-many-arguments, too-many-locals, redefined-outer-name, R0801

import os
from datetime import date, datetime, timedelta
from queue import Queue

import dataframely as dy
import polars as pl

from lamp_py.ingestion.convert_gtfs_rt_fullset import GtfsRtFullPartitionConverter
from lamp_py.ingestion.converter import ConfigType

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.runtime_utils.remote_files import springboard_rt_vehicle_positions
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE, S3Location

from lamp_py.tableau.conversions import convert_gtfs_rt_vehicle_position
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.jobs.lamp_jobs import GTFS_RT_TABLEAU_PROJECT
from lamp_py.utils.filter_bank import FilterBankRtVehiclePositions


# def delta_reingestion_runner(
#     start_date: date,
#     end_date: date,
#     final_output_path: S3Location,
#     polars_filter: pl.Expr | None = None,
#     max_workers: int = 4,
#     local_output_location: str = "/tmp/gtfs-rt-continuous/",
# ) -> None:
#     """
#     Docstring for delta_reingestion_runner

#     :param start_date: start of reingestion range
#     :type start_date: date
#     :param end_date: end of reingestion range
#     :type end_date: date
#     :param final_output_path: final resting prefix-path of output artifacts
#     :type final_output_path: S3Location
#     :param polars_filter: optional polars expression filter to be applied
#     :type polars_filter: pl.Expr | None
#     :param max_workers: number of worker threads to ingest delta files with - default is 4
#     :type max_workers: int
#     :param local_output_location: temporary working directory to place outputs
#     :type local_output_location: str

#     """
#     logger = ProcessLogger("backfiller")
#     logger.log_start()

#     cur_date = start_date

#     while cur_date <= end_date:
#         prefix = (
#             os.path.join(
#                 LAMP,
#                 "delta",
#                 cur_date.strftime("%Y"),
#                 cur_date.strftime("%m"),
#                 cur_date.strftime("%d"),
#             )
#             + "/"
#         )

#         file_list = file_list_from_s3(
#             S3_ARCHIVE,
#             prefix,
#             in_filter="mbta.com_realtime_TripUpdates_enhanced.json.gz",
#         )

#         print(len(file_list))

#         converter = GtfsRtFullPartitionConverter(
#             config_type=ConfigType.RT_TRIP_UPDATES,
#             metadata_queue=Queue(),
#             local_output_location=local_output_location,
#             remote_output_location=final_output_path,
#             polars_filter=polars_filter if polars_filter is not None else pl.lit(True),
#             max_workers=max_workers,
#         )
#         converter.add_files(file_list)
#         converter.convert()

#         cur_date = cur_date + timedelta(days=1)

#     class MinimalSchema(dy.Schema):
#         "Intersection of descendant rail schemas."
#         trip_id = dy.String(nullable=True, alias="trip_update.trip.trip_id")
#         start_date = dy.String(nullable=True, alias="trip_update.trip.start_date")
#         feed_timestamp = dy.Datetime(nullable=True)
#         departure_time = dy.Datetime(nullable=True, alias="trip_update.stop_time_update.departure.time")

#     final_output_base_tu = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES_20251024_20251124.parquet")

#     hyperjob = FilteredHyperJob(
#         remote_input_location=final_output_path,
#         remote_output_location=final_output_base_tu,
#         start_date=start_date,
#         end_date=end_date,
#         processed_schema=MinimalSchema.to_pyarrow_schema(),
#         dataframe_filter=adhoc_convert_tz_filter_revenue_only,
#         parquet_filter=None,
#         tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
#     )

#     hyperjob.run_parquet()
#     hyperjob.create_local_hyper()


def adhoc_convert_tz_filter_revenue_only(df: pl.DataFrame) -> pl.DataFrame:
    """
    Docstring for adhoc_convert_tz_filter_revenue_only

    :param df: trip_updates dataframe
    :type df: polars DataFrame
    :return: trip_updates with timezones converted and filtered to revenue only
    :rtype: polars DataFrame
    """
    # Filter data to  = TRUE, trip_update.stop_time_update.schedule_relationship != SKIPPED, and trip_update.trip.schedule_relationship != CANCELED
    df = df.with_columns(
        pl.from_epoch(pl.col("trip_update.stop_time_update.departure.time"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("trip_update.stop_time_update.arrival.time"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("trip_update.timestamp"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("feed_timestamp"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
    )

    df = df.filter(
        pl.col("trip_update.trip.revenue"),
    ).select(
        [
            "trip_update.trip.trip_id",
            "trip_update.trip.start_date",
            "trip_update.stop_time_update.departure.time",  #  - Convert from Epoch format to regular datetime
            "feed_timestamp",  # - Convert from Epoch format to regular datetime
        ]
    )

    return df


def run_backfill() -> None:
    """
    Full encapsulated method to call all of this backfill job
    """
    local_tmp_output = "/tmp/gtfs-rt-continuous"

    start = datetime(2025, 10, 24, 0, 0, 0)
    end = datetime(2025, 11, 24, 0, 0, 0)

    polars_filter = pl.col("trip_update.trip.route_id").is_in(
        ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E", "Mattapan"]
    )

    final_output_path = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES_20251024_20251124")

    final_output_base_vp = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_VEHICLE_POSITION_20251024_20251124.parquet")

    rt_vp_unfiltered_hyperjob = FilteredHyperJob(
        remote_input_location=springboard_rt_vehicle_positions,
        remote_output_location=final_output_base_vp,
        start_date=start,
        end_date=end,
        processed_schema=convert_gtfs_rt_vehicle_position.VehiclePositions.to_pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_vehicle_position.apply_gtfs_rt_vehicle_positions_timezone_conversions,
        parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    rt_vp_unfiltered_hyperjob.run_parquet()
    rt_vp_unfiltered_hyperjob.create_local_hyper()

    delta_reingestion_runner(
        start_date=start,
        end_date=end,
        local_output_location=local_tmp_output,
        final_output_path=final_output_path,
        polars_filter=polars_filter,
    )


if __name__ == "__main__":
    run_backfill()
