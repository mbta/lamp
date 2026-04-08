from datetime import datetime, timezone

from aiohttp.hdrs import FROM

from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.daily.config import END_HOUR, START_HOUR
from datetime import timedelta
import polars as pl
import os
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE
from lamp_py.utils.filter_bank import HeavyRailFilter, LightRailFilter
from tests.bus_performance_manager.test_gtfs import S3Location
from lamp_py.ingestion.backfill.delta_reingestion import delta_reingestion_runner
from lamp_py.ingestion.backfill.convert_gtfs_rt_fullset import GtfsRtTripsFullSetConverter, delta_reingestion_runner
from queue import Queue


def within_daily_processing_window() -> bool:
    now = datetime.now(timezone.utc)
    hour = now.hour
    return START_HOUR <= hour < END_HOUR


def reprocess_trip_updates_terminal_prediction() -> bool:

    all_terminal_stops = LightRailFilter.terminal_stop_ids + HeavyRailFilter.terminal_stop_ids
    polars_filter = pl.col("trip_update.trip.route_id").is_in(
        ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E", "Mattapan"]
    ) & pl.col("trip_update.stop_time_update.stop_id").is_in(all_terminal_stops)

    pass


def reprocess_trip_updates() -> bool:
    """
    Full encapsulated method to call all of this backfill job
    """

    local_tmp_output = "/tmp/gtfs-rt-continuous/"
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    if not os.path.exists(local_tmp_output):
        os.makedirs(local_tmp_output)

    final_output_path = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES_FULLSET")

    # construct and run converter once per day
    converter = GtfsRtTripsFullSetConverter(
        config_type=ConfigType.RT_TRIP_UPDATES,
        metadata_queue=Queue(),
        output_location=local_tmp_output,
        max_workers=4,
        verbose=True,
    )

    delta_reingestion_runner(
        start_date=yesterday,
        end_date=yesterday,
        local_output_location=local_tmp_output,
        final_output_path=final_output_path,
        converter_template_instance=converter,
    )

    return True