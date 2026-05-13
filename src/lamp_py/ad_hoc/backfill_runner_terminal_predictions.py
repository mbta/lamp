# pylint: disable=too-many-positional-arguments,too-many-arguments, too-many-locals, redefined-outer-name, R0801

import os
from datetime import date, datetime, timedelta
from queue import Queue

import polars as pl

from lamp_py.ingestion.convert_gtfs_rt_fullset import GtfsRtFullPartitionConverter
from lamp_py.ingestion.backfill.delta_reingestion import delta_reingestion_runner
from lamp_py.ingestion.converter import ConfigType

from lamp_py.runtime_utils.remote_files import S3_ARCHIVE, S3Location

if __name__ == "__main__":
    local_tmp_output = "/tmp/gtfs-rt-continuous/"

    if not os.path.exists(local_tmp_output):
        os.makedirs(local_tmp_output)

    start = datetime(2026, 3, 3, 0, 0, 0)
    end = datetime(2026, 3, 3, 0, 0, 0)
    config=ConfigType.RT_TRIP_UPDATES    

    final_output_path = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES_FULLSET")
    final_output_path_daily = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES")

    # construct and run converter once per day
    converter = GtfsRtFullPartitionConverter(
        config_type=config,
        metadata_queue=Queue(),
        local_output_location=local_tmp_output,
        # remote_output_location=final_output_path_daily,
        max_workers=16,
        time_chunk_minutes=15,
        move_source_on_completion=True
    )

    delta_reingestion_runner(
        start_date=start.date(),
        end_date=end.date(),
        local_output_location=local_tmp_output,
        final_output_path=final_output_path,
        converter=converter,
        in_filter="mbta.com_realtime_TripUpdates_enhanced.json.gz",
        bucket=S3_ARCHIVE,
    )
