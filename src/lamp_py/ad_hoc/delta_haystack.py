# pylint: disable=too-many-positional-arguments,too-many-arguments, too-many-locals, redefined-outer-name, R0801

from xmlrpc import client

import boto3
import json
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from queue import Queue
from typing import Dict, Iterable, List, Optional
import pyarrow
import pyarrow.dataset as pd
import pyarrow.parquet as pq
import dataframely as dy
import polars as pl
from threading import current_thread

from lamp_py.flashback.events import unnest_vehicle_positions
from lamp_py.ingestion.config_rt_trip import RtTripDetail
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter, TableData
from lamp_py.ingestion.converter import ConfigType

from lamp_py.aws.s3 import dt_from_obj_path, file_list_from_s3, upload_file
from lamp_py.runtime_utils.remote_files import springboard_rt_vehicle_positions
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE, S3Location

from lamp_py.tableau.conversions import convert_gtfs_rt_vehicle_position
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.jobs.lamp_jobs import GTFS_RT_TABLEAU_PROJECT
from lamp_py.utils.filter_bank import FilterBankRtVehiclePositions
import gzip

# read everything from a day in archive
# parse it json like before...rerun processing, spit out tmp file
# ensure the file name is in different partition
# output back to springboard

# is this a good way to do it?
# kind of want...merge sort or something.
# process files is running 1 worker for each type. so this will be slow.
# want to run N workers for all of the files.

# test gz to pyarrow vs gz to polars - time it

# specialize GtfsRtConverter


# pylint disable=too-many-arguments
class GtfsRtTripsAdHocConverter(GtfsRtConverter):
    """
    Converter that handles GTFS Real Time JSON data

    https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    """

    def __init__(
        self,
        config_type: ConfigType,
        metadata_queue: Queue[Optional[str]],
        output_location: str,
        polars_filter: pl.Expr | None = None,  # default to true - which will essentially not filter
        max_workers: int = 4,
    ) -> None:
        GtfsRtConverter.__init__(self, config_type, metadata_queue, max_workers=max_workers)

        self.detail = RtTripDetail()

        self.tmp_folder = output_location

        self.data_parts: Dict[datetime, TableData] = {}

        self.error_files: List[str] = []
        self.archive_files: List[str] = []

        if polars_filter is None:
            polars_filter = pl.lit(None)
        self.filter = polars_filter

    def convert(self) -> None:

        process_logger = ProcessLogger(
            "parquet_table_creator",
            table_type="gtfs-rt",
            config_type=str(self.config_type),
            file_count=len(self.files),
        )
        process_logger.log_start()

        table_count = 0
        try:
            for table in self.process_files():
                if table.num_rows == 0:
                    continue
                partition_dt = self.partition_dt(table)

                local_path = os.path.join(
                    self.tmp_folder,
                    LAMP,
                    str(self.config_type),
                    f"year={partition_dt.year}",
                    f"month={partition_dt.month}",
                    f"day={partition_dt.day}",
                    f"{partition_dt.isoformat()}_part_{str(table_count)}.parquet",
                )
                os.makedirs(Path(local_path).parent, exist_ok=True)

                self.write_local_pq(table, local_path)

                pool = pyarrow.default_memory_pool()
                pool.release_unused()
                table_count += 1
                process_logger.add_metadata(table_count=table_count)

        except Exception as exception:
            process_logger.log_failure(exception)
        else:
            process_logger.log_complete()
        finally:
            self.data_parts = {}
            # self.move_s3_files()
            # self.clean_local_folders()

    def write_local_pq(self, table: pyarrow.Table, local_path: str) -> None:
        """
        just write the file out..
        """
        print("running GtfsRtTripUpdatesConverter::write_local_pq")

        writer = pq.ParquetWriter(local_path, schema=table.schema, compression="zstd", compression_level=3)
        writer.write_table(table)
        writer.close()

    def process_files(self) -> Iterable[pyarrow.table]:
        """
        iterate through all of the files to be converted - apply a polars filter to narrow results at the source to reduce write churn - filter at json.gz input level

        only yield a new table when table size crosses over min_rows of yield_check
        """

        process_logger = ProcessLogger(
            "create_pyarrow_tables",
            config_type=str(self.config_type),
        )
        process_logger.log_start()

        with ThreadPoolExecutor(max_workers=self.max_workers, initializer=self.thread_init) as pool:
            for result_dt, result_filename, rt_data in pool.map(self.gz_to_pyarrow, self.files):
                # errors in gtfs_rt conversions are handled in the gz_to_pyarrow
                # function. if one is encountered, the datetime will be none. log
                # the error and move on to the next file.
                if result_dt is None:
                    logging.error(
                        "skipping processing: %s",
                        result_filename,
                    )
                    continue

                # create key for self.data_parts dictionary
                dt_part = datetime(
                    year=result_dt.year,
                    month=result_dt.month,
                    day=result_dt.day,
                )
                # create new self.table_groups entry for key if it doesn't exist
                if dt_part not in self.data_parts:
                    self.data_parts[dt_part] = TableData()
                    tmp = self.detail.transform_for_write(rt_data)
                    df = pl.from_arrow(tmp)
                    self.data_parts[dt_part].table = df.filter(self.filter).to_arrow()  # type: ignore

                else:
                    self.data_parts[dt_part].table = pyarrow.concat_tables(
                        [
                            self.data_parts[dt_part].table,
                            pl.from_arrow(self.detail.transform_for_write(rt_data)).filter(self.filter).to_arrow(),  # type: ignore
                        ]
                    )

                self.data_parts[dt_part].files.append(result_filename)

                yield from self.yield_check(process_logger)

        # yield any remaining tables
        yield from self.yield_check(process_logger, min_rows=-1)

        process_logger.add_metadata(file_count=0, number_of_rows=0)
        process_logger.log_complete()


def delta_haystack_search_(
    start_date: date,
    end_date: date,
    final_output_path: S3Location,
    polars_filter: pl.Expr | None = None,
    max_workers: int = 4,
    local_output_location: str = "/tmp/gtfs-rt-continuous/",
) -> None:
    """
    Docstring for delta_reingestion_runner

    :param start_date: start of reingestion range
    :type start_date: date
    :param end_date: end of reingestion range
    :type end_date: date
    :param final_output_base: final resting prefix-path of output artifacts
    :type final_output_base: S3Location
    :param polars_filter: optional polars expression filter to be applied
    :type polars_filter: pl.Expr | None
    :param max_workers: number of worker threads to ingest delta files with - default is 4
    :type max_workers: int
    :param local_output_location: temporary working directory to place outputs
    :type local_output_location: str

    """

    #### Stage 1: local to local (MANY to many)

    # construct and run converter once per day
    converter = GtfsRtTripsAdHocConverter(
        config_type=ConfigType.RT_TRIP_UPDATES,
        metadata_queue=Queue(),
        output_location=local_output_location,
        polars_filter=polars_filter,
        max_workers=max_workers,
    )
    converter.add_files(file_list)
    # this outputs to local output_location=tmp_output_location
    converter.convert()

    ## Stage 2: local to local (many to 1)

    # Define the path to your input Parquet files (can use a glob pattern)
    converter_output_path = (
        f"{local_output_location}/lamp/RT_TRIP_UPDATES/year={cur_date.year}/month={cur_date.month}/day={cur_date.day}/"
    )
    consolidated_parquet_output_file = f"/tmp/{cur_date.year}_{cur_date.month}_{cur_date.day}.parquet"

    # Create a dataset from the input files
    ds = pd.dataset(converter_output_path, format="parquet")

    with pq.ParquetWriter(
        consolidated_parquet_output_file, schema=ds.schema, compression="zstd", compression_level=3
    ) as writer:
        for batch in ds.to_batches(batch_size=512 * 1024):
            writer.write_batch(batch)

    #### Stage 3: local to remote (one to one)

    # upload local to remote
    upload_file(
        consolidated_parquet_output_file,
        consolidated_parquet_output_file.replace(
            "/tmp/", f"{final_output_path.s3_uri}/year={cur_date.year}/month={cur_date.month}/day={cur_date.day}/"
        ),
    )

    class MinimalSchema(dy.Schema):
        "Intersection of descendant rail schemas."
        trip_id = dy.String(nullable=True, alias="trip_update.trip.trip_id")
        start_date = dy.String(nullable=True, alias="trip_update.trip.start_date")
        feed_timestamp = dy.Datetime(nullable=True)
        departure_time = dy.Datetime(nullable=True, alias="trip_update.stop_time_update.departure.time")

    final_output_base_tu = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES_20251024_20251124.parquet")

    hyperjob = FilteredHyperJob(
        remote_input_location=final_output_path,
        remote_output_location=final_output_base_tu,
        start_date=start_date,
        end_date=end_date,
        processed_schema=MinimalSchema.to_pyarrow_schema(),
        dataframe_filter=adhoc_convert_tz_filter_revenue_only,
        parquet_filter=None,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    hyperjob.run_parquet()
    hyperjob.create_local_hyper()


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


def delta_haystack_search() -> None:
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

    delta_haystack_search(
        start_date=start,
        end_date=end,
        local_output_location=local_tmp_output,
        final_output_path=final_output_path,
        polars_filter=polars_filter,
    )

    logger = ProcessLogger("backfiller")
    logger.log_start()

    cur_date = start_date

    while cur_date <= end_date:
        prefix = (
            os.path.join(
                LAMP,
                "delta",
                cur_date.strftime("%Y"),
                cur_date.strftime("%m"),
                cur_date.strftime("%d"),
            )
            + "/"
        )

        file_list = file_list_from_s3(
            S3_ARCHIVE,
            prefix,
            in_filter="mbta.com_realtime_TripUpdates_enhanced.json.gz",
        )

        print(len(file_list))

        cur_date = cur_date + timedelta(days=1)


def get_all_delta_files_matching(start_dt: datetime | date, end_dt: datetime | date, delta_prefix: str) -> pl.DataFrame:
    """
    glob delta files matching a date range -

    start_date and end_date are inclusive, and form the prefix for the s3 path
    in_filter - arbitrary string search within the path

    example path:
    s3://mbta-ctd-dataplatform-archive/lamp/delta/2026/03/02/2026-03-02T00:00:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz

    """
    logger = ProcessLogger("backfiller")
    logger.log_start()

    cur_date = start_dt

    if isinstance(cur_date, datetime):
        cur_date = cur_date.date()
    if isinstance(end_dt, datetime):
        end_date = end_dt.date()
    # prefix from date

    file_list_all = pl.DataFrame(schema={"file": pl.Utf8, "timestamp": pl.Datetime, "prefix": pl.Utf8, "key": pl.Utf8})

    while cur_date <= end_date:
        delta_prefix = (
            os.path.join(
                LAMP,
                "delta",
                cur_date.strftime("%Y"),
                cur_date.strftime("%m"),
                cur_date.strftime("%d"),
            )
            + "/"
        )

        file_list = file_list_from_s3(
            S3_ARCHIVE,
            delta_prefix,
            in_filter=delta_prefix,
        )

        file_list_df = (
            pl.DataFrame({"file": file_list})
            .with_columns(
                timestamp=pl.col("file").str.slice(57, 20).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ"),
                prefix=pl.lit(S3_ARCHIVE),
                key=pl.col("file").str.slice(5 + len(S3_ARCHIVE) + 1),
                # (pl.col("timestamp") >= pl.lit(start_dt)) & (pl.col("timestamp") <= pl.lit(end_dt))
            )
            .filter([pl.col("timestamp") >= start_dt, pl.col("timestamp") <= end_dt])
        )
        file_list_all = pl.concat([file_list_all, file_list_df])
        print(len(file_list_df))

        cur_date = cur_date + timedelta(days=1)

    return file_list_all


def process_delta_files(file_list: pl.DataFrame, polars_filter: pl.Expr) -> pl.DataFrame:

    all_files = file_list["file"].to_list()

    def gz_json_to_polars_df(filename, filter) -> bool:
        """
        read gz file from s3, convert to polars df
        """
        # read gz file from s3
        try:
            file_system = current_thread().__dict__["file_system"]
            filename = filename.replace("s3://", "")

            # some of our older files are named incorrectly, with a simple
            # .json suffix rather than a .json.gz suffix. in those cases, the
            # s3 open_input_stream is unable to deduce the correct compression
            # algo and fails with a UnicodeDecodeError. catch this failure and
            # retry using a gzip compression algo. (EAFP Style)
            try:
                with file_system.open_input_stream(filename) as file:
                    json_data = json.load(file)
            except UnicodeDecodeError as _:
                with file_system.open_input_stream(filename, compression="gzip") as file:
                    json_data = json.load(file)

            # convert json to polars df
            return pl.DataFrame(json_data["entity"]).filter(filter).height > 0
        except FileNotFoundError as _:
            return False
        except Exception as _:
            # self.thread_init()
            # return (None, filename, None)
            return False

    gz_json_to_polars_df
    # file_list = file_list.with_columns(polars_filter.alias('filter'), pl.struct("file", "filter").alias("processing"))
    file_listsss = (
        file_list.with_columns(
            pl.col("processing").map_batches(gz_json_to_polars_df, return_dtype=bool).alias("matches")
        )
        .filter(pl.col("matches"))
        .select("file")
        .to_series()
        .to_list()
    )

    return file_listsss


def process_s3_json_file(bucket_name, object_key, polars_filter) -> pl.DataFrame:

    try:
        client = boto3.client("s3", region_name="us-east-1")

        response = client.get_object(Bucket=bucket_name, Key=object_key)

        data = gzip.decompress(response["Body"].read())
        json_data = json.loads(data.decode("utf-8"))
        df = pl.DataFrame(json_data).select("entity")
        df = df.select("entity").unnest("entity").unnest("vehicle").rename({"id": "global_id"}).unnest("vehicle")
        if df.select("label").filter(pl.col("label").str.contains("LF")).height > 0:
            print(f"Found! {object_key} ")
        # Perform further processing...
        return json_data
    except Exception as e:
        print(f"Error: {e}")


# Example of running multiple tasks concurrently
def find_in_delta(files_to_process, polars_filter):

    tasks = [
        process_s3_json_file(bucket, key, polars_filter)
        for bucket, key in zip(files_to_process["prefix"], files_to_process["key"])
    ]


def generate_all_candidate_files(start_dt: datetime, end_dt: datetime, delta_suffix: str) -> pl.DataFrame:
    """
    generate a dataframe of all candidate files in the delta haystack - this is the search space for the haystack search
    """
    assert (end_dt - start_dt).total_seconds() < 86400, "Search window must be less than 1 day"

    all_dt = [start_dt + timedelta(seconds=i) for i in range(int((end_dt - start_dt).total_seconds()) + 1)]
    delta_key = [
        os.path.join(LAMP, "delta", dt.strftime("%Y/%m/%d/"), f"{dt.isoformat()}Z_{delta_suffix}") for dt in all_dt
    ]
    full_path = [os.path.join(S3_ARCHIVE, key) for key in delta_key]

    return pl.DataFrame({"file": full_path, "timestamp": all_dt, "prefix": S3_ARCHIVE, "key": delta_key})


if __name__ == "__main__":

    # search space
    # 7:06:14
    start = datetime(2026, 3, 2, 19, 6, 0)
    end = datetime(2026, 3, 2, 19, 7, 0)

    # file_list = get_all_delta_files_matching(start, end, delta_prefix="mbta.com_realtime_VehiclePositions_enhanced.json.gz")
    file_list = generate_all_candidate_files(
        start, end, delta_suffix="https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz"
    )
    # file_list = pl.read_parquet('file_list.parquet')
    polars_filter = pl.col("vehicle.vehicle.id").str.contains("DF")

    found_delta = find_in_delta(files_to_process=file_list, polars_filter=polars_filter)
    # delta_haystack_search()
    # if True:
    #     import json
    #     # with open('file_list.json', 'w') as fout:
    #     #     json.dump(file_list, fout)
    #     with open('file_list.json', 'r') as fin:
    #         file_list = json.load(fin)

    # process_delta_files(file_list, polars_filter)
