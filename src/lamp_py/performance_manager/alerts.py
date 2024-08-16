import os
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone

import pandas
import pyarrow
import pyarrow.dataset as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc
import sqlalchemy as sa
from dateutil.relativedelta import relativedelta

from lamp_py.aws.s3 import (
    download_file,
    read_parquet,
    upload_file,
    version_check,
)

from lamp_py.postgres.metadata_schema import MetadataLog
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import (
    S3_PUBLIC,
    LAMP,
)

from .gtfs_utils import BOSTON_TZ


class AlertsS3Info:
    """S3 Constant info for Alerts Parquet File"""

    s3_path: str = "s3://" + os.path.join(
        S3_PUBLIC, LAMP, "tableau", "alerts", "LAMP_RT_ALERTS.parquet"
    )
    version_key: str = "lamp_version"
    file_version: str = "1.1.0"

    parquet_schema: pyarrow.schema = pyarrow.schema(
        [
            ("id", pyarrow.int64()),
            ("cause", pyarrow.string()),
            ("cause_detail", pyarrow.string()),
            ("effect", pyarrow.string()),
            ("effect_detail", pyarrow.string()),
            ("severity_level", pyarrow.string()),
            ("severity", pyarrow.int8()),
            ("alert_lifecycle", pyarrow.string()),
            ("duration_certainty", pyarrow.string()),
            ("header_text.translation.text", pyarrow.string()),
            ("description_text.translation.text", pyarrow.string()),
            ("service_effect_text.translation.text", pyarrow.string()),
            ("timeframe_text.translation.text", pyarrow.string()),
            ("recurrence_text.translation.text", pyarrow.string()),
            ("created_datetime", pyarrow.timestamp("us")),
            ("created_timestamp", pyarrow.int64()),
            ("last_modified_datetime", pyarrow.timestamp("us")),
            ("last_modified_timestamp", pyarrow.int64()),
            ("last_push_notification_datetime", pyarrow.timestamp("us")),
            ("last_push_notification_timestamp", pyarrow.int64()),
            ("closed_datetime", pyarrow.timestamp("us")),
            ("closed_timestamp", pyarrow.int64()),
            ("active_period.start_datetime", pyarrow.timestamp("us")),
            ("active_period.start_timestamp", pyarrow.int64()),
            ("active_period.end_datetime", pyarrow.timestamp("us")),
            ("active_period.end_timestamp", pyarrow.int64()),
            ("informed_entity.route_id", pyarrow.string()),
            ("informed_entity.route_type", pyarrow.int8()),
            ("informed_entity.direction_id", pyarrow.int8()),
            ("informed_entity.stop_id", pyarrow.string()),
            ("informed_entity.facility_id", pyarrow.string()),
            ("informed_entity.activities", pyarrow.string()),
        ]
    )


class AlertParquetHandler:
    """
    This class handles all of the interactions with alert data thats stored as a parquet file on s3.
    """

    def __init__(self, update_alerts: bool) -> None:
        self.s3_path: str = AlertsS3Info.s3_path

        self.local_path: str = os.path.join("/tmp", "alerts.parquet")
        self.parquet_schema: pyarrow.Schema = AlertsS3Info.parquet_schema

        # flag used to upload if new data is appended to the parquet file.
        self.new_data: bool = False

        # only download the remote file if its being updated.
        if update_alerts:
            download_file(object_path=self.s3_path, file_name=self.local_path)

    def existing_id_timestamp_pairs(self) -> pandas.DataFrame:
        """
        get all unique alert id / last modified timestamp pairs in the local
        parquet file, a key that can be used to identify alerts that have
        already been processed.
        """
        columns = ["id", "last_modified_timestamp"]

        if os.path.exists(self.local_path):
            existing_alerts = pq.read_table(
                self.local_path, columns=columns
            ).to_pandas()
            existing_alerts = existing_alerts.drop_duplicates()
            return existing_alerts

        return pandas.DataFrame(columns=columns)

    def append_new_records(self, alerts: pandas.DataFrame) -> None:
        """
        append alerts to the end of the local parquet file using batches to keep memory usage lower
        """
        process_logger = ProcessLogger(
            process_name="append_new_alerts_records",
        )
        process_logger.log_start()

        alerts = alerts.reset_index(drop=True)
        alerts_table = pyarrow.Table.from_pandas(
            alerts, schema=self.parquet_schema
        )

        if alerts_table.num_rows == 0:
            process_logger.log_complete()
            return

        if os.path.exists(self.local_path):
            joined_ds = pd.dataset(
                [pd.dataset(self.local_path), pd.dataset(alerts_table)],
                schema=self.parquet_schema,
            )
        else:
            joined_ds = pd.dataset(alerts_table, schema=self.parquet_schema)

        process_logger.add_metadata(
            new_records=alerts_table.num_rows,
            total_records=joined_ds.count_rows(),
        )

        # write new alerts to a new file and then join them with the local file in batches
        new_path = os.path.join("/tmp", "new_alerts.parquet")
        row_group_count = 0

        partition_key = "active_period.start_timestamp"
        partition_key_arr = joined_ds.to_table(columns=[partition_key]).column(
            partition_key
        )

        # the start is the start of the month containing the minimum timestamp
        start = pc.min(partition_key_arr).as_py()
        start_dt = datetime.fromtimestamp(start)
        start = datetime(start_dt.year, start_dt.month, 1)

        # "now" is the maximum timestamp in the dataset
        now = pc.max(partition_key_arr).as_py()
        now = datetime.fromtimestamp(now)

        with pq.ParquetWriter(new_path, schema=self.parquet_schema) as writer:
            while start < now:
                end = start + relativedelta(months=1)
                if end < now:
                    table = joined_ds.filter(
                        (pc.field(partition_key) >= int(start.timestamp()))
                        & (pc.field(partition_key) < int(end.timestamp()))
                    ).to_table()
                else:
                    table = joined_ds.filter(
                        (pc.field(partition_key) >= int(start.timestamp()))
                    ).to_table()

                if table.num_rows > 0:
                    row_group_count += 1
                    writer.write_table(table)

                start = end

            table = joined_ds.filter(
                (pc.field(partition_key).is_null())
            ).to_table()

            if table.num_rows > 0:
                row_group_count += 1
                writer.write_table(table)

        os.replace(new_path, self.local_path)
        self.new_data = True

        process_logger.add_metadata(row_group_count=row_group_count)
        process_logger.log_complete()

    def upload_data(self) -> None:
        """
        upload the local parquet file to s3 if new data was added
        """
        if self.new_data:
            upload_file(
                file_name=self.local_path,
                object_path=self.s3_path,
                extra_args={
                    "Metadata": {
                        AlertsS3Info.version_key: AlertsS3Info.file_version
                    }
                },
            )


def extract_alerts(
    alert_files: List[str], existing_id_timestamp_pairs: pandas.DataFrame
) -> pandas.DataFrame:
    """Read alerts data from unprocessed files, remove duplicates, and set types"""
    columns = [
        "id",
        "alert.cause",
        "alert.cause_detail",
        "alert.effect",
        "alert.effect_detail",
        "alert.header_text.translation",
        "alert.description_text.translation",
        "alert.severity_level",
        "alert.severity",
        "alert.created_timestamp",
        "alert.last_modified_timestamp",
        "alert.last_push_notification_timestamp",
        "alert.closed_timestamp",
        "alert.alert_lifecycle",
        "alert.duration_certainty",
        "alert.service_effect_text.translation",
        "alert.timeframe_text.translation",
        "alert.recurrence_text.translation",
        "alert.active_period",
        "alert.informed_entity",
    ]

    rename_map = {
        "alert.cause": "cause",
        "alert.cause_detail": "cause_detail",
        "alert.effect": "effect",
        "alert.effect_detail": "effect_detail",
        "alert.header_text.translation": "header_text.translation",
        "alert.description_text.translation": "description_text.translation",
        "alert.severity_level": "severity_level",
        "alert.severity": "severity",
        "alert.created_timestamp": "created_timestamp",
        "alert.last_modified_timestamp": "last_modified_timestamp",
        "alert.last_push_notification_timestamp": "last_push_notification_timestamp",
        "alert.closed_timestamp": "closed_timestamp",
        "alert.alert_lifecycle": "alert_lifecycle",
        "alert.duration_certainty": "duration_certainty",
        "alert.service_effect_text.translation": "service_effect_text.translation",
        "alert.timeframe_text.translation": "timeframe_text.translation",
        "alert.recurrence_text.translation": "recurrence_text.translation",
        "alert.active_period": "active_period",
        "alert.informed_entity": "informed_entity",
    }

    alerts = (
        read_parquet(filename=alert_files, columns=columns)
        .rename(columns=rename_map)
        .drop_duplicates(subset=["id", "last_modified_timestamp"])
    )

    alerts["id"] = alerts["id"].astype("int64")
    alerts["cause"] = alerts["cause"].astype("string")
    alerts["cause_detail"] = alerts["cause_detail"].astype("string")
    alerts["effect"] = alerts["effect"].astype("string")
    alerts["effect_detail"] = alerts["effect_detail"].astype("string")
    alerts["severity_level"] = alerts["severity_level"].astype("string")
    alerts["severity"] = alerts["severity"].astype("int8")
    alerts["alert_lifecycle"] = alerts["alert_lifecycle"].astype("string")
    alerts["duration_certainty"] = alerts["duration_certainty"].astype("string")
    alerts["created_timestamp"] = alerts["created_timestamp"].astype("Int64")
    alerts["last_modified_timestamp"] = alerts[
        "last_modified_timestamp"
    ].astype("Int64")
    alerts["last_push_notification_timestamp"] = alerts[
        "last_push_notification_timestamp"
    ].astype("Int64")
    alerts["closed_timestamp"] = alerts["closed_timestamp"].astype("Int64")

    # perform an anti-join against existing alerts. merge with existing pairs
    # and keep only the records that are new.
    alerts = pandas.merge(
        alerts,
        existing_id_timestamp_pairs,
        on=["id", "last_modified_timestamp"],
        how="left",
        indicator=True,
    )
    alerts = alerts[alerts["_merge"] == "left_only"]
    alerts = alerts.drop(columns=["_merge"])

    return alerts


def transform_translations(alerts: pandas.DataFrame) -> pandas.DataFrame:
    """For each string field with translations, pull out the English string"""

    def process_translation(
        translations: Optional[List[Dict[str, str]]]
    ) -> Optional[str]:
        """small lambda for processing the translation"""
        if translations is None:
            return None
        for translation in translations:
            if translation["language"] == "en":
                return translation["text"]
        return None

    translation_columns = [
        "header_text",
        "description_text",
        "service_effect_text",
        "timeframe_text",
        "recurrence_text",
    ]

    drop_columns = []
    for key in translation_columns:
        translation_key = f"{key}.translation"
        alerts[f"{translation_key}.text"] = alerts[translation_key].apply(
            process_translation
        )
        drop_columns.append(translation_key)

    alerts = alerts.drop(columns=drop_columns)

    return alerts


def unix_to_est(unix_time: Optional[int]) -> Optional[datetime]:
    """
    Utility for converting a unix timestamp into a datetime object.
    Indexing errors occur when using pandas built in datetime manipulation
    functions with NaN's, so filter them out with this function instead.
    """
    if unix_time is None or pandas.isna(unix_time):
        return None

    # Create a datetime object from the Unix timestamp
    dt_utc = datetime.fromtimestamp(unix_time, tz=timezone.utc)

    # Convert to Eastern Time (ET) with DST consideration
    dt_est = dt_utc.astimezone(BOSTON_TZ)

    # Remove the timezone information
    dt_est_naive = dt_est.replace(tzinfo=None)

    return dt_est_naive


def transform_timestamps(alerts: pandas.DataFrame) -> pandas.DataFrame:
    """
    Transform all timestamps to easter standard time.
    """
    timestamp_columns = [
        "created",
        "last_modified",
        "last_push_notification",
        "closed",
    ]

    # convert all of the timestamp columns to eastern standard time
    for key in timestamp_columns:
        timestamp_key = f"{key}_timestamp"
        datetime_key = f"{key}_datetime"
        alerts[datetime_key] = alerts[timestamp_key].apply(unix_to_est)

    return alerts


def explode_active_periods(alerts: pandas.DataFrame) -> pandas.DataFrame:
    """
    The active period column holds a list of dicts that map start and end keys
    to integer timestamps. This list will be a single element for "rolling
    alerts" that don't have a determined or estimated end time. Other alerts
    may be active for multiple distinct periods and we want to create a record
    for each.
    * Explode the active period column
    * Extract the start and end timestamps
    * Convert the timestamps to datetimes
    * Remove active period columns
    """
    # pull out the active period timestamps from the dict in that column
    alerts = alerts.explode("active_period")

    def extract_start_end(
        period: Dict[str, int] | float | None
    ) -> Tuple[int | None, int | None]:
        """
        small lambda for extracting start and end timestamps

        Note that the period could be None or NaN based on source data and the
        result of the explode function, resulting in the weird looking type
        hint above.
        """
        if isinstance(period, dict):
            return period.get("start"), period.get("end")
        return None, None

    alerts[
        [
            "active_period.start_timestamp",
            "active_period.end_timestamp",
        ]
    ] = (
        alerts["active_period"].apply(extract_start_end).apply(pandas.Series)
    )

    # convert these timestamps to Int64 to avoid floating point errors
    alerts["active_period.start_timestamp"] = alerts[
        "active_period.start_timestamp"
    ].astype("Int64")
    alerts["active_period.end_timestamp"] = alerts[
        "active_period.end_timestamp"
    ].astype("Int64")

    # drop the active period list column
    alerts = alerts.drop(columns=["active_period"])

    # convert all of the timestamp columns to eastern standard time
    for base in ["start", "end"]:
        timestamp_key = f"active_period.{base}_timestamp"
        datetime_key = f"active_period.{base}_datetime"
        alerts[datetime_key] = alerts[timestamp_key].apply(unix_to_est)

    return alerts


def explode_informed_entity(alerts: pandas.DataFrame) -> pandas.DataFrame:
    """
    the 'informed_entity' column is a list of dicts describing stops
    along routes in directions that are effected by the alert. explode each
    record along this list and extract the required information into new
    columns
    """
    alerts = alerts.explode("informed_entity")

    informed_entity_keys = [
        "route_id",
        "route_type",
        "direction_id",
        "stop_id",
        "facility_id",
        "activities",
    ]

    # extract information from the informed entity
    for key in informed_entity_keys:
        full_key = f"informed_entity.{key}"
        alerts[full_key] = alerts["informed_entity"].apply(
            lambda x, k=key: None if x is None else x.get(k)
        )

    alerts = alerts.drop(columns=["informed_entity"])

    # transform the activities field from a list to a pipe delimitated string
    alerts["informed_entity.activities"] = alerts[
        "informed_entity.activities"
    ].apply(
        lambda x: (
            None
            if x is None
            else "|".join(str(item) for item in x if item is not None)
        )
    )

    # the commuter rail informed entity contains extra details that aren't
    # extracted in this transformation. those instances will appear as
    # duplicated records, so remove them.
    alerts = alerts.drop_duplicates()

    return alerts


def get_alert_files(
    md_db_manager: DatabaseManager, unprocessed_only: bool
) -> List[Dict[str, str]]:
    """
    Get unprocessed RT Alert Files from the MetadataLog table.

    @return [
        { 'pk_id': 1, 'path': 'mbta-ctd-dataplatform-springboard/lamp/RT_ALERTS/year=2024/month=1/day=4/hour=0/uuid_1-0.parquet' },
        { 'pk_id': 2, 'path': 'mbta-ctd-dataplatform-springboard/lamp/RT_ALERTS/year=2024/month=1/day=4/hour=1/uuid_2-0.parquet' },
        ...
    ]
    """
    if unprocessed_only:
        read_md = sa.select(MetadataLog.pk_id, MetadataLog.path).where(
            (MetadataLog.rail_pm_processed == sa.false())
            & (MetadataLog.path.contains("RT_ALERTS"))
        )
    else:
        read_md = sa.select(MetadataLog.pk_id, MetadataLog.path).where(
            (MetadataLog.path.contains("RT_ALERTS"))
        )

    return md_db_manager.select_as_list(read_md)


def process_alerts(md_db_manager: DatabaseManager) -> None:
    """
    Gather unprocessed alerts files from springboard, process them, and add them to the public export
    """
    process_logger = ProcessLogger("process_alerts")
    process_logger.log_start()

    version_match = version_check(
        obj=AlertsS3Info.s3_path, version=AlertsS3Info.file_version
    )

    metadata_records = get_alert_files(
        md_db_manager=md_db_manager,
        unprocessed_only=version_match,
    )

    process_logger.add_metadata(
        version_match=version_match,
        alert_file_count=len(metadata_records),
    )

    # if there are no new alerts files, exit
    if len(metadata_records) == 0:
        process_logger.log_complete()
        return

    # create a handler object that will download, append, and upload data
    parquet_handler = AlertParquetHandler(update_alerts=version_match)
    existing_id_timestamp_pairs = parquet_handler.existing_id_timestamp_pairs()

    # process up to 24 hours at a time
    chunk_size = 24
    for i in range(0, len(metadata_records), chunk_size):
        # get a months worth of files
        chunk = metadata_records[i : i + chunk_size]

        pk_ids = [record["pk_id"] for record in chunk]
        alert_files = [record["path"] for record in chunk]

        subprocess_logger = ProcessLogger(
            "process_alerts_chunk", alert_files=alert_files
        )
        subprocess_logger.log_start()

        try:
            # extract the data and transform it for publication
            alerts = extract_alerts(alert_files, existing_id_timestamp_pairs)

            if alerts.empty:
                subprocess_logger.log_complete()
                continue

            alerts = transform_translations(alerts)
            alerts = transform_timestamps(alerts)
            alerts = explode_active_periods(alerts)
            alerts = explode_informed_entity(alerts)

            subprocess_logger.add_metadata(explode_alerts=len(alerts))

            # add the new alerts to the local temp file that will be published
            parquet_handler.append_new_records(alerts)

            # add new id timestamp pairs to existing for next pass
            new_id_timestamp_pairs = alerts[["id", "last_modified_timestamp"]]
            existing_id_timestamp_pairs = pandas.concat(
                [existing_id_timestamp_pairs, new_id_timestamp_pairs]
            )

            # update metadata for the files that were processed
            md_db_manager.execute(
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(pk_ids))
                .values(rail_pm_processed=True)
            )

            subprocess_logger.log_complete()

        except Exception as error:
            # on errors, update metadata for files that failed
            md_db_manager.execute(
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(pk_ids))
                .values(rail_pm_processed=True, rail_pm_process_fail=True)
            )

            subprocess_logger.log_failure(error)

    # upload the file with new data and exit
    parquet_handler.upload_data()
    process_logger.log_complete()
