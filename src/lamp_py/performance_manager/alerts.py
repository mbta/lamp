import os
from typing import List, Dict, Tuple, Optional

import pandas
import pyarrow
import pyarrow.dataset as pd
import pyarrow.parquet as pq
import sqlalchemy as sa

from lamp_py.aws.s3 import (
    download_file,
    read_parquet,
    upload_file,
    object_metadata,
)

from lamp_py.postgres.metadata_schema import MetadataLog
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.process_logger import ProcessLogger


class AlertParquetHandler:
    """
    This class handles all of the interactions with alert data thats stored as a parquet file on s3.
    """

    def __init__(self) -> None:
        bucket_name: str = os.environ.get("PUBLIC_ARCHIVE_BUCKET", "")
        self.s3_path: str = os.path.join(
            bucket_name, "lamp", "tableau", "alerts", "LAMP_RT_ALERTS.parquet"
        )

        self.local_path: str = os.path.join("/tmp", "alerts.parquet")
        self.version_key: str = "version"
        self.file_version: str = "1.0.0"

        self.local_exists: bool = self.download_data()

        self.new_data: bool = False

    @property
    def parquet_schema(self) -> pyarrow.schema:
        """the parquet schema for aggregated alerts files"""
        return pyarrow.schema(
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
                ("header_text", pyarrow.string()),
                ("description_text", pyarrow.string()),
                ("service_effect_text", pyarrow.string()),
                ("timeframe_text", pyarrow.string()),
                ("recurrence_text", pyarrow.string()),
                ("created", pyarrow.timestamp("ms", tz="America/New_York")),
                ("created_timestamp", pyarrow.uint64()),
                (
                    "last_modified",
                    pyarrow.timestamp("ms", tz="America/New_York"),
                ),
                ("last_modified_timestamp", pyarrow.uint64()),
                (
                    "last_push_notification",
                    pyarrow.timestamp("ms", tz="America/New_York"),
                ),
                ("last_push_notification_timestamp", pyarrow.uint64()),
                ("closed", pyarrow.timestamp("ms", tz="America/New_York")),
                ("closed_timestamp", pyarrow.uint64()),
                (
                    "active_period_start",
                    pyarrow.timestamp("ms", tz="America/New_York"),
                ),
                ("active_period_start_timestamp", pyarrow.uint64()),
                (
                    "active_period_end",
                    pyarrow.timestamp("ms", tz="America/New_York"),
                ),
                ("active_period_end_timestamp", pyarrow.uint64()),
                ("informed_entity.route_id", pyarrow.string()),
                ("informed_entity.route_type", pyarrow.int8()),
                ("informed_entity.direction_id", pyarrow.int8()),
                ("informed_entity.stop_id", pyarrow.string()),
                ("informed_entity.facility_id", pyarrow.string()),
                ("informed_entity.activities", pyarrow.string()),
            ]
        )

    def existing_id_timestamp_pairs(self) -> pandas.DataFrame:
        """
        get all unique alert id / last modified timestamp pairs in the local
        parquet file, a key that can be used to identify alerts that have
        already been processed.
        """
        columns = ["id", "last_modified_timestamp"]
        if self.local_exists:
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
        self.new_data = True
        alerts = alerts.reset_index(drop=True)
        alerts_table = pyarrow.Table.from_pandas(
            alerts, schema=self.parquet_schema
        )

        # if there is no local file, write the alerts to the local file and exit
        if not self.local_exists:
            with pq.ParquetWriter(
                self.local_path, schema=self.parquet_schema
            ) as writer:
                writer.write_table(alerts_table)

            self.local_exists = True
            return

        # write new alerts to a new file and then join them with the local file in batches
        new_path = os.path.join("/tmp", "new_alerts.parquet")
        joined_path = os.path.join("/tmp", "joined_alerts.parquet")

        with pq.ParquetWriter(new_path, schema=self.parquet_schema) as writer:
            writer.write_table(alerts_table)

        joined_batches = pd.dataset(
            [
                pd.dataset(self.local_path),
                pd.dataset(new_path),
            ]
        ).to_batches()

        with pq.ParquetWriter(
            joined_path, schema=self.parquet_schema
        ) as writer:
            for batch in joined_batches:
                writer.write_batch(batch)

        # the joined file becomes the local file and we can delete the new and
        # joined files
        os.replace(joined_path, self.local_path)
        os.remove(new_path)

    def download_data(self) -> bool:
        """
        download the s3 parquet file to a temp location if the versions match.
        return True if the file was downloaded successfully, False otherwise.
        """
        try:
            lamp_version = object_metadata(self.s3_path).get(
                self.version_key, ""
            )

            if lamp_version != self.file_version:
                return False

            return download_file(
                object_path=self.s3_path,
                file_name=self.local_path,
            )

        except Exception:
            return False

    def upload_data(self) -> None:
        """
        upload the local parquet file to s3 if new data was added
        """
        if self.new_data:
            upload_file(
                file_name=self.local_path,
                object_path=self.s3_path,
                extra_args={"Metadata": {self.version_key: self.file_version}},
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
        alerts[key] = alerts[translation_key].apply(process_translation)
        drop_columns.append(translation_key)

    alerts = alerts.drop(columns=drop_columns)

    return alerts


def transform_timestamps(alerts: pandas.DataFrame) -> pandas.DataFrame:
    """
    Extract timestamps from the active period columns and transform all timestamps to easter standard time.
    """

    # pull out the active period timestamps from the dict in that column
    def extract_start_end(
        periods: Optional[List[Dict[str, int]]]
    ) -> Tuple[int | None, int | None]:
        """small lambda for extracting start and end timestamps"""
        if periods is None or len(periods) == 0:
            return None, None
        return periods[0].get("start"), periods[0].get("end")

    alerts[
        [
            "active_period_start_timestamp",
            "active_period_end_timestamp",
        ]
    ] = (
        alerts["active_period"].apply(extract_start_end).apply(pandas.Series)
    )

    # convert these timestamps to Int64 to avoid floating point errors
    alerts["active_period_start_timestamp"] = alerts[
        "active_period_start_timestamp"
    ].astype("Int64")
    alerts["active_period_end_timestamp"] = alerts[
        "active_period_end_timestamp"
    ].astype("Int64")

    # drop the active period list column
    alerts = alerts.drop(columns=["active_period"])

    # convert all of the timestamp columns to eastern standard time
    timestamp_columns = [
        "created",
        "last_modified",
        "last_push_notification",
        "closed",
        "active_period_start",
        "active_period_end",
    ]

    for key in timestamp_columns:
        timestamp_key = f"{key}_timestamp"

        alerts[key] = (
            pandas.to_datetime(alerts[timestamp_key], unit="s")
            .dt.tz_localize("UTC", ambiguous="infer")
            .dt.tz_convert("America/New_York")
        )

    return alerts


def explode_alerts(alerts: pandas.DataFrame) -> pandas.DataFrame:
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


def get_unprocessed_alert_files(
    md_db_manager: DatabaseManager,
) -> List[Dict[str, str]]:
    """
    Get unprocessed RT Alert Files from the MetadataLog table.

    @return [
        { 'pk_id': 1, 'path': 'mbta-ctd-dataplatform-springboard/lamp/RT_ALERTS/year=2024/month=1/day=4/hour=0/uuid_1-0.parquet' },
        { 'pk_id': 2, 'path': 'mbta-ctd-dataplatform-springboard/lamp/RT_ALERTS/year=2024/month=1/day=4/hour=1/uuid_2-0.parquet' },
        ...
    ]
    """
    read_md = sa.select(MetadataLog.pk_id, MetadataLog.path).where(
        (MetadataLog.rail_pm_processed == sa.false())
        & (MetadataLog.path.contains("RT_ALERTS"))
    )

    return md_db_manager.select_as_list(read_md)


def process_alerts(md_db_manager: DatabaseManager) -> None:
    """
    Gather unprocessed alerts files from springboard, process them, and add them to the public export
    """
    process_logger = ProcessLogger("process_alerts")
    process_logger.log_start()
    unprocessed_records = get_unprocessed_alert_files(md_db_manager)

    # if there are no new alerts files, exit
    if len(unprocessed_records) == 0:
        process_logger.log_complete()
        return

    # create a handler object that will download, append, and upload data
    parquet_handler = AlertParquetHandler()
    existing_id_timestamp_pairs = parquet_handler.existing_id_timestamp_pairs()

    # process up to 24 hours at a time
    chunk_size = 24
    for i in range(0, len(unprocessed_records), chunk_size):
        # get a months worth of files
        chunk = unprocessed_records[i : i + chunk_size]

        pk_ids = [record["pk_id"] for record in chunk]
        alert_files = [record["path"] for record in chunk]

        subprocess_logger = ProcessLogger(
            "process_alerts_chunk", alert_files=alert_files
        )
        subprocess_logger.log_start()

        try:
            # extract the data and transform it for publication
            alerts = extract_alerts(alert_files, existing_id_timestamp_pairs)

            process_logger.add_metadata(extracted_alerts=len(alerts))

            if alerts.empty:
                subprocess_logger.log_complete()
                continue

            alerts = transform_translations(alerts)
            alerts = transform_timestamps(alerts)
            alerts = explode_alerts(alerts)
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
