from typing import Dict, List, Optional
import os
from datetime import datetime
import tempfile
from queue import Queue

from abc import ABC, abstractmethod
import polars as pl
import pyarrow
import pyarrow.dataset as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc
from dateutil.relativedelta import relativedelta

from lamp_py.aws.s3 import download_file, upload_file
from lamp_py.aws.kinesis import KinesisReader
from lamp_py.ingestion.utils import explode_table_column, flatten_schema
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import (
    LAMP,
    S3_SPRINGBOARD,
)


class GlidesConverter(ABC):
    """
    Abstract Base Class for Archiving Glides Events
    """

    glides_user = pyarrow.struct(
        [
            ("emailAddress", pyarrow.string()),
            ("badgeNumber", pyarrow.string()),
        ]
    )

    glides_location = pyarrow.struct(
        [
            ("gtfsId", pyarrow.string()),
            ("todsId", pyarrow.string()),
        ]
    )

    glides_metadata = pyarrow.struct(
        [
            ("location", glides_location),
            ("author", glides_user),
            ("inputType", pyarrow.string()),
            ("inputTimestamp", pyarrow.string()),
        ]
    )

    def __init__(self, base_filename: str) -> None:
        self.tmp_dir = "/tmp"
        self.base_filename = base_filename
        self.type = self.base_filename.replace(".parquet", "")
        self.local_path = os.path.join(self.tmp_dir, self.base_filename)
        self.remote_path = (
            f"s3://{S3_SPRINGBOARD}/{LAMP}/GLIDES/{base_filename}"
        )

        self.records: List[Dict] = []

        self.download_remote()

    @property
    @abstractmethod
    def event_schema(self) -> pyarrow.schema:
        """Schema for incoming events before flattening"""

    @property
    @abstractmethod
    def unique_key(self) -> str:
        """Key in record['data'] that is unique to this event type"""

    def download_remote(self) -> None:
        """download the remote parquet path for appending"""
        if os.path.exists(self.local_path):
            os.remove(self.local_path)

        download_file(object_path=self.remote_path, file_name=self.local_path)

    @abstractmethod
    def convert_records(self) -> pd.Dataset:
        """Convert incoming records into a flattened table of records"""

    def append_records(self) -> None:
        """Add incoming records to a local parquet file"""
        process_logger = ProcessLogger(
            process_name="append_glides_records", type=self.type
        )
        process_logger.log_start()

        new_dataset = self.convert_records()

        joined_ds = new_dataset
        if os.path.exists(self.local_path):
            joined_ds = pd.dataset([new_dataset, pd.dataset(self.local_path)])

        process_logger.add_metadata(
            new_records=new_dataset.count_rows(),
            total_records=joined_ds.count_rows(),
        )

        now = datetime.now()
        start = datetime(2024, 1, 1)

        with tempfile.TemporaryDirectory() as tmp_dir:

            new_path = os.path.join(tmp_dir, self.base_filename)
            row_group_count = 0
            with pq.ParquetWriter(new_path, schema=joined_ds.schema) as writer:
                while start < now:
                    end = start + relativedelta(months=1)
                    if end < now:
                        row_group = pl.DataFrame(
                            joined_ds.filter(
                                (pc.field("time") >= start)
                                & (pc.field("time") < end)
                            ).to_table()
                        )

                    else:
                        row_group = pl.DataFrame(
                            joined_ds.filter(
                                (pc.field("time") >= start)
                            ).to_table()
                        )

                    if not row_group.is_empty():
                        unique_table = (
                            row_group.unique(keep="first")
                            .sort(by=["time"])
                            .to_arrow()
                            .cast(new_dataset.schema)
                        )

                        row_group_count += 1
                        writer.write_table(unique_table)

                    start = end

            os.replace(new_path, self.local_path)
            process_logger.add_metadata(row_group_count=row_group_count)

        upload_file(file_name=self.local_path, object_path=self.remote_path)

        process_logger.log_complete()


class EditorChanges(GlidesConverter):
    """
    Converter for Editor Change Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.editors_changed.v1
    """

    def __init__(self) -> None:
        GlidesConverter.__init__(self, base_filename="editor_changes.parquet")

    @property
    def event_schema(self) -> pyarrow.schema:
        """
        Editor Changes Schema is the base Kinesis data plus metadata plus a
        list of changes objects.
        """
        glides_editor_change = pyarrow.struct(
            [
                ("type", pyarrow.string()),
                ("location", self.glides_location),
                ("editor", self.glides_user),
            ]
        )

        # NOTE: These tables will eventually be uniqued via polars, which will
        # not work if any of the types in the schema are objects.
        return pyarrow.schema(
            [
                (
                    "data",
                    pyarrow.struct(
                        [
                            ("metadata", self.glides_metadata),
                            ("changes", pyarrow.list_(glides_editor_change)),
                        ]
                    ),
                ),
                ("id", pyarrow.string()),
                ("type", pyarrow.string()),
                ("time", pyarrow.timestamp("ms")),
                ("source", pyarrow.string()),
                ("specversion", pyarrow.string()),
                ("dataschema", pyarrow.string()),
            ]
        )

    @property
    def unique_key(self) -> str:
        return "changes"

    def convert_records(self) -> pd.Dataset:
        process_logger = ProcessLogger(
            process_name="convert_records", type=self.type
        )
        process_logger.log_start()

        editors_table = pyarrow.Table.from_pylist(
            self.records, schema=self.event_schema
        )
        editors_table = flatten_schema(editors_table)
        editors_table = explode_table_column(editors_table, "data.changes")
        editors_table = flatten_schema(editors_table)
        editors_dataset = pd.dataset(editors_table)

        process_logger.log_complete()
        return editors_dataset


class OperatorSignIns(GlidesConverter):
    """
    Converter for Operator Sign In Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.operator_signed_in.v1
    """

    def __init__(self) -> None:
        GlidesConverter.__init__(
            self, base_filename="operator_sign_ins.parquet"
        )

    @property
    def event_schema(self) -> pyarrow.schema:
        # NOTE: These tables will eventually be uniqued via polars, which will
        # not work if any of the types in the schema are objects.
        return pyarrow.schema(
            [
                (
                    "data",
                    pyarrow.struct(
                        [
                            ("metadata", self.glides_metadata),
                            (
                                "operator",
                                pyarrow.struct(
                                    [("badgeNumber", pyarrow.string())]
                                ),
                            ),
                            # a timestamp but it needs reformatting for pyarrow
                            ("signedInAt", pyarrow.string()),
                            (
                                "signature",
                                pyarrow.struct(
                                    [
                                        ("type", pyarrow.string()),
                                        ("version", pyarrow.int16()),
                                    ]
                                ),
                            ),
                        ]
                    ),
                ),
                ("id", pyarrow.string()),
                ("type", pyarrow.string()),
                ("time", pyarrow.timestamp("ms")),
                ("source", pyarrow.string()),
                ("specversion", pyarrow.string()),
                ("dataschema", pyarrow.string()),
            ]
        )

    @property
    def unique_key(self) -> str:
        return "operator"

    def convert_records(self) -> pd.Dataset:
        process_logger = ProcessLogger(
            process_name="convert_records", type=self.type
        )
        process_logger.log_start()
        osi_table = pyarrow.Table.from_pylist(
            self.records, schema=self.event_schema
        )
        osi_table = flatten_schema(osi_table)
        osi_dataset = pd.dataset(osi_table)

        process_logger.log_complete()
        return osi_dataset


class TripUpdates(GlidesConverter):
    """
    Converter for Trip Update Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.trips_updated.v1
    """

    def __init__(self) -> None:
        GlidesConverter.__init__(self, base_filename="trip_updates.parquet")

    @property
    def event_schema(self) -> pyarrow.schema:
        glides_trip_key = pyarrow.struct(
            [
                ("serviceDate", pyarrow.string()),
                ("startLocation", self.glides_location),
                ("endLocation", self.glides_location),
                ("startTime", pyarrow.string()),
                ("endTime", pyarrow.string()),
                ("revenue", pyarrow.string()),
                ("glidesId", pyarrow.string()),
            ]
        )

        glides_trip_update = pyarrow.struct(
            [
                ("previousTripKey", glides_trip_key),
                ("type", pyarrow.string()),
                ("tripKey", glides_trip_key),
                ("comment", pyarrow.string()),
                ("startLocation", self.glides_location),
                ("endLocation", self.glides_location),
                # can be "unset" string :(
                ("startTime", pyarrow.string()),
                # can be "unset" string :(
                ("endTime", pyarrow.string()),
                # an array of objects
                ("cars", pyarrow.string()),
                ("revenue", pyarrow.string()),
                ("dropped", pyarrow.string()),
                # an object with an array of objects
                ("scheduled", pyarrow.string()),
            ]
        )

        # NOTE: These tables will eventually be uniqued via polars, which will
        # not work if any of the types in the schema are objects.
        return pyarrow.schema(
            [
                (
                    "data",
                    pyarrow.struct(
                        [
                            ("metadata", self.glides_metadata),
                            ("tripUpdates", pyarrow.list_(glides_trip_update)),
                        ]
                    ),
                ),
                ("id", pyarrow.string()),
                ("type", pyarrow.string()),
                ("time", pyarrow.timestamp("ms")),
                ("source", pyarrow.string()),
                ("specversion", pyarrow.string()),
                ("dataschema", pyarrow.string()),
            ]
        )

    @property
    def unique_key(self) -> str:
        return "tripUpdates"

    def convert_records(self) -> pd.Dataset:
        def flatten_multitypes(record: Dict) -> Dict:
            """
            For each update in a record, flatten out the objects in "cars",
            "dropped", and "cars". These objects are poorly structured for our
            needs and can't be strongly typed. Leave them as json strings for
            now and parse them later when analyzing.
            """
            for update in record["data"]["tripUpdates"]:
                # convert these objects into strings if they exist
                for key in ["scheduled", "dropped", "cars"]:
                    try:
                        update[key] = str(update[key])
                    except KeyError:
                        pass

            return record

        process_logger = ProcessLogger(
            process_name="convert_records", type=self.type
        )
        process_logger.log_start()

        modified_records = [flatten_multitypes(r) for r in self.records]
        tu_table = pyarrow.Table.from_pylist(
            modified_records, schema=self.event_schema
        )
        tu_table = flatten_schema(tu_table)
        tu_table = explode_table_column(tu_table, "data.tripUpdates")
        tu_table = flatten_schema(tu_table)
        tu_dataset = pd.dataset(tu_table)

        process_logger.log_complete()
        return tu_dataset


def ingest_glides_events(
    kinesis_reader: KinesisReader, metadata_queue: Queue[Optional[str]]
) -> None:
    """
    ingest glides records from the kinesis stream and add them to parquet files
    """
    process_logger = ProcessLogger(process_name="ingest_glides_events")
    process_logger.log_start()

    try:
        converters = [
            EditorChanges(),
            OperatorSignIns(),
            TripUpdates(),
        ]

        for record in kinesis_reader.get_records():
            try:
                # format this so it can be used to partition parquet files
                record["time"] = datetime.fromisoformat(
                    record["time"].replace("Z", "+00:00")
                )

                data_keys = record["data"].keys()

                for converter in converters:
                    if converter.unique_key in data_keys:
                        converter.records.append(record)
                        break
                else:
                    raise KeyError(f"No distinguishing key in {data_keys}")
            except Exception as e:
                process_logger.log_failure(e)

        for converter in converters:
            converter.append_records()
            metadata_queue.put(converter.remote_path)

    except Exception as e:
        process_logger.log_failure(e)

    process_logger.log_complete()
