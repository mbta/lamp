from typing import Dict, List, Optional, Type
import os
from datetime import datetime
import tempfile
from queue import Queue

from abc import ABC, abstractmethod
import dataframely as dy
import polars as pl
import pyarrow
import pyarrow.dataset as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc
from dateutil.relativedelta import relativedelta

from lamp_py.aws.s3 import download_file, upload_file
from lamp_py.aws.kinesis import KinesisReader
from lamp_py.ingestion.utils import explode_table_column, flatten_table_schema
from lamp_py.utils.dataframely import unnest_columns
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import (
    LAMP,
    S3_SPRINGBOARD,
)

RFC3339_TIME_REGEX = r"[0-9]T[012][0-9]:[0-5][0-9]:[0-6][0-9](.[0-9]*)?(Z|[+-][012][0-9]:[0-5][0-9])$"
GTFS_TIME_REGEX = r"^([0-9]{2}):([0-5][0-9]):([0-5][0-9])$"  # clock can be greater than 24 hours
RFC3339_DATE_REGEX = r"^[0-9]{4}-[01][0-9]-[0-3]"


class GlidesConverter(ABC):
    """
    Abstract Base Class for Archiving Glides Events
    """

    user = dy.Struct(
        {
            "emailAddress": dy.String(metadata={"reader_roles": ["GlidesUserEmail"]}),
            "badgeNumber": dy.String(),
        }
    )

    location = dy.Struct({"gtfsId": dy.String(nullable=True), "todsId": dy.String(nullable=True)})

    metadata = dy.Struct(
        {
            "location": location,
            "author": user,
            "inputType": dy.String(),
            "inputTimestamp": dy.String(regex=RFC3339_DATE_REGEX + RFC3339_TIME_REGEX),
        }
    )

    trip_key = dy.Struct(
        {
            "serviceDate": dy.String(nullable=True),
            "tripId": dy.String(nullable=True),
            "startLocation": location,
            "endLocation": location,
            "startTime": dy.String(nullable=True, regex=GTFS_TIME_REGEX),
            "endTime": dy.String(nullable=True, regex=GTFS_TIME_REGEX),
            "revenue": dy.String(nullable=True, regex=r"(non)?revenue"),
            "glidesId": dy.String(),
        }
    )

    class Record(dy.Schema):
        """Base schema for all Glides records."""

        id = dy.String()
        type = dy.String()
        time = dy.Datetime(time_unit="ms")  # in %Y-%m-%dT%H:%M:%S%:z format before serialization
        source = dy.String()
        specversion = dy.String()
        dataschema = dy.String()

    class Table(Record):
        """Base schema for all Glides tables."""

    def __init__(self, base_filename: str) -> None:
        self.tmp_dir = "/tmp"
        self.base_filename = base_filename
        self.type = self.base_filename.replace(".parquet", "")
        self.local_path = os.path.join(self.tmp_dir, self.base_filename)
        self.remote_path = f"s3://{S3_SPRINGBOARD}/{LAMP}/GLIDES/{base_filename}"

        self.records: List[Dict] = []

        self.download_remote()

    @property
    @abstractmethod
    def event_schema(self) -> pyarrow.schema:
        """Schema for incoming events before flattening"""

    @property
    @abstractmethod
    def table_schema(self) -> pyarrow.schema:
        """Schema for springboard-ready datasets after flattening."""

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
        process_logger = ProcessLogger(process_name="append_glides_records", type=self.type)
        process_logger.log_start()

        new_dataset = self.convert_records()

        joined_ds = new_dataset
        if os.path.exists(self.local_path):
            joined_ds = pd.dataset([new_dataset, pd.dataset(self.local_path)], schema=self.table_schema)

        process_logger.add_metadata(
            new_records=new_dataset.count_rows(),
            total_records=joined_ds.count_rows(),
        )

        now = datetime.now()
        start = datetime(2024, 1, 1)

        with tempfile.TemporaryDirectory() as tmp_dir:

            new_path = os.path.join(tmp_dir, self.base_filename)
            row_group_count = 0
            with pq.ParquetWriter(new_path, schema=self.table_schema) as writer:
                while start < now:
                    end = start + relativedelta(months=1)
                    if end < now:
                        row_group = pl.DataFrame(
                            joined_ds.filter((pc.field("time") >= start) & (pc.field("time") < end)).to_table()
                        )

                    else:
                        row_group = pl.DataFrame(joined_ds.filter((pc.field("time") >= start)).to_table())

                    if not row_group.is_empty():
                        unique_table = (
                            row_group.unique(keep="first").sort(by=["time"]).to_arrow().cast(self.table_schema)
                        )

                        row_group_count += 1
                        writer.write_table(unique_table)

                    start = end

            os.replace(new_path, self.local_path)
            process_logger.add_metadata(row_group_count=row_group_count)

        process_logger.log_complete()

    def upload_records(self) -> None:
        """Upload local parquet file to remote storage."""
        upload_file(file_name=self.local_path, object_path=self.remote_path)


class EditorChanges(GlidesConverter):
    """
    Converter for Editor Change Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.editors_changed.v1
    """

    class Record(GlidesConverter.Record):
        """Edits made by one inspector session."""

        data = dy.Struct(
            {
                "metadata": GlidesConverter.metadata,
                "changes": dy.List(
                    dy.Struct(
                        {
                            "type": dy.String(regex=r"start|stop"),
                            "location": GlidesConverter.location,
                            "editor": GlidesConverter.user,
                        }
                    )
                ),
            }
        )

    Table: Type[GlidesConverter.Table] = type("Table", (GlidesConverter.Table,), unnest_columns({"data": Record.data}))

    def __init__(self) -> None:
        GlidesConverter.__init__(self, base_filename="editor_changes.parquet")

    @property
    def event_schema(self) -> pyarrow.schema:
        return self.Record.to_pyarrow_schema()

    @property
    def table_schema(self) -> pyarrow.schema:
        return self.Table.to_pyarrow_schema()

    @property
    def unique_key(self) -> str:
        return "changes"

    def convert_records(self) -> pd.Dataset:
        process_logger = ProcessLogger(process_name="convert_records", type=self.type)
        process_logger.log_start()

        editors_table = pyarrow.Table.from_pylist(self.records, schema=self.event_schema)
        editors_table = flatten_table_schema(editors_table)
        editors_table = explode_table_column(editors_table, "data.changes")
        editors_table = flatten_table_schema(editors_table)
        editors_dataset = pd.dataset(editors_table)

        process_logger.log_complete()
        return editors_dataset


class OperatorSignIns(GlidesConverter):
    """
    Converter for Operator Sign In Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.operator_signed_in.v1
    """

    class Record(GlidesConverter.Record):
        """Operator confirmations of fitness for duty."""

        data = dy.Struct(
            {
                "metadata": GlidesConverter.metadata,
                "operator": dy.Struct({"badgeNumber": dy.String()}),
                "signedInAt": dy.String(regex=RFC3339_DATE_REGEX + RFC3339_TIME_REGEX),
                "signature": dy.Struct({"type": dy.String(), "version": dy.Int16()}),
            }
        )

    Table: Type[GlidesConverter.Table] = type("Table", (GlidesConverter.Table,), unnest_columns({"data": Record.data}))

    def __init__(self) -> None:
        GlidesConverter.__init__(self, base_filename="operator_sign_ins.parquet")

    @property
    def event_schema(self) -> pyarrow.schema:
        return self.Record.to_pyarrow_schema()

    @property
    def table_schema(self) -> pyarrow.schema:
        return self.Table.to_pyarrow_schema()

    @property
    def unique_key(self) -> str:
        return "operator"

    def convert_records(self) -> pd.Dataset:
        process_logger = ProcessLogger(process_name="convert_records", type=self.type)
        process_logger.log_start()
        osi_table = pyarrow.Table.from_pylist(self.records, schema=self.event_schema)
        osi_table = flatten_table_schema(osi_table)
        osi_dataset = pd.dataset(osi_table)

        process_logger.log_complete()
        return osi_dataset


class TripUpdates(GlidesConverter):
    """
    Converter for Trip Update Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.trips_updated.v1
    """

    class Record(GlidesConverter.Record):
        """New information about trips, such as operator assignments or dropped trips."""

        data = dy.Struct(
            {
                "metadata": GlidesConverter.metadata,
                "tripUpdates": dy.List(
                    dy.Struct(
                        {
                            "previousTripKey": GlidesConverter.trip_key,
                            "type": dy.String(),
                            "tripKey": GlidesConverter.trip_key,
                            "comment": dy.String(),
                            "startLocation": GlidesConverter.location,
                            "endLocation": GlidesConverter.location,
                            "startTime": dy.String(),  # can be "unset" string :(
                            "endTime": dy.String(),  # can be "unset" string :(
                            "cars": dy.String(),  # an array of objects
                            "revenue": dy.String(),
                            "dropped": dy.String(),
                            "scheduled": dy.String(),  # an object with an array of objects
                        }
                    )
                ),
            }
        )

    Table: Type[GlidesConverter.Table] = type("Table", (GlidesConverter.Table,), unnest_columns({"data": Record.data}))

    def __init__(self) -> None:
        GlidesConverter.__init__(self, base_filename="trip_updates.parquet")

    @property
    def event_schema(self) -> pyarrow.schema:
        return self.Record.to_pyarrow_schema()

    @property
    def table_schema(self) -> pyarrow.schema:
        return self.Table.to_pyarrow_schema()

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

        process_logger = ProcessLogger(process_name="convert_records", type=self.type)
        process_logger.log_start()

        modified_records = [flatten_multitypes(r) for r in self.records]
        tu_table = pyarrow.Table.from_pylist(modified_records, schema=self.event_schema)
        tu_table = flatten_table_schema(tu_table)
        tu_table = explode_table_column(tu_table, "data.tripUpdates")
        tu_table = flatten_table_schema(tu_table)
        tu_dataset = pd.dataset(tu_table)

        process_logger.log_complete()
        return tu_dataset


class VehicleTripAssignment(GlidesConverter):
    """
    Converter for Vehcile Trip Assignment Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.vehicle_trip_assignment.v1
    """

    class Record(GlidesConverter.Record):
        """Which vehicle is operating each trip at the current moment."""

        data = dy.Struct(
            {
                "vehicleId": dy.String(),
                "tripKey": dy.Struct(
                    {
                        "serviceDate": dy.String(),
                        "tripId": dy.String(),
                        "scheduled": dy.String(),
                    }
                ),
            }
        )

    Table: Type[GlidesConverter.Table] = type("Table", (GlidesConverter.Table,), unnest_columns({"data": Record.data}))

    def __init__(self) -> None:
        GlidesConverter.__init__(self, base_filename="vehicle_trip_assignment.parquet")

    @property
    def event_schema(self) -> pyarrow.schema:
        return self.Record.to_pyarrow_schema()

    @property
    def table_schema(self) -> pyarrow.schema:
        return self.Table.to_pyarrow_schema()

    @property
    def unique_key(self) -> str:
        return "tripKey"

    def convert_records(self) -> pd.Dataset:
        process_logger = ProcessLogger(process_name="convert_records", type=self.type)
        process_logger.log_start()

        tu_table = pyarrow.Table.from_pylist(self.records, schema=self.event_schema)
        tu_table = flatten_table_schema(tu_table)
        tu_dataset = pd.dataset(tu_table)

        process_logger.log_complete()
        return tu_dataset


def ingest_glides_events(kinesis_reader: KinesisReader, metadata_queue: Queue[Optional[str]]) -> None:
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
            VehicleTripAssignment(),
        ]

        for record in kinesis_reader.get_records():
            try:
                # format this so it can be used to partition parquet files
                record["time"] = datetime.fromisoformat(record["time"].replace("Z", "+00:00"))

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
            converter.upload_records()
            metadata_queue.put(converter.remote_path)

    except Exception as e:
        process_logger.log_failure(e)

    process_logger.log_complete()
