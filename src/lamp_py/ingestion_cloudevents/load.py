import dataframely as dy
import polars as pl
from deltalake import DeltaTable

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import S3DeltaTable


class CloudEvent(dy.Schema):
    """Minimum columns required by MBTA CloudEvent."""

    id = dy.String(primary_key=True)
    source = dy.String(primary_key=True)
    specversion = dy.String(regex="1.0")
    type = dy.String()
    time = dy.Datetime(time_zone="UTC")
    data = dy.String()


def read(files: list[str]) -> pl.LazyFrame:
    """Read a CloudEvent, coerces columns, and adds metadata."""
    lf = pl.scan_ndjson(
        files,
        include_file_paths="_object_key",
        schema=CloudEvent.to_polars_schema(),
    )
    return lf


def normalize(lf: pl.LazyFrame, explode_lists: bool = False, unnest_structs: bool = True) -> pl.LazyFrame:
    """Recursively normalize a LazyFrame to yield a new LazyFrame."""
    dtypes = lf.collect_schema().dtypes()
    if explode_lists and pl.List in dtypes:
        lf = normalize(
            lf.explode(pl.selectors.list()),
            explode_lists=explode_lists,
            unnest_structs=unnest_structs,
        )
    elif unnest_structs and pl.Struct in dtypes:
        lf = normalize(
            lf.unnest(pl.selectors.struct(), separator="."),
            explode_lists=explode_lists,
            unnest_structs=unnest_structs,
        )
    return lf


def delta_append(destination: S3DeltaTable, data: pl.LazyFrame) -> None:
    """
    Stream a LazyFrame to a partitioned delta table.

    If the table does not exist, it will be created.
    """
    lf = data.with_columns(**destination.partitions)

    if not DeltaTable.is_deltatable(destination.s3_uri):
        DeltaTable.create(
            destination.s3_uri, CloudEvent.to_pyarrow_schema(), mode="append", partition_by=list(destination.partitions.keys())
        )

    lf.sink_delta(
        target=destination.s3_uri,
        mode="append",
        engine="streaming",
        delta_write_options={
            "partition_by": destination.partitions.keys(),
            "schema_mode": "merge",
        },
    )


def ingest(file_list: list[str], destination: S3DeltaTable) -> None:
    """Stream a list of files into a delta table."""
    process_logger = ProcessLogger("ingest", destination=destination.prefix, file_count=file_list)
    process_logger.log_start()
    delta_append(
        destination,
        read(file_list),
    )
    process_logger.log_complete()
