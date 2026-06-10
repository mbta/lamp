# pylint: disable=R0914
# pylint too many local variables (more than 15)
from importlib.resources import files

import os
import tempfile
from typing import (
    List,
    Tuple,
)

from pyarrow import fs

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as pd

from lamp_py.aws.s3 import (
    upload_file,
)

from lamp_py.runtime_utils.remote_files import (
    S3_SPRINGBOARD,
)
import time


# with pq.ParquetWriter(self.local_parquet_path, schema=self.output_processed_schema) as writer:
#     for batch in ds.to_batches(
#         batch_size=500_000,
#         columns=[col for col in ds.schema.names if col != "lamp_record_hash"],
#         filter=self.parquet_filter,
#         batch_readahead=1,
#         fragment_readahead=0,
#     ):
#         # don't check empty batch if no rows
#         if batch.num_rows == 0:
#             continue


def partition_column() -> str:
    return "trip_update.trip.route_id"


def table_sort_order() -> List[Tuple[str, str]]:
    return [
        ("feed_timestamp", "ascending"),
        ("trip_update.trip.route_pattern_id", "ascending"),
        ("trip_update.trip.direction_id", "ascending"),
        ("trip_update.vehicle.id", "ascending"),
    ]


def table_sort_order_pl() -> Tuple[List[str], List[bool]]:
    sort_order = table_sort_order()
    return (
        [item[0] for item in sort_order],
        [item[1] == "ascending" for item in sort_order],
    )


def write_local_pq(local_path: str, partition_column: str, in_partition_sort: List[Tuple[str, str]]) -> None:

    ds_paths = [s.replace("s3://", "") for s in files]

    # s3_uris = file_list_from_s3(
    #         bucket_name=self.remote_input_location.bucket,
    #         file_prefix=self.remote_input_location.prefix,
    #     )

    ds_paths = os.listdir(local_path)
    ds_paths = [os.path.join(local_path, s) for s in ds_paths]

    ds = pd.dataset(
        ds_paths,
        format="parquet",
        filesystem=fs.LocalFileSystem(),
    )

    with tempfile.TemporaryDirectory(delete=False) as temp_dir:
        rail_full_set_path = os.path.join(temp_dir, "rail_full_set.parquet")

        print(rail_full_set_path)
        # include the hash column for debug
        rail_full_set_writer = pq.ParquetWriter(
            rail_full_set_path, schema=ds.schema, compression="zstd", compression_level=3
        )

        partitions = pc.unique(ds.to_table(columns=[partition_column]).column(partition_column))

        partitions = sorted(partitions.to_pylist())

        print(f"Found {len(partitions)} unique partitions based on column {partition_column}")

        debug = True
        if debug:
            start_time = time.time()

        # col, order = table_sort_order_pl()
        for part in partitions:
            write_table = ds.to_table(filter=((pc.field(partition_column) == part))).sort_by(in_partition_sort)

            # write_table = pl.from_arrow(ds.to_table(
            #     filter=(
            #         (pc.field(partition_column) == part)
            #     )
            # )).sort(col, descending=order).to_arrow().cast(ds.schema)

            rail_full_set_writer.write_table(write_table)

            if debug:
                elapsed = time.time() - start_time
                print(f"Processed partition {part}: {elapsed:.2f}s elapsed, total rows: {write_table.num_rows}")

        rail_full_set_writer.close()

        if False:
            # upload the upload_path file (without hash) to s3
            # replace the first part of the path with the s3 path
            upload_file(
                upload_path,
                local_path.replace(self.tmp_folder, S3_SPRINGBOARD),
            )


if __name__ == "__main__":
    write_local_pq(
        "/Users/hhuang/ingestion/lamp/RT_TRIP_UPDATES/year=2026/month=2/day=1/", partition_column(), table_sort_order()
    )
    write_local_pq(
        "s3://mbta-ctd-dataplatform-dev-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=4/day=1/2026-04-01T00:00:00.parquet",
        partition_column(),
        table_sort_order(),
    )
