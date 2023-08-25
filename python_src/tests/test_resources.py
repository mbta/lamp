import os
import pyarrow
from pyarrow import csv, parquet

test_files_dir = os.path.join(os.path.dirname(__file__), "test_files")

incoming_dir = os.path.join(test_files_dir, "INCOMING")
springboard_dir = os.path.join(test_files_dir, "SPRINGBOARD")


def csv_to_vp_parquet(csv_filepath: str, parquet_filepath: str) -> None:
    """
    read vehicle position data in csv format and write it to a parquet file
    """
    vp_csv_options = csv.ConvertOptions(
        column_types={
            "current_status": pyarrow.string(),
            "current_stop_sequence": pyarrow.int64(),
            "stop_id": pyarrow.string(),
            "vehicle_timestamp": pyarrow.int64(),
            "direction_id": pyarrow.int64(),
            "route_id": pyarrow.string(),
            "trip_id": pyarrow.string(),
            "start_date": pyarrow.string(),
            "start_time": pyarrow.string(),
            "vehicle_id": pyarrow.string(),
            "vehicle_consist": pyarrow.string(),
        },
        # in our ingestion, if a key is missing, the value written to the
        # parquet file is null. mimic this behavior by making empty strings
        # null instead of ''.
        strings_can_be_null=True,
    )

    table = csv.read_csv(csv_filepath, convert_options=vp_csv_options)
    parquet.write_table(table, parquet_filepath)
