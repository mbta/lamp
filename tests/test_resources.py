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
            "vehicle.current_status": pyarrow.string(),
            "vehicle.current_stop_sequence": pyarrow.uint32(),
            "vehicle.stop_id": pyarrow.string(),
            "vehicle.timestamp": pyarrow.uint64(),
            "vehicle.trip.direction_id": pyarrow.uint8(),
            "vehicle.trip.route_id": pyarrow.string(),
            "vehicle.trip.trip_id": pyarrow.string(),
            "vehicle.trip.start_date": pyarrow.string(),
            "vehicle.trip.start_time": pyarrow.string(),
            "vehicle.vehicle.id": pyarrow.string(),
            "vehicle.vehicle.consist": pyarrow.string(),
        },
        # in our ingestion, if a key is missing, the value written to the
        # parquet file is null. mimic this behavior by making empty strings
        # null instead of ''.
        strings_can_be_null=True,
    )

    table = csv.read_csv(csv_filepath, convert_options=vp_csv_options)
    parquet.write_table(table, parquet_filepath)
