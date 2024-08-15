import os

from lamp_py.ingestion.light_rail_gps import raw_gps_schema
from lamp_py.ingestion.light_rail_gps import dataframe_from_gz

from ..test_resources import test_files_dir

mock_file_list = [
    os.path.join(
        test_files_dir,
        "INCOMING/2024-05-01T02:30:11Z_s3_mbta_ctd_trc_data_rtr_prod_LightRailRawGPS.json.gz",
    )
]


def test_light_rail_gps() -> None:
    """
    test gtfs_events_for_date pipeline
    """
    dataframe, archive_files, error_files = dataframe_from_gz(mock_file_list)

    assert len(archive_files) == 1

    assert len(error_files) == 0

    assert dataframe.schema == raw_gps_schema

    assert dataframe.shape[0] == 190
