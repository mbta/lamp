import zipfile

from typing import IO, List, Tuple, Union

import pyarrow
from pyarrow import csv

from .s3_utils import get_zip_buffer
from .converter import Converter, ConfigType
from .logging_utils import ProcessLogger


def zip_to_pyarrow(filename: str) -> List[Tuple[str, pyarrow.Table]]:
    """
    convert a schedule gtfs zip file into tables. the zip file is essentially a
    small database with each contained file (outside of feed info) acting as its
    own table. info on the gtfs scheduling standard can be found at
    http://gtfs.org/schedule/
    """
    process_logger = ProcessLogger("convert_single_gtfs", filename=filename)
    process_logger.log_start()

    try:
        tables = []
        filelike_input: Union[str, IO[bytes]] = filename
        last_modified = 0
        if filename.startswith("s3://"):
            file_to_load = str(filename).replace("s3://", "")
            filelike_input, last_modified = get_zip_buffer(file_to_load)

        with zipfile.ZipFile(filelike_input) as gtfs_zip:
            # iterate over each "table"
            for gtfs_filename in gtfs_zip.namelist():
                with gtfs_zip.open(gtfs_filename) as file:
                    table = csv.read_csv(file)

                    # add the last modified timestamp
                    timestamp = [last_modified] * table.num_rows
                    table = table.append_column("timestamp", [timestamp])

                filename_prefix = gtfs_filename.replace(".txt", "").upper()
                tables.append((filename_prefix, table))

        process_logger.log_complete()
        return tables
    except Exception as exception:
        process_logger.log_failure(exception)
        return []


class GtfsConverter(Converter):
    """
    Converter for GTFS Schedule Data
    """

    def __init__(self, config_type: ConfigType) -> None:
        Converter.__init__(self, config_type)

        self.tables: List[Tuple[str, pyarrow.Table]] = []

    def add_file(self, file: str) -> bool:
        self.tables = zip_to_pyarrow(file)
        return True

    def reset(self) -> None:
        self.tables = []
        self.archive_files = []
        self.error_files = []

    def get_tables(self) -> List[Tuple[str, pyarrow.Table]]:
        return self.tables

    @property
    def partition_cols(self) -> list[str]:
        return ["timestamp"]
