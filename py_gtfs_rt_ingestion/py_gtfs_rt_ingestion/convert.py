import json
import logging

from datetime import datetime
from typing import Union

import pyarrow as pa
from pyarrow import fs

from .configuration import Configuration


def gz_to_pyarrow(filename: str, config: Configuration) -> Union[str, pa.Table]:
    """
    Accepts filename as string. Converts gzipped json -> pyarrow table.

    Will handle Local or S3 filenames.
    """
    logging.info("Converting %s to Parquet Table", filename)
    try:
        if filename.startswith("s3://"):
            active_fs = fs.S3FileSystem()
            file_to_load = str(filename).replace("s3://", "")
        else:
            active_fs = fs.LocalFileSystem()
            file_to_load = filename

        with active_fs.open_input_stream(file_to_load) as file:
            json_data = json.load(file)

        pa_table = _json_to_pyarrow(json_data=json_data, config=config)

        return pa_table

    except Exception as exception:
        logging.error("Error converting %s", filename)
        logging.exception(exception)
        return filename


def _json_to_pyarrow(json_data: dict, config: Configuration) -> pa.Table:
    # Create empty 'table' as dict of lists for export schema
    table = config.empty_table()

    # parse timestamp info out of the header
    feed_timestamp = json_data["header"]["timestamp"]
    timestamp = datetime.utcfromtimestamp(feed_timestamp)

    # for each entity in the list, create a record, add it to the table
    for entity in json_data["entity"]:
        record = config.record_from_entity(entity=entity)
        record.update(
            {
                "year": timestamp.year,
                "month": timestamp.month,
                "day": timestamp.day,
                "hour": timestamp.hour,
                "feed_timestamp": feed_timestamp,
            }
        )

        for key in record:
            table[key].append(record[key])

    return pa.table(table, schema=config.export_schema)
