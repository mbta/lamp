import gzip
import json
import logging
import pyarrow as pa

from datetime import datetime
from pyarrow import fs

from .configuration import Configuration

def gz_to_pyarrow(filename: str, config: Configuration) -> pa.Table:
    """
    Accepts filename as string. Converts gzipped json -> pyarrow table.
    """
    logging.info("Converting %s to Parquet Table" % filename)
    try:
        with gzip.open(filename, 'rb') as f:
            json_data = json.load(f)
            pa_table = _json_to_pyarrow(json_data=json_data, config=config)

        return pa_table

    except Exception as exception:
        logging.exception("Error converting %s" % filename)
        return filename


def s3_to_pyarrow(filename: str, config: Configuration) -> pa.Table:
    logging.info("Converting %s to Parquet Table" % filename)
    try:
        s3_fs = fs.S3FileSystem()
        with s3_fs.open_input_stream(filename) as f:
            json_data = json.load(f)
            pa_table = _json_to_pyarrow(json_data=json_data, config=config)

        return pa_table

    except Exception as exception:
        logging.exception("Error converting %s" % filename)
        return filename


def _json_to_pyarrow(json_data: dict, config: Configuration) -> pa.Table:
    # table = {key.name:[] for key in config.export_schema}
    table = config.empty_table()

    # parse timestamp info out of the header
    header = json_data['header']
    feed_timestamp = header['timestamp']
    timestamp = datetime.utcfromtimestamp(feed_timestamp)

    # for each entity in the list, create a record, add it to the table
    for entity in json_data['entity']:
        record = config.record_from_entity(entity=entity)
        record.update({
            'year': timestamp.year,
            'month': timestamp.month,
            'day': timestamp.day,
            'hour': timestamp.hour,
            'feed_timestamp': feed_timestamp
        })

        for key in record:
            table[key].append(record[key])

    return pa.table(table, schema=config.export_schema)
