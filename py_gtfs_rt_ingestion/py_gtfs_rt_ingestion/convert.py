import gzip
import json
import logging
import pyarrow as pa

from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

from .configuration import Configuration

# TODO this is fine for now, but maybe an environ variable?
MULTIPROCESSING_POOL_SIZE = 4

def gz_to_pyarrow(filename: Path, config: Configuration):
    """
    Accepts filename as string. Converts gzipped json -> pyarrow table.
    """
    logging.info("Converting %s to Parquet Table" % filename.name)

    # Enclose entire function in try block, Process can't fail silently.
    # Must return either pyarrow table or filename
    try:
        table = {key.name:[] for key in config.export_schema}

        with gzip.open(filename, 'rb') as f:
            json_data = json.loads(f.read())

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

        ret_obj = pa.table(table, schema=config.export_schema)

    except Exception as e:
        logging.exception(
            "Error occured converting %s to Parquet File." % filename.name)
        ret_obj = filename

    # Send pyarrow table or filename back to main Process for concatenation
    return ret_obj

def convert_files(filepaths: list[Path], config: Configuration) -> pa.Table:
    logging.info("Creating pool with %d threads" % MULTIPROCESSING_POOL_SIZE)
    pool = Pool(MULTIPROCESSING_POOL_SIZE)
    workers = pool.starmap_async(gz_to_pyarrow,
                                 [(f, config) for f in filepaths])

    # Collect pyarrow tables from Processes and concat
    pa_table = None
    failed_ingestion = []

    for ret_obj in workers.get():
        if isinstance(ret_obj, pa.Table):
            if pa_table is None:
                pa_table = ret_obj
            else:
                pa_table = pa.concat_tables([pa_table,ret_obj])
        else:
            failed_ingestion.append(ret_obj)

    logging.info(
        "Completed converting %d files with config %s" % (len(filepaths),
                                                          config.config_type))

    if len(failed_ingestion) > 0:
        logging.warning("Unable to process %d files" % len(failed_ingestion))

    return pa_table
