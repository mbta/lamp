#!/usr/bin/env python

import argparse
import gzip
import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from datetime import datetime
from multiprocessing import Pool
from multiprocessing import Queue
from pathlib import Path

from py_gtfs_rt_ingestion import Configuration


DESCRIPTION = "Convert a json file into a parquet file. Used for testing."
MULTIPROCESSING_POOL_SIZE = 4

def gz_to_pyarrow(filename: str, config: Configuration, return_queue: Queue) -> None:
    """
    Accepts filename as string. Converts gzipped json -> pyarrow table. 

    parrow table sent back to main process using return_queue.
    If conversion fails, returns filename.
    """
    # Enclose entire function in try block, Process can't fail silently.
    # Must return either pyarrow table or filename
    try:
        with gzip.open(filename, 'rb') as f:
            json_data = json.loads(f.read())

        # parse timestamp info out of the header
        header = json_data['header']
        feed_timestamp = header['timestamp']
        timestamp = datetime.utcfromtimestamp(feed_timestamp)

        # create dict of lists 
        ret_obj = {key.name:[] for key in config.export_schema}

        # for each entity in the list, create a record and add it to the table
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
                ret_obj[key].append(record[key])

        ret_obj = pa.table(ret_obj, schema=config.export_schema)

    # Catch failure, send back filename for handling
    except Exception as e:
        print(e, flush=True)
        ret_obj = filename

    # Send pyarrow table or filename back to main Process for concatenation
    return_queue.put(ret_obj)
    return_queue.close()

def lambda_handler(event: dict, context) -> None:
    """
    AWS Lambda Python handled function as described in AWS Developer Guide:
    https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html
    :param event: The event dict sent by Amazon API Gateway that contains all of the
                  request data.
    :param context: The context in which the function is called.
    :return: A response that is sent to Amazon API Gateway, to be wrapped into
             an HTTP response. The 'statusCode' field is the HTTP status code
             and the 'body' field is the body of the response.

    expected event structure:
    {
        file_batch: [file_name_1, file_name_2, ...],
    }
    batch files should all be of same ConfigType
    """
    if 'file_batch' not in event:
        raise Exception("Lambda event missing 'file_batch' key.")

    file_batch = event['file_batch']

    config = Configuration(file_batch[0])

    return_queue = Queue()

    pool = Pool(MULTIPROCESSING_POOL_SIZE)
    pool.starmap_async(gz_to_pyarrow, [(f, config, return_queue) for f in file_batch])

    # Collect pyarrow tables from Processes and concat
    pa_table = None
    failed_ingestion = []
    for _ in range(len(file_batch)):
        ret_obj = return_queue.get()
        if pa_table is None and not isinstance(ret_obj, str):
            pa_table = ret_obj
        elif not isinstance(ret_obj, str):
            pa.concat_tables([pa_table,ret_obj])
        else:
            failed_ingestion.append(ret_obj)

    # Clean up return_queue and pool
    return_queue.close()
    return_queue.join_thread()
    pool.close()
    pool.join()

    # TODO:
    # 1. Implement output directory as env variable for lamda function.
    # 2. Do something with failed conversion filenames.
    OUTPUT_DIR = ''

    pq.write_to_dataset(
        pa_table,
        root_path=OUTPUT_DIR,
        partition_cols=['year','month','day','hour']
    )

    return None

def parseArgs(args) -> dict:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '--input',
        dest='input_file',
        type=str,
        required=True,
        help='provide filename to ingest')

    parser.add_argument(
        '--output',
        dest='output_dir',
        type=str,
        required=True,
        help='provide a directory to output')

    parsed_args = parser.parse_args(args)

    return parsed_args


def convert_json_to_parquet(input_filename: Path, output_dir: Path) -> None:
    """
    convert in input *.json.gz file into a parquet file

    * get the configuration information to transform
    * create a table object
    * parse datetime from the header of the json file
    * append to the table for each element the entities list in the json file
    * write the table
    """
    config = Configuration(filename=input_filename.name)

    table = {key.name:[] for key in config.export_schema}

    with gzip.open(input_filename, 'rb') as f:
        json_data = json.loads(f.read())

        # parse timestamp info out of the header
        header = json_data['header']
        feed_timestamp = header['timestamp']
        timestamp = datetime.utcfromtimestamp(feed_timestamp)

        # for each entity in the list, create a record and add it to the table
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

    pq.write_to_dataset(
        pa.table(table, schema=config.export_schema),
        root_path=output_dir,
        partition_cols=['year','month','day','hour']
    )


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    convert_json_to_parquet(input_filename=Path(args.input_file),
                            output_dir=Path(args.output_dir))
