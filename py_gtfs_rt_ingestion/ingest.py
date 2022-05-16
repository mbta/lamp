#!/usr/bin/env python

import argparse
import gzip
import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from datetime import datetime
from pathlib import Path

from py_gtfs_rt_ingestion import Configuration

DESCRIPTION = "Convert a json file into a parquet file. Used for testing."

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


def convert_json_to_parquet(input_filename: str, output_dir: str) -> None:
    """
    convert in input *.json.gz file into a parquet file

    * get the configuration information to transform
    * create a table object
    * parse datetime from the header of the json file
    * append to the table for each element the entities list in the json file
    * write the table
    """
    config = Configuration.from_filename(input_filename)

    schema = config.get_schema()
    table = {key.name:[] for key in schema}

    with gzip.open(Path(input_filename), 'rb') as f:
        json_data = json.loads(f.read())

        header = json_data['header']
        feed_timestamp = header['timestamp']
        timestamp = datetime.utcfromtimestamp(feed_timestamp)

        for entity in json_data['entity']:
            vehicle = entity['vehicle']
            record = {
                'year': timestamp.year,
                'month': timestamp.month,
                'day': timestamp.day,
                'hour': timestamp.hour,
                'feed_timestamp': feed_timestamp,
                'vehicle_timestamp': vehicle.get('timestamp'),
                'vehicle_id': entity.get('id'),
                'vehicle_label': vehicle["vehicle"].get('label'),
                'current_status': vehicle.get('current_status'),
                'current_stop_sequence': vehicle.get("current_stop_sequence"),
                'stop_id': vehicle.get("stop_id"),
                'position': vehicle.get('position'),
                'trip': vehicle.get("trip"),
                'consist_labels': [],
            }
            if vehicle['vehicle'].get('consist') is not None:
                record['consist_labels'] = [consist["label"]
                                            for consist
                                            in vehicle["vehicle"].get("consist")]

            for key in record:
                table[key].append(record[key])

    pq.write_to_dataset(
        pa.table(table, schema=schema),
        root_path=output_dir,
        partition_cols=['year','month','day','hour']
    )


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    convert_json_to_parquet(input_filename=args.input_file,
                            output_dir=args.output_dir)
