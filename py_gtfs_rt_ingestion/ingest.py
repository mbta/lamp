import argparse
import gzip
import json
import sys

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from py_gtfs_rt_ingestion.helpers import get_datetime_from_header
from py_gtfs_rt_ingestion.helpers import get_vehicle_schema

DESCRIPTION = ""


def parseArgs(args) -> dict:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '--fname',
        dest='fname',
        type=str,
        required=True,
        help='provide filename to ingest')

    parsed_args = parser.parse_args(args)

    return parsed_args


def run(filename) -> None:
    if 'https_cdn.mbta.com_realtime_VehiclePositions_enhanced' not in filename:
        raise Exception('Requires VehiclePostitions file.')

    fname = Path(filename)
    schema = get_vehicle_schema()

    pa_table_dict = {k.name:[] for k in schema}

    with gzip.open(fname, 'rb') as f:
        json_data = json.loads(f.read())
        feed_timestamp = json_data['header']['timestamp']
        feed_dt = get_datetime_from_header(json_data)

        for ent in json_data['entity']:
            v = ent['vehicle']
            r = {
                'year': feed_dt.year,
                'month': feed_dt.month,
                'day': feed_dt.day,
                'hour': feed_dt.hour,
                'feed_timestamp': feed_timestamp,
                'vehicle_timestamp': v.get('timestamp'),
                'vehicle_id': ent.get('id'),
                'vehicle_label': v["vehicle"].get('label'),
                'current_status': v.get('current_status'),
                'current_stop_sequence': v.get("current_stop_sequence"),
                'stop_id': v.get("stop_id"),
                'position': v.get('position'),
                'trip': v.get("trip"),
                'consist_labels': [],
            }
            if v['vehicle'].get('consist') is not None:
                r['consist_labels'] = [c["label"] for c in v["vehicle"].get("consist")]

            for k in r:
                pa_table_dict[k].append(r[k])

    pa_table = pa.table(pa_table_dict, schema=schema)
    pq.write_to_dataset(
        pa_table,
        root_path='./sample_data',
        partition_cols=['year','month','day','hour']
    )


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    run(args)
