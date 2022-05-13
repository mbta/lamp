# from awsglue.utils import getResolvedOptions
import argparse
import gzip
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from py_gtfs_rt_ingestion.helpers import get_datetime_from_header
from py_gtfs_rt_ingestion.helpers import get_vehicle_schema

def run() -> None:
    """
    Accepts VehiclePositions filename to parse 

    Currently utuilizing argparse.ArgumentParser()
    Will require getResolveOptions from asglue.utils for Glue Job parameter parsing
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('fname', type=str, help='provide filename to ingest')
    args = parser.parse_args()

    if 'https_cdn.mbta.com_realtime_VehiclePositions_enhanced' not in args.fname:
        raise Exception('Requires VehiclePostitions file.')

    fname = Path(args.fname)
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
    run()


