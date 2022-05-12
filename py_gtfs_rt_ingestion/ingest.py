# from awsglue.utils import getResolvedOptions
import argparse
import gzip
from pathlib import Path
import pyarrow as pa
from pyarrow import json
import pyarrow.parquet as pq

from lib.helpers import get_datetime_from_header
from lib.helpers import get_vehicle_schema

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

    with gzip.open(fname, 'r') as f:
        pa_table = json.read_json(f)
        feed_timestamp = pa_table['header'][0]['timestamp']
        feed_dt = get_datetime_from_header(feed_timestamp)

        
        for ent in pa_table['entity'][0]:
            v = ent['vehicle']
            r = {
                'year': feed_dt.year,
                'month': feed_dt.month,
                'day': feed_dt.day,
                'hour': feed_dt.hour,
                'feed_timestamp': feed_timestamp.as_py(),
                'vehicle_timestamp': v.get('timestamp').as_py(),
                'vehicle_id': ent.get('id').as_py(),
                'vehicle_label': v["vehicle"].get('label').as_py(),
                'current_status': v.get('current_status').as_py(),
                'current_stop_sequence': v.get("current_stop_sequence").as_py(),
                'stop_id': v.get("stop_id").as_py(),
                'position': v.get('position').as_py(),
                'trip': v.get("trip").as_py(),
                'consist_labels': [],
            }
            if v['vehicle'].get('consist').as_py() is not None:
                r['consist_labels'] = [c["label"] for c in v["vehicle"].get("consist").as_py()]
                
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


