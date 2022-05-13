from datetime import datetime
import pyarrow as pa
from typing import Dict

def get_datetime_from_header(d:Dict) -> datetime:
    """
    Provide GTFS-FT dictionary object, from JSON loads
    
    Returns UTC datetime from 'timestamp' in 'header'
    """
    if 'timestamp' not in d['header']:
        raise KeyError("Missing 'timestamp' key in 'header'.")

    return datetime.utcfromtimestamp(d['header']['timestamp'])


def get_vehicle_schema() -> pa.schema:
    return pa.schema([
        ('year', pa.int16()),
        ('month', pa.int8()),
        ('day', pa.int8()),
        ('hour', pa.int8()),
        ('feed_timestamp', pa.int64()),
        ('vehicle_timestamp', pa.int64()),
        ('vehicle_id', pa.string()),
        ('vehicle_label', pa.string()),
        ('current_status', pa.string()),
        ('current_stop_sequence', pa.int16()),
        ('stop_id', pa.string()),
        ('position', pa.struct([
            ('latitude', pa.float64()),
            ('longitude', pa.float64()),
            ('bearing', pa.float32()),])),
        ('trip', pa.struct([
            ('trip_id', pa.string()),
            ('route_id', pa.string()),
            ('direction_id', pa.int8()),
            ('schedule_relationship', pa.string()),
            ('start_date', pa.string()),
            ('start_time', pa.string())])),
        ('consist_labels', pa.list_(pa.string()))
    ])