import pyarrow

from .config_base import ConfigDetail
from .config_base import ConfigType

class RtVehicleDetail(ConfigDetail):
    @property
    def config_type(self) -> ConfigType:
        return ConfigType.RT_VEHICLE_POSITIONS

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema([
                ('year', pyarrow.int16()),
                ('month', pyarrow.int8()),
                ('day', pyarrow.int8()),
                ('hour', pyarrow.int8()),
                ('feed_timestamp', pyarrow.int64()),
                ('vehicle_timestamp', pyarrow.int64()),
                ('vehicle_id', pyarrow.string()),
                ('vehicle_label', pyarrow.string()),
                ('current_status', pyarrow.string()),
                ('current_stop_sequence', pyarrow.int16()),
                ('stop_id', pyarrow.string()),
                ('position', pyarrow.struct([
                    ('latitude', pyarrow.float64()),
                    ('longitude', pyarrow.float64()),
                    ('bearing', pyarrow.float32()),])),
                ('trip', pyarrow.struct([
                    ('trip_id', pyarrow.string()),
                    ('route_id', pyarrow.string()),
                    ('direction_id', pyarrow.int8()),
                    ('schedule_relationship', pyarrow.string()),
                    ('start_date', pyarrow.string()),
                    ('start_time', pyarrow.string())])),
                ('consist_labels', pyarrow.list_(pyarrow.string()))
            ])
    
    def record_from_entity(self, entity: dict) -> dict:
        vehicle = entity['vehicle']
        record = {
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

        return record
