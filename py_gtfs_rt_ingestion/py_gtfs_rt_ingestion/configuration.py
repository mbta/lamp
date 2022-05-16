from enum import Enum, auto

import pyarrow

class Configuration(Enum):
    """
    Configuration that handles the specifics of each of our JSON record types.

    https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz
    https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz
    https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced.json.gz
    https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz
    https_mbta_integration.mybluemix.net_vehicleCount.gz
    """
    RT_ALERTS = auto()
    RT_TRIP_UPDATES = auto()
    RT_VEHICLE_POSITIONS = auto()

    @classmethod
    def from_filename(cls, filename: str):
        if 'mbta.com_realtime_Alerts_enhanced' in filename:
            return cls.RT_ALERTS
        if 'mbta.com_realtime_TripUpdates_enhanced' in filename:
            return cls.RT_TRIP_UPDATES
        if 'mbta.com_realtime_VehiclePositions_enhanced' in filename:
            return cls.RT_VEHICLE_POSITIONS

        raise Exception("Bad Configuration from filename %s" % filename)

    def get_schema(self) -> pyarrow.schema:
        if self == self.__class__.RT_VEHICLE_POSITIONS:
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

        raise Exception("No Current Schema for %s" % self.name)

    def record_from_entity(self, entity: dict) -> dict:
        if self == self.__class__.RT_VEHICLE_POSITIONS:
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

        raise Exception("No Current Schema for %s" % self.name)

