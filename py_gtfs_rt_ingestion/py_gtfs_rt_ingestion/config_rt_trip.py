import pyarrow

from .config_base import ConfigDetail
from .config_base import ConfigType

class RtTripDetail(ConfigDetail):
    @property
    def config_type(self) -> ConfigType:
        return ConfigType.RT_TRIP_UPDATES

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema([
            # header -> timestamp
            ('year', pyarrow.int16()),
            ('month', pyarrow.int8()),
            ('day', pyarrow.int8()),
            ('hour', pyarrow.int8()),
            ('feed_timestamp', pyarrow.int64()),
            # entity
            ('entity_id', pyarrow.string()), # actual label: id
            # entity -> trip_update
            ('timestamp', pyarrow.int64()),
            ('stop_time_update', pyarrow.list_(
                pyarrow.struct([
                    pyarrow.field('departure', pyarrow.struct([
                        pyarrow.field('time', pyarrow.int64()),
                        pyarrow.field('uncertainty', pyarrow.int64()),
                    ])),
                    pyarrow.field('stop_id', pyarrow.string()),
                    pyarrow.field('stop_sequence', pyarrow.int64()),
                    pyarrow.field('arrival', pyarrow.struct([
                        pyarrow.field('time', pyarrow.int64()),
                        pyarrow.field('uncertainty', pyarrow.int64()),
                    ])),
                    pyarrow.field('schedule_relationship', pyarrow.string()),
                    pyarrow.field('boarding_status',pyarrow.string()),
                ])
            )),
            # entity -> trip_update -> trip
            ('direction_id', pyarrow.int64()),
            ('route_id', pyarrow.string()),
            ('start_date', pyarrow.string()),
            ('start_time', pyarrow.string()),
            ('trip_id', pyarrow.string()),
            ('route_pattern_id', pyarrow.string()),
            ('schedule_relationship', pyarrow.string()),
            # entity -> trip_update -> vehicle
            ('vehicle_id', pyarrow.string()), # actual label: id
            ('vehicle_label', pyarrow.string()), # actual label: label
        ])

    def record_from_entity(self, entity: dict) -> dict:
        transform_schema = {
            'entity': (
                ('id','entity_id'),
            ),
            'entity,trip_update': (
                ('timestamp',),
                ('stop_time_update',),
            ),
            'entity,trip_update,trip': (
                ('direction_id',),
                ('route_id',),
                ('start_date',),
                ('start_time',),
                ('trip_id',),
                ('route_pattern_id',),
                ('schedule_relationship',),
            ),
            'entity,trip_update,vehicle': (
                ('id','vehicle_id',),
                ('label','vehicle_label',),
            )
        }

        def drill_entity(f:str) -> dict:
            ret_dict = entity
            for k in f.split(',')[1:]:
                if ret_dict.get(k) is None:
                    return None
                ret_dict = ret_dict.get(k)
            return ret_dict

        record = {}
        for drill_keys in transform_schema.keys():
            pull_dict = drill_entity(drill_keys)
            for get_field in transform_schema[drill_keys]:
                if pull_dict is None:
                    record[get_field[-1]] = None
                else:
                    record[get_field[-1]] = pull_dict.get(get_field[0])

        return record

