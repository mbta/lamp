from dataclasses import dataclass

from lamp_py.runtime_utils.remote_files import (
    S3DeltaTable,
    S3Location,
    asset_event_source,
    asset_event_table,
    control_system_variables_source,
    control_system_variables_table,
    io_frame_source,
    io_frame_table,
)


@dataclass
class CloudEventProducer:
    """Bundle of CloudEvent information."""

    source: S3Location
    destination: S3DeltaTable


com_mbta_railds_asset_event = CloudEventProducer(source=asset_event_source, destination=asset_event_table)
com_mbta_railds_control_system_variables = CloudEventProducer(
    source=control_system_variables_source, destination=control_system_variables_table
)
com_mbta_railds_io_frame = CloudEventProducer(source=io_frame_source, destination=io_frame_table)

PRODUCERS: list[CloudEventProducer] = [
    com_mbta_railds_asset_event,
    com_mbta_railds_control_system_variables,
    com_mbta_railds_io_frame,
]
