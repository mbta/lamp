import pyarrow

from .config_base import ConfigDetails
from .config_base import ConfigType

class RtAlertsDetails(ConfigDetails):
    @property
    def config_type(self) -> ConfigType:
        return ConfigType.RT_ALERTS

    @property
    def export_schema(self) -> pyarrow.schema:
        ...

    def record_from_entity(self, entity: dict) -> dict:
        ...

