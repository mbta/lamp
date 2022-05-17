import pyarrow

from .config_base import ConfigDetail
from .config_base import ConfigType

class RtAlertsDetail(ConfigDetail):
    @property
    def config_type(self) -> ConfigType:
        return ConfigType.RT_ALERTS

    @property
    def export_schema(self) -> pyarrow.schema:
        ...

    def record_from_entity(self, entity: dict) -> dict:
        ...

