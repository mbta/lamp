from .converter import ConfigType
from .converter import Converter
from .convert_gtfs import GtfsConverter
from .convert_gtfs_rt import GtfsRtConverter
from .error import NoImplException


def get_converter(config_type: ConfigType) -> Converter:
    """
    Get the correct converter object for a given config type
    """
    if config_type.is_gtfs():
        return GtfsConverter(config_type)
    if config_type.is_gtfs_rt():
        return GtfsRtConverter(config_type)

    raise NoImplException(f"No Converter for Config Type {config_type}")
