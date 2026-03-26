# https://mbta-alerts-ui.s3.us-east-1.amazonaws.com/translated-feeds-staging/Alerts.json
# https://mbta-alerts-ui.s3.us-east-1.amazonaws.com/translated-feeds-staging/Alerts_enhanced.json
import gzip
from pathlib import Path
from typing import Optional

from duckdb import df
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType
from queue import Queue
import urllib.request


# Download and gzip the file
url = "https://mbta-alerts-ui.s3.us-east-1.amazonaws.com/translated-feeds-staging/Alerts.json"
gzip_path = Path("alerts.json.gz")

with urllib.request.urlopen(url) as response:
    with gzip.open(gzip_path, "wb") as f:
        f.write(response.read())

        # Initialize converter with RT_ALERTS config
        metadata_queue: Queue[Optional[str]] = Queue()
        converter = GtfsRtConverter(ConfigType.RT_ALERTS, metadata_queue)
        converter.files = [str(gzip_path)]
        converter.convert()
