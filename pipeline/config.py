"""Settings for the pipeline. Environment variables override the defaults."""

import os
from pathlib import Path

# Citi Bike publishes its live data in the General Bikeshare Feed Specification (GBFS).
FEEDS = {
    "station_status": "https://gbfs.citibikenyc.com/gbfs/en/station_status.json",
    "station_information": "https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
}

RAW_DIR = Path(os.environ.get("RAW_DIR", "data/raw"))
REQUEST_TIMEOUT_SECONDS = 30
