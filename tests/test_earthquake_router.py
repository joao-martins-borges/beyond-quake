from datetime import datetime, timezone
from ingestion.usgs import USGS
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")


def test_parse_earthquakes_basic():
    sample = {
        "features": [
            {
                "id": "abc123",
                "properties": {
                    "place": "Somewhere",
                    "mag": 4.4,
                    "time": 1725780905000,
                    "updated": 1725781905000,
                },
                "geometry": {"coordinates": [1.1, 2.2, 10.0]},
            }
        ]
    }

    logger.info("Parsing features: count=%d", len(sample["features"]))
    quakes = USGS().parse_earthquakes(sample)
    logger.info("Parsed earthquakes: count=%d", len(quakes))
    assert len(quakes) == 1
    q = quakes[0]
    logger.info(
        "Parse earthquake: id=%s location=%s mag=%s depth_km=%s",
        q["id"], q["location"], q["magnitude"], q["depth_km"],
    )
    assert q["id"] == "abc123"
    assert q["location"] == "Somewhere"
    assert q["magnitude"] == 4.4
    assert q["depth_km"] == 10.0
    assert q["time_utc"] == datetime.fromtimestamp(1725780905, tz=timezone.utc)
    assert q["updated_utc"] == datetime.fromtimestamp(1725781905, tz=timezone.utc)