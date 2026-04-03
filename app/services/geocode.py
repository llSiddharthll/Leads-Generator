import time
import requests
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


@lru_cache(maxsize=200)
def get_coordinates(location: str):
    """Geocode a location string to (lat, lon). Cached to avoid rate limits."""
    params = {
        "q": location,
        "format": "json",
        "limit": 1
    }

    headers = {
        "User-Agent": "CreativeMonkLeadEngine/3.0 (contact: info@thecreativemonk.in)"
    }

    for attempt in range(3):
        try:
            response = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
            if response.status_code == 429:
                wait = 2 ** attempt
                logger.warning("Nominatim rate limited, waiting %ds...", wait)
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            if not data:
                return None
            return float(data[0]["lat"]), float(data[0]["lon"])
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                time.sleep(2 ** attempt)
                continue
            raise
        except Exception:
            if attempt < 2:
                time.sleep(1)
                continue
            raise

    return None
