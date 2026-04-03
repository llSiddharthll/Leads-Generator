import time
import requests
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# In-memory cache (survives across requests, not across restarts)
_geo_cache = {}


def get_coordinates(location: str):
    """Geocode a location string to (lat, lon). Uses cache + multiple fallbacks."""
    if not location:
        return None

    cache_key = location.strip().lower()
    if cache_key in _geo_cache:
        return _geo_cache[cache_key]

    result = _try_nominatim(location) or _try_ddg_geocode(location)
    if result:
        _geo_cache[cache_key] = result
    return result


def _try_nominatim(location: str):
    """Primary geocoder: OpenStreetMap Nominatim."""
    params = {"q": location, "format": "json", "limit": 1}
    headers = {"User-Agent": "CreativeMonkLeadEngine/3.0 (contact: info@thecreativemonk.in)"}

    for attempt in range(2):
        try:
            resp = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
            if resp.status_code == 429:
                logger.warning("Nominatim rate limited (attempt %d)", attempt + 1)
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            data = resp.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
            return None
        except Exception as e:
            logger.warning("Nominatim failed: %s", e)
            if attempt < 1:
                time.sleep(1)
    return None


def _try_ddg_geocode(location: str):
    """Fallback geocoder: use DuckDuckGo maps search to find coordinates."""
    try:
        from webscout import DuckDuckGoSearch
        ddg = DuckDuckGoSearch()
        results = ddg.maps(location, max_results=1)
        if results:
            lat = results[0].get("latitude")
            lon = results[0].get("longitude")
            if lat and lon:
                logger.info("DDG geocode fallback worked for %s: %s,%s", location, lat, lon)
                return float(lat), float(lon)
    except Exception as e:
        logger.warning("DDG geocode fallback failed: %s", e)
    return None
