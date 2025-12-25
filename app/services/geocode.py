import requests

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

def get_coordinates(location: str):
    params = {
        "q": location,
        "format": "json",
        "limit": 1
    }

    headers = {
        "User-Agent": "business-finder-app"
    }

    response = requests.get(NOMINATIM_URL, params=params, headers=headers)
    response.raise_for_status()

    data = response.json()

    if not data:
        return None

    return float(data[0]["lat"]), float(data[0]["lon"])
