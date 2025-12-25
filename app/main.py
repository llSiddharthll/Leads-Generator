from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request

from app.services.geocode import get_coordinates
from app.services.overpass import get_businesses

app = FastAPI(title="Business Finder")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="app/templates")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/find-businesses")
def find_businesses(
    niche: str = Query(...),
    location: str = Query(...),
    radius_km: float = Query(...)
):
    coords = get_coordinates(location)
    if not coords:
        return {"count": 0, "businesses": []}

    lat, lon = coords
    businesses = get_businesses(lat, lon, radius_km, niche)

    return {
        "count": len(businesses),
        "businesses": businesses
    }
