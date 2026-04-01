from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel
from typing import Optional

from app.services.geocode import get_coordinates
from app.services.overpass import get_businesses
from app.services import gemini

app = FastAPI(title="Creative Monk Lead Engine")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="app/templates")


@app.get("/health")
def health_check():
    return {"status": "ok"}


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


class AnalyzeLeadRequest(BaseModel):
    name: str
    category: str
    website_url: Optional[str] = None
    address: Optional[str] = None
    has_phone: bool = False
    has_social: bool = False


@app.post("/ai/analyze-lead")
def analyze_lead(req: AnalyzeLeadRequest):
    result = gemini.analyze_lead(
        name=req.name,
        category=req.category,
        website_url=req.website_url,
        address=req.address,
        has_phone=req.has_phone,
        has_social=req.has_social,
    )
    if not result:
        return {"error": "AI analysis failed. Please try again."}
    return result


class GeneratePitchRequest(BaseModel):
    name: str
    category: str
    location: str
    has_website: bool = False
    has_phone: bool = False
    has_social: bool = False
    website_url: Optional[str] = None


@app.post("/ai/generate-pitch")
def generate_pitch(req: GeneratePitchRequest):
    result = gemini.generate_pitch(
        name=req.name,
        category=req.category,
        location=req.location,
        has_website=req.has_website,
        has_phone=req.has_phone,
        has_social=req.has_social,
        website_url=req.website_url,
    )
    if not result:
        return {"error": "Pitch generation failed. Please try again."}
    return result


class BulkSummaryRequest(BaseModel):
    leads: list
    niche: str
    location: str


@app.post("/ai/bulk-summary")
def bulk_summary(req: BulkSummaryRequest):
    result = gemini.bulk_summary(
        leads=req.leads,
        niche=req.niche,
        location=req.location,
    )
    if not result:
        return {"error": "Bulk analysis failed. Please try again."}
    return result
