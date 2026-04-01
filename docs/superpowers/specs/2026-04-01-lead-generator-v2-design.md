# Creative Monk Lead Generator v2 — Design Spec

## Overview
Transform the generic "Business Finder" into "Creative Monk Lead Engine" — an AI-powered lead intelligence tool for Creative Monk's sales team. Finds businesses via OSM, scores them by digital presence weakness, and uses Gemini AI to generate personalized outreach pitches.

## Architecture
- **Backend**: FastAPI (existing), add Gemini AI endpoints
- **Frontend**: Single-page Tailwind app, Creative Monk branded
- **Data**: OSM/Overpass (free), Gemini API for AI analysis
- **Storage**: None — all in-memory, no database, no logs
- **Deploy**: Same DO droplet (167.99.97.126), same CI/CD pipeline

## Backend Endpoints

### Existing (keep)
- `GET /health` — health check
- `GET /find-businesses` — OSM search (niche, location, radius_km)

### New
- `POST /ai/analyze-lead` — Gemini analyzes a single business's website/presence
  - Input: `{ name, website_url?, category, address? }`
  - Output: `{ score, website_analysis, missing_services[], pitch_message, priority }`
- `POST /ai/generate-pitch` — Gemini generates personalized outreach for a lead
  - Input: `{ name, category, website_url?, has_website, has_phone, has_social, location }`
  - Output: `{ whatsapp_pitch, email_pitch, recommended_services[] }`
- `POST /ai/bulk-summary` — Gemini summarizes a batch of leads
  - Input: `{ leads[], niche, location }`
  - Output: `{ summary, total_hot, total_warm, total_cold, top_opportunities[] }`

## Lead Scoring (client-side, fast)
- **HOT** (no website OR no phone) — prime target, needs Creative Monk
- **WARM** (has website but no social, or basic website) — upsell opportunity
- **COLD** (has website + phone + social) — harder sell but still potential

## AI Integration (Gemini 2.0 Flash)
- Model: `gemini-2.0-flash` (fast, cheap, good enough)
- System prompt includes Creative Monk's services, brand voice, pricing approach
- Each AI call is independent — no conversation state needed

## Frontend Design

### Branding
- Primary: Orange (#FF6600) matching Creative Monk
- Dark header/hero area
- White cards on light gray background
- Font: System fonts (Inter-like via Tailwind defaults)

### Layout
1. **Header**: "Creative Monk Lead Engine" with orange accent
2. **Search Section**: Industry chips (pre-built niches) + custom search form
3. **Results Summary Bar**: "Found X leads — Y hot, Z warm" + "AI Analyze All" button
4. **Lead Cards Grid**: Each card shows business info, lead score badge, action buttons
5. **Export Bar**: CSV download with AI pitches included

### Lead Card Design
- Business name + category badge
- Score badge (HOT/WARM/COLD with color)
- Contact info (phone, website, address)
- Action buttons: Call, WhatsApp, Maps, Website
- "Generate AI Pitch" button → expands card with personalized message
- Copy pitch to clipboard button

### Pre-built Industry Niches
Restaurants, Cafes, Hotels, Salons & Spas, Gyms & Fitness, Real Estate, Clinics & Hospitals, Coaching Institutes, Boutiques & Fashion, Wedding Planners, Auto Dealers, Dental Clinics, Pet Shops, Photography Studios

## File Structure
```
app/
  main.py          — FastAPI app with all endpoints
  services/
    geocode.py     — existing, unchanged
    overpass.py    — existing, unchanged
    gemini.py      — NEW: Gemini AI integration
app/templates/
  index.html       — complete rewrite
static/
  app.js           — can be removed (all JS inline in template)
```

## No-gos
- No database, no file storage, no logs
- No user accounts or auth
- No paid APIs beyond Gemini
- No external CSS/JS beyond Tailwind CDN + Font Awesome CDN
