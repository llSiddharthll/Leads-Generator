import requests
import json
from typing import Optional

GEMINI_API_KEY = "AIzaSyAkrhQIBoUHkXfpPjQdXN3aiGzp3EAUlSo"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"

CREATIVE_MONK_CONTEXT = """You are an AI assistant for Creative Monk (The Creative Monk), a digital marketing agency based in Zirakpur, Punjab, India.

Creative Monk's Services:
- Social Media Marketing (Instagram, Facebook, LinkedIn, YouTube management)
- Search Engine Optimization (SEO)
- PPC Advertising (Google Ads, Meta Ads)
- Website Development (WordPress, Shopify, Custom React/Next.js, Static sites, Landing pages)
- Graphic Design (Logo, Branding, Social media creatives, Package design)
- Photography & Videography (Product shoots, Brand shoots, Reels/Shorts)
- SaaS Development (Custom web applications, dashboards, automation tools)
- Local Business Marketing (Google Business Profile optimization, Local SEO)
- Lead Generation campaigns

Key Facts:
- 500+ projects completed, 250+ happy clients, 4.9/5 rating
- Works with startups and established businesses across 15+ industries
- Contact: +91 94634 45566, info@thecreativemonk.in
- Website: thecreativemonk.in

Tone: Professional but friendly, confident, results-focused. Use Indian business context (INR, Indian market references). Keep messages concise and action-oriented."""


def _call_gemini(prompt: str, max_tokens: int = 1024) -> Optional[str]:
    """Make a request to Gemini API."""
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.7,
        },
    }

    try:
        resp = requests.post(GEMINI_URL, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        return None


def analyze_lead(name: str, category: str, website_url: Optional[str] = None,
                 address: Optional[str] = None, has_phone: bool = False,
                 has_social: bool = False) -> Optional[dict]:
    """Analyze a single lead and provide insights."""
    prompt = f"""{CREATIVE_MONK_CONTEXT}

Analyze this business as a potential client for Creative Monk:

Business: {name}
Category: {category}
Website: {website_url or 'NO WEBSITE'}
Address: {address or 'Unknown'}
Has Phone Listed: {'Yes' if has_phone else 'No'}
Has Social Media: {'Yes' if has_social else 'No'}

Respond in STRICT JSON format (no markdown, no code blocks):
{{
  "score": "HOT" or "WARM" or "COLD",
  "priority": 1-10 (10 = highest priority lead),
  "analysis": "2-3 sentence analysis of this lead's digital presence gaps",
  "missing_services": ["list of Creative Monk services this business likely needs"],
  "approach_angle": "The best angle to approach this business — what pain point to lead with",
  "estimated_value": "Estimated monthly retainer range in INR if they become a client"
}}"""

    result = _call_gemini(prompt, 512)
    if not result:
        return None

    try:
        cleaned = result.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0]
        return json.loads(cleaned)
    except (json.JSONDecodeError, IndexError):
        return None


def generate_pitch(name: str, category: str, location: str,
                   has_website: bool = False, has_phone: bool = False,
                   has_social: bool = False, website_url: Optional[str] = None) -> Optional[dict]:
    """Generate personalized outreach pitches for a lead."""
    digital_status = []
    if not has_website:
        digital_status.append("NO website")
    elif website_url:
        digital_status.append(f"has website: {website_url}")
    if not has_phone:
        digital_status.append("no phone listed online")
    if not has_social:
        digital_status.append("no social media presence found")
    if not digital_status:
        digital_status.append("has basic online presence")

    status_str = ", ".join(digital_status)

    prompt = f"""{CREATIVE_MONK_CONTEXT}

Generate outreach messages for this potential client:

Business: {name}
Category: {category}
Location: {location}
Digital Status: {status_str}

Create personalized, non-spammy outreach messages. Reference their specific business and what they're missing.

Respond in STRICT JSON format (no markdown, no code blocks):
{{
  "whatsapp_pitch": "A short WhatsApp message (under 200 words) — casual, friendly, to the point. Start with greeting, mention their business by name, highlight 1 specific gap, suggest a free consultation. End with 'Team Creative Monk' signature.",
  "email_subject": "Email subject line (compelling, not clickbait)",
  "email_pitch": "Professional email (under 250 words) — mention their business, identify 2-3 specific opportunities, include a soft CTA for a free consultation call. Sign off as Creative Monk team.",
  "recommended_services": ["top 3 services to pitch first"],
  "conversation_starter": "A single opening line if you meet them in person or cold-call"
}}"""

    result = _call_gemini(prompt, 800)
    if not result:
        return None

    try:
        cleaned = result.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0]
        return json.loads(cleaned)
    except (json.JSONDecodeError, IndexError):
        return None


def bulk_summary(leads: list, niche: str, location: str) -> Optional[dict]:
    """Generate a summary analysis of a batch of leads."""
    hot = sum(1 for l in leads if not l.get("contact", {}).get("website"))
    warm = sum(1 for l in leads if l.get("contact", {}).get("website") and not l.get("contact", {}).get("facebook") and not l.get("contact", {}).get("instagram"))
    cold = len(leads) - hot - warm

    lead_names = [l.get("name", "Unknown") for l in leads[:20]]

    prompt = f"""{CREATIVE_MONK_CONTEXT}

Summarize this batch of leads for Creative Monk's sales team:

Search: {niche} businesses in {location}
Total Found: {len(leads)}
Hot Leads (no website): {hot}
Warm Leads (website but weak social): {warm}
Cold Leads (decent presence): {cold}

Sample businesses: {', '.join(lead_names)}

Respond in STRICT JSON format (no markdown, no code blocks):
{{
  "summary": "3-4 sentence executive summary for the sales team — how many leads, quality breakdown, opportunity size",
  "strategy": "Recommended outreach strategy for this batch",
  "top_pitch_angle": "The single best pitch angle for {niche} businesses in {location}",
  "estimated_conversion": "Realistic estimate of how many might convert from {len(leads)} leads",
  "quick_wins": "Which leads to contact FIRST and why"
}}"""

    result = _call_gemini(prompt, 600)
    if not result:
        return None

    try:
        cleaned = result.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0]
        parsed = json.loads(cleaned)
        parsed["total_hot"] = hot
        parsed["total_warm"] = warm
        parsed["total_cold"] = cold
        return parsed
    except (json.JSONDecodeError, IndexError):
        return {"total_hot": hot, "total_warm": warm, "total_cold": cold,
                "summary": f"Found {len(leads)} {niche} businesses in {location}. {hot} have no website (hot leads), {warm} have weak digital presence (warm), {cold} have decent presence."}
