import requests
import json
from typing import Optional

GEMINI_API_KEY = "AIzaSyAkrhQIBoUHkXfpPjQdXN3aiGzp3EAUlSo"
GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_MODELS = ["gemini-2.5-flash-lite", "gemini-2.0-flash-lite", "gemini-2.0-flash"]

CREATIVE_MONK_CONTEXT = """You are the AI sales strategist for Creative Monk (The Creative Monk), a full-service digital agency in Zirakpur, Punjab, India.

COMPLETE SERVICE CATALOG (pitch ANY of these based on the lead's needs):

1. SOCIAL MEDIA MARKETING
   - Instagram, Facebook, LinkedIn, YouTube management
   - Content calendars, reels/shorts creation, community management
   - Paid social campaigns (Meta Ads)
   - Ideal for: restaurants, cafes, salons, gyms, boutiques, hotels

2. SEARCH ENGINE OPTIMIZATION (SEO)
   - Local SEO, Google Business Profile optimization
   - On-page/off-page SEO, keyword ranking
   - Ideal for: clinics, real estate, coaching, lawyers, hotels

3. PPC ADVERTISING
   - Google Ads, Meta Ads, display campaigns
   - Lead generation campaigns, remarketing
   - Ideal for: real estate, coaching institutes, hospitals, e-commerce

4. WEBSITE DEVELOPMENT
   - WordPress, Shopify, Custom React/Next.js
   - E-commerce stores, landing pages, portfolio sites
   - Ideal for: ANY business without a website

5. GRAPHIC DESIGN & BRANDING
   - Logo design, brand identity, packaging design
   - Social media creatives, menu cards, brochures
   - Ideal for: new businesses, restaurants, boutiques, startups

6. PHOTOGRAPHY & VIDEOGRAPHY
   - Product photography, food photography
   - Brand shoots, team photos, property shoots
   - Reels, YouTube content, ad films
   - Ideal for: restaurants, hotels, salons, real estate, gyms, boutiques

7. SaaS & CUSTOM SOFTWARE
   - Custom dashboards, CRM systems, booking systems
   - Automation tools, inventory management
   - Ideal for: growing businesses, chains, franchises

8. LOCAL BUSINESS MARKETING
   - Google Business Profile setup/optimization
   - Google Maps ranking, local citations
   - Review management, local directory listings
   - Ideal for: ANY local business

KEY FACTS:
- 342% average client ROI | 500+ projects | 250+ clients | 4.9/5 rating
- Contact: +91 94634 45566 | info@thecreativemonk.in
- Website: thecreativemonk.in

PITCH RULES:
- Be specific to the business type — a gym needs different services than a restaurant
- Lead with the most valuable service for THAT specific business
- Always mention 3-4 relevant services, not just one
- Use Indian business context (mention Google Maps visibility, Instagram reels, WhatsApp marketing)
- For businesses WITHOUT a website: lead with Google Business Profile + social media first (cheaper, faster ROI), then upsell website
- For businesses WITH a website: lead with social media management + SEO + photography
- Always be warm, respectful, non-pushy. Reference their specific business by name.
- Keep WhatsApp messages under 160 words. Keep emails under 220 words.
- Use hinglish naturally where it fits (e.g., "aapki" instead of "your" occasionally) for WhatsApp
"""


def _call_gemini(prompt: str, max_tokens: int = 1024) -> Optional[str]:
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.7},
    }
    for model in GEMINI_MODELS:
        try:
            url = f"{GEMINI_BASE}/{model}:generateContent?key={GEMINI_API_KEY}"
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 429:
                continue
            resp.raise_for_status()
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            continue
    return None


def _parse_json(text: str) -> Optional[dict]:
    if not text:
        return None
    try:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0]
        return json.loads(cleaned)
    except (json.JSONDecodeError, IndexError):
        return None


def analyze_lead(name: str, category: str, website_url: Optional[str] = None,
                 address: Optional[str] = None, has_phone: bool = False,
                 has_social: bool = False) -> Optional[dict]:
    prompt = f"""{CREATIVE_MONK_CONTEXT}

Analyze this business as a potential client. Think about ALL services they could need, not just websites:

Business: {name}
Category: {category}
Website: {website_url or 'NO WEBSITE'}
Address: {address or 'Unknown'}
Has Phone Listed Online: {'Yes' if has_phone else 'No'}
Has Social Media: {'Yes' if has_social else 'No'}

Consider: Does a {category} in India typically need social media? Photography? Google Maps presence? A booking system? Brand refresh?

Respond in STRICT JSON (no markdown, no code blocks):
{{
  "score": "HOT" or "WARM" or "COLD",
  "priority": 1-10,
  "analysis": "2-3 sentence analysis — what this business is likely missing and why they need help",
  "missing_services": ["List 4-5 specific Creative Monk services this business needs, ordered by priority"],
  "top_service": "The single most impactful service to pitch first",
  "approach_angle": "The specific pain point to lead with when contacting them",
  "estimated_monthly_value": "Estimated monthly retainer in INR if they sign up (e.g., ₹15,000-25,000/month)",
  "urgency": "Why they should act NOW (seasonal, competition, market trend)"
}}"""
    return _parse_json(_call_gemini(prompt, 600))


def generate_pitch(name: str, category: str, location: str,
                   has_website: bool = False, has_phone: bool = False,
                   has_social: bool = False, website_url: Optional[str] = None) -> Optional[dict]:
    digital_gaps = []
    if not has_website: digital_gaps.append("NO website")
    if not has_phone: digital_gaps.append("no phone listed online")
    if not has_social: digital_gaps.append("no social media presence found")
    if has_website and website_url: digital_gaps.append(f"has website: {website_url}")
    if not digital_gaps: digital_gaps.append("has basic online presence but likely underperforming")

    prompt = f"""{CREATIVE_MONK_CONTEXT}

Generate outreach messages for this potential client. Remember to pitch MULTIPLE services relevant to their business type, not just websites:

Business: {name}
Category: {category}
Location: {location}
Digital Gaps: {', '.join(digital_gaps)}

Think about what a {category} in {location} typically needs:
- Do they need food/product photography? Brand shoots?
- Social media presence for discovery?
- Google Maps / local SEO for walk-in traffic?
- A website or online ordering system?
- Print materials (menu cards, brochures)?

Respond in STRICT JSON (no markdown, no code blocks):
{{
  "whatsapp_pitch": "WhatsApp message (under 160 words). Friendly, specific. Mention their business name. Highlight 2 specific gaps. Mention a quick win (e.g., 'we can set up your Google Business Profile this week'). End with soft CTA. Sign: Team Creative Monk. Use light hinglish where natural.",
  "email_subject": "Compelling email subject line — specific to their business",
  "email_pitch": "Professional email (under 220 words). Mention their business by name. Identify 3 specific opportunities for their business type. Include one success stat. Soft CTA for free consultation. Sign: Team Creative Monk.",
  "recommended_services": ["Top 4 services to pitch, ordered by priority for THIS business type"],
  "conversation_starter": "A natural opening line for a cold call or walk-in visit to their business",
  "quick_win": "One thing Creative Monk can deliver in under 1 week that would immediately help this business"
}}"""
    return _parse_json(_call_gemini(prompt, 900))


def bulk_summary(leads: list, niche: str, location: str) -> Optional[dict]:
    hot = sum(1 for l in leads if not l.get("contact", {}).get("website"))
    warm = sum(1 for l in leads if l.get("contact", {}).get("website")
               and not l.get("contact", {}).get("facebook")
               and not l.get("contact", {}).get("instagram"))
    cold = len(leads) - hot - warm

    has_phone = sum(1 for l in leads if l.get("contact", {}).get("phone"))
    has_website = sum(1 for l in leads if l.get("contact", {}).get("website"))
    names = [l.get("name", "?") for l in leads[:25]]

    prompt = f"""{CREATIVE_MONK_CONTEXT}

Create a sales intelligence report for Creative Monk's team:

Search: {niche} businesses in {location}
Total Found: {len(leads)}
Without Website: {hot} (HOT leads)
Weak Presence: {warm} (WARM leads)
Has Presence: {cold} (COLD leads)
Have Phone Number: {has_phone}
Have Website: {has_website}
Sample Names: {', '.join(names)}

Think about what {niche} businesses in {location} typically need. Consider seasonality, local market, competition.

Respond in STRICT JSON (no markdown, no code blocks):
{{
  "summary": "3-4 sentence executive summary. How many leads, quality breakdown, total revenue opportunity.",
  "strategy": "Step-by-step outreach strategy for this batch (what to do this week)",
  "top_pitch_angle": "The single best pitch angle for {niche} businesses in {location} right now",
  "services_to_push": ["Top 3 services to pitch to {niche} businesses, with why"],
  "estimated_conversion": "Realistic conversion estimate with expected monthly revenue",
  "quick_wins": "Which leads to contact TODAY and what to offer them",
  "market_insight": "One insight about the {niche} market in {location} that the sales team should know"
}}"""

    result = _parse_json(_call_gemini(prompt, 700))
    if result:
        result["total_hot"] = hot
        result["total_warm"] = warm
        result["total_cold"] = cold
        return result
    return {
        "total_hot": hot, "total_warm": warm, "total_cold": cold,
        "summary": f"Found {len(leads)} {niche} businesses in {location}. {hot} without website, {warm} with weak presence, {cold} with decent presence. {has_phone} have phone numbers for direct outreach."
    }
