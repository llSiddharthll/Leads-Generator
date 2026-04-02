"""
Lead Engine Search - Multi-source business discovery.

Primary:   DuckDuckGo Maps via webscout (structured data, social, hours)
Secondary: Google Local scraping (ratings, reviews, phone numbers)
Tertiary:  Google Search scraping (catch stragglers)

All free, no API keys required.
"""

import hashlib
import re
import time
import logging
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from urllib.parse import urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# --- webscout import (optional but recommended) ---
try:
    from webscout import DuckDuckGoSearch
    HAS_WEBSCOUT = True
except ImportError:
    HAS_WEBSCOUT = False
    logger.warning("webscout not installed — run: pip install webscout")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Business:
    name: Optional[str] = None
    category: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    opening_hours: Optional[dict] = None
    facebook: Optional[str] = None
    instagram: Optional[str] = None
    twitter: Optional[str] = None
    linkedin: Optional[str] = None
    youtube: Optional[str] = None
    source: Optional[str] = None
    image: Optional[str] = None
    description: Optional[str] = None
    place_id: Optional[str] = None

    def hash_id(self) -> str:
        parts = [
            (self.name or "").lower().strip(),
            re.sub(r"[^0-9]", "", self.phone or "")[-10:],
            (self.address or "").lower().strip()[:40],
        ]
        return hashlib.md5("|".join(parts).encode()).hexdigest()

    def is_valid(self) -> bool:
        if not self.name:
            return False
        low = self.name.lower().strip()
        if low in {"unknown", "none", "null", "", "na", "n/a", "yes", "no"} or len(low) <= 2:
            return False
        return True

    def to_dict(self) -> Dict:
        contact: Dict[str, str] = {}
        if self.phone:      contact["phone"] = self.phone
        if self.website:    contact["website"] = self.website
        if self.email:      contact["email"] = self.email
        if self.facebook:   contact["facebook"] = self.facebook
        if self.instagram:  contact["instagram"] = self.instagram
        if self.twitter:    contact["twitter"] = self.twitter
        if self.linkedin:   contact["linkedin"] = self.linkedin
        if self.youtube:    contact["youtube"] = self.youtube

        address: Dict[str, str] = {}
        if self.address:    address["street"] = self.address
        if self.city:       address["city"] = self.city

        d: Dict = {"name": self.name}
        if self.category:       d["category"] = self.category
        if contact:             d["contact"] = contact
        if address:             d["address"] = address
        if self.latitude:       d["latitude"] = self.latitude
        if self.longitude:      d["longitude"] = self.longitude
        if self.rating:         d["rating"] = self.rating
        if self.review_count:   d["review_count"] = self.review_count
        if self.opening_hours:  d["opening_hours"] = self.opening_hours
        if self.source:         d["source"] = self.source
        if self.image:          d["image"] = self.image
        if self.description:    d["description"] = self.description
        return d


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    cleaned = "".join(c for c in str(phone) if c.isdigit() or c == "+")
    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]
    if cleaned and len(cleaned) == 10 and not cleaned.startswith("+"):
        cleaned = "+91" + cleaned
    return cleaned if cleaned and len(cleaned) >= 10 else None


def _normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    url = str(url).strip()
    if len(url) < 4 or " " in url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    p = urlparse(url)
    return url if p.scheme and p.netloc else None


def _clean_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    name = " ".join(str(name).strip().split())
    name = re.sub(r"\s*\([^)]*\)\s*$", "", name)
    name = re.sub(r"\s*\[[^\]]*\]\s*$", "", name)
    return name.strip() or None


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _deduplicate(businesses: List[Business]) -> List[Business]:
    seen_hashes: Set[str] = set()
    seen_names: Set[str] = set()
    unique: List[Business] = []

    for b in businesses:
        if not b.is_valid():
            continue
        h = b.hash_id()
        if h in seen_hashes:
            continue
        name_key = (b.name or "").lower().strip()
        if name_key in seen_names:
            continue
        seen_hashes.add(h)
        seen_names.add(name_key)
        unique.append(b)

    return unique


# ---------------------------------------------------------------------------
# Source 1: DuckDuckGo Maps via webscout
# ---------------------------------------------------------------------------

def _search_ddg_maps(niche: str, lat: float, lon: float,
                     radius_km: float, location: str) -> List[Business]:
    """Primary search — structured business data from DDG Maps."""
    if not HAS_WEBSCOUT:
        return []

    businesses: List[Business] = []
    queries = [niche, f"best {niche}", f"{niche} shop"]

    for query in queries:
        try:
            ddg = DuckDuckGoSearch()
            results = ddg.maps(
                query,
                place=location if location else None,
                latitude=str(lat),
                longitude=str(lon),
                radius=max(1, int(radius_km)),
                max_results=40,
            )
            for r in results:
                name = _clean_name(r.get("title"))
                if not name:
                    continue

                hours_raw = r.get("hours")
                opening_hours = None
                if isinstance(hours_raw, dict):
                    opening_hours = hours_raw

                # Clean address (DDG sometimes puts "· Restaurant" type data in address)
                raw_addr = r.get("address") or ""
                if raw_addr.startswith("·") or len(raw_addr) < 5:
                    raw_addr = ""

                # Normalize source field (DDG returns full URLs like tripadvisor.com/...)
                raw_source = r.get("source", "")
                if "tripadvisor" in str(raw_source).lower():
                    source = "tripadvisor"
                elif "apple" in str(raw_source).lower():
                    source = "apple_maps"
                elif raw_source and len(str(raw_source)) < 30:
                    source = str(raw_source)
                else:
                    source = "ddg_maps"

                businesses.append(Business(
                    name=name,
                    category=r.get("category", niche),
                    phone=_normalize_phone(r.get("phone")),
                    website=_normalize_url(r.get("url")),
                    address=raw_addr or None,
                    city=location,
                    latitude=r.get("latitude"),
                    longitude=r.get("longitude"),
                    facebook=r.get("facebook"),
                    instagram=r.get("instagram"),
                    twitter=r.get("twitter"),
                    source=source,
                    image=r.get("image"),
                    description=r.get("desc"),
                    opening_hours=opening_hours,
                ))
        except Exception as e:
            logger.warning("DDG maps query %r failed: %s", query, e)
    return businesses


# ---------------------------------------------------------------------------
# Source 2: Google Local search scraping
# ---------------------------------------------------------------------------

_GOOGLE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}


def _search_google_local(niche: str, location: str) -> List[Business]:
    """Scrape Google Local Pack (tbm=lcl) for ratings + phone numbers."""
    businesses: List[Business] = []
    queries = [
        f"{niche} in {location}",
        f"best {niche} {location}",
    ]

    for query in queries:
        try:
            resp = requests.get(
                "https://www.google.com/search",
                params={"q": query, "tbm": "lcl", "hl": "en", "num": "20"},
                headers=_GOOGLE_HEADERS,
                timeout=12,
            )
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for div in soup.find_all("div", class_="rllt__details"):
                name_el = div.select_one(".dbg0pd, .OSrXXb")
                if not name_el:
                    continue
                name = _clean_name(name_el.get_text())
                if not name:
                    continue

                text = div.get_text(separator=" | ")

                # Extract phone — look for Indian phone patterns
                phone_m = re.search(r"(\+?\d[\d\s\-]{8,}\d)", text)
                phone = _normalize_phone(phone_m.group(1)) if phone_m else None

                # Rating: "4.1" shown in Y0A0hc or yi40Hd spans
                rating = None
                rating_el = div.select_one(".yi40Hd, .Y0A0hc")
                if rating_el:
                    rat_text = rating_el.get_text().strip()
                    try:
                        r_val = round(float(rat_text), 1)
                        if 1.0 <= r_val <= 5.0:
                            rating = r_val
                    except ValueError:
                        pass
                # Fallback: regex from text
                if not rating:
                    rat_m = re.search(r"\b([1-5]\.\d)\b", text)
                    if rat_m:
                        try:
                            rating = round(float(rat_m.group(1)), 1)
                        except ValueError:
                            pass

                # Review count: "(3K)" or "(1,234)" or "(456)"
                review_count = None
                rev_m = re.search(r"\(([\d,.]+[KkMm]?)\)", text)
                if rev_m:
                    try:
                        rv = rev_m.group(1).replace(",", "")
                        if rv.upper().endswith("K"):
                            review_count = int(float(rv[:-1]) * 1000)
                        elif rv.upper().endswith("M"):
                            review_count = int(float(rv[:-1]) * 1000000)
                        else:
                            review_count = int(rv)
                    except (ValueError, IndexError):
                        pass

                # Business status (open/closed)
                is_open = None
                if "closed" in text.lower():
                    is_open = False
                elif "open" in text.lower():
                    is_open = True

                # Try to grab address snippet — match real address indicators
                addr_parts = [p.strip() for p in text.split("|")]
                address = None
                _addr_re = re.compile(
                    r"\b(?:road|rd|street|st\b\.?|sector|sec|block|lane|nagar|colony|"
                    r"market|phase|plot|floor|near|opp|chowk|marg|path|avenue|ave)\b",
                    re.I,
                )
                for part in addr_parts:
                    part_clean = part.strip().lstrip("·").strip()
                    if len(part_clean) < 5:
                        continue
                    if _addr_re.search(part_clean):
                        address = part_clean
                        break

                # Extract category from text (usually after "·", like "· Cafe")
                gcat = niche
                for part in addr_parts:
                    part_clean = part.strip().lstrip("·").strip()
                    if not part_clean or len(part_clean) > 25 or len(part_clean) < 3:
                        continue
                    low = part_clean.lower()
                    # Skip non-category parts
                    if part_clean[0].isdigit() or low.startswith("("):
                        continue
                    if _addr_re.search(part_clean):
                        continue
                    if any(kw in low for kw in ["open", "close", "dine", "take", "deliver", "rated"]):
                        continue
                    if "₹" in part_clean or "$" in part_clean or "€" in part_clean:
                        continue
                    if low == name.lower():
                        continue
                    if re.match(r"^\d", part_clean) or re.match(r"^\(", part_clean):
                        continue
                    # Likely a category label
                    gcat = part_clean
                    break

                businesses.append(Business(
                    name=name,
                    category=gcat,
                    phone=phone,
                    city=location,
                    rating=rating,
                    review_count=review_count,
                    address=address,
                    source="google_local",
                ))

            time.sleep(0.8)
        except Exception as e:
            logger.warning("Google local search failed for %r: %s", query, e)
    return businesses


# ---------------------------------------------------------------------------
# Source 3: Google regular search (text results → discover websites)
# ---------------------------------------------------------------------------

def _search_google_text(niche: str, location: str) -> List[Business]:
    """Scrape regular Google results to find business websites and info."""
    businesses: List[Business] = []

    queries = [
        f"{niche} in {location} contact number",
        f"top {niche} {location}",
    ]

    for query in queries:
        try:
            resp = requests.get(
                "https://www.google.com/search",
                params={"q": query, "hl": "en", "num": "20"},
                headers=_GOOGLE_HEADERS,
                timeout=12,
            )
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Extract from search result snippets
            for g in soup.select("div.g, div.tF2Cxc"):
                link_el = g.select_one("a[href]")
                title_el = g.select_one("h3")
                snippet_el = g.select_one(".VwiC3b, .IsZvec, span.st")

                if not link_el or not title_el:
                    continue

                href = link_el.get("href", "")
                if not href.startswith("http"):
                    continue
                # Skip aggregator/directory sites
                skip_domains = ["justdial", "sulekha", "yelp", "tripadvisor", "zomato",
                                "swiggy", "indiamart", "tradeindia", "yellow", "facebook",
                                "instagram", "twitter", "linkedin", "youtube", "wikipedia",
                                "google.com", "govt"]
                domain = urlparse(href).netloc.lower()
                if any(s in domain for s in skip_domains):
                    continue

                title = _clean_name(title_el.get_text())
                if not title:
                    continue

                snippet = snippet_el.get_text() if snippet_el else ""

                # Extract phone from snippet
                phone_m = re.search(r"(\+?\d[\d\s\-]{8,}\d)", snippet)
                phone = _normalize_phone(phone_m.group(1)) if phone_m else None

                # Extract email from snippet
                email_m = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", snippet)
                email = email_m.group(0) if email_m else None

                businesses.append(Business(
                    name=title,
                    category=niche,
                    phone=phone,
                    email=email,
                    website=_normalize_url(href),
                    city=location,
                    source="google_text",
                ))

            time.sleep(0.8)
        except Exception as e:
            logger.warning("Google text search failed for %r: %s", query, e)
    return businesses


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------

def _merge_into(primary: List[Business], *secondaries: List[Business]) -> List[Business]:
    """Merge secondary lists into primary, enriching matches by name."""
    by_name: Dict[str, Business] = {}
    for b in primary:
        key = (b.name or "").lower().strip()
        if key:
            by_name[key] = b

    for secondary in secondaries:
        for b in secondary:
            key = (b.name or "").lower().strip()
            if not key:
                continue
            if key in by_name:
                existing = by_name[key]
                if b.rating and not existing.rating:
                    existing.rating = b.rating
                if b.review_count and not existing.review_count:
                    existing.review_count = b.review_count
                if b.phone and not existing.phone:
                    existing.phone = b.phone
                if b.website and not existing.website:
                    existing.website = b.website
                if b.email and not existing.email:
                    existing.email = b.email
                if b.address and not existing.address:
                    existing.address = b.address
                if b.facebook and not existing.facebook:
                    existing.facebook = b.facebook
                if b.instagram and not existing.instagram:
                    existing.instagram = b.instagram
                if b.twitter and not existing.twitter:
                    existing.twitter = b.twitter
            else:
                by_name[key] = b
                primary.append(b)

    return primary


# ---------------------------------------------------------------------------
# Website enrichment (crawl business websites for social links + emails)
# ---------------------------------------------------------------------------

_SOCIAL_PATTERNS = {
    "facebook":  re.compile(r"https?://(?:www\.)?facebook\.com/[\w.\-]+", re.I),
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/[\w.\-]+", re.I),
    "twitter":   re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[\w.\-]+", re.I),
    "linkedin":  re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in)/[\w.\-]+", re.I),
    "youtube":   re.compile(r"https?://(?:www\.)?youtube\.com/(?:@|channel/|c/)[\w.\-]+", re.I),
}

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Emails to ignore (generic/spam)
_IGNORE_EMAILS = {"noreply", "no-reply", "support@wix", "support@squarespace",
                  "support@wordpress", "example", "test@", "user@"}


def _crawl_website(business: Business) -> Business:
    """Visit a business website and extract social links + emails."""
    if not business.website:
        return business
    try:
        resp = requests.get(
            business.website,
            headers={
                "User-Agent": _GOOGLE_HEADERS["User-Agent"],
                "Accept": "text/html",
            },
            timeout=8,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return business

        html = resp.text[:200_000]  # cap at 200KB

        # Extract social links
        for key, pattern in _SOCIAL_PATTERNS.items():
            if getattr(business, key):
                continue
            m = pattern.search(html)
            if m:
                setattr(business, key, m.group(0))

        # Extract emails
        if not business.email:
            emails = _EMAIL_RE.findall(html)
            for email in emails:
                low = email.lower()
                if any(skip in low for skip in _IGNORE_EMAILS):
                    continue
                if low.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
                    continue
                business.email = email
                break

    except Exception:
        pass
    return business


def _enrich_websites(businesses: List[Business], max_workers: int = 8) -> List[Business]:
    """Crawl websites in parallel for social/email enrichment."""
    to_crawl = [b for b in businesses if b.website]
    if not to_crawl:
        return businesses

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_crawl_website, b): b for b in to_crawl}
        for future in as_completed(futures, timeout=30):
            try:
                future.result()
            except Exception:
                pass

    return businesses


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_businesses(lat: float, lon: float, radius_km: float,
                   niche: str, location: str = "") -> List[Dict]:
    """
    Search for businesses using multiple free sources, merge, enrich, and
    return a list of lead dicts sorted by opportunity (hottest first).
    """
    lat, lon, radius_km = float(lat), float(lon), float(radius_km)
    if radius_km <= 0 or radius_km > 50:
        raise ValueError("Radius must be between 0.1 and 50 km")

    niche_clean = str(niche).strip().lower()

    # --- Phase 1: Discovery (parallel) ---
    ddg_results: List[Business] = []
    google_local: List[Business] = []
    google_text: List[Business] = []

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_ddg = pool.submit(_search_ddg_maps, niche_clean, lat, lon, radius_km, location)
        f_glocal = pool.submit(_search_google_local, niche_clean, location) if location else None
        f_gtext = pool.submit(_search_google_text, niche_clean, location) if location else None

        try:
            ddg_results = f_ddg.result(timeout=45)
        except Exception as e:
            logger.error("DDG search failed: %s", e)

        if f_glocal:
            try:
                google_local = f_glocal.result(timeout=20)
            except Exception as e:
                logger.error("Google local failed: %s", e)

        if f_gtext:
            try:
                google_text = f_gtext.result(timeout=20)
            except Exception as e:
                logger.error("Google text failed: %s", e)

    logger.info("Sources: DDG=%d  GLocal=%d  GText=%d",
                len(ddg_results), len(google_local), len(google_text))

    # --- Phase 2: Merge ---
    all_biz = _merge_into(ddg_results, google_local, google_text)

    # --- Phase 3: Deduplicate ---
    unique = _deduplicate(all_biz)

    # --- Phase 4: Website enrichment (parallel crawl) ---
    unique = _enrich_websites(unique, max_workers=10)

    # --- Phase 5: Sort (hottest leads first) ---
    def lead_sort_key(b: Business):
        gaps = 0
        if not b.website:   gaps += 3
        if not b.facebook and not b.instagram: gaps += 2
        if not b.phone:     gaps += 1
        if not b.email:     gaps += 1
        return (-gaps, (b.name or "").lower())

    unique.sort(key=lead_sort_key)

    return [b.to_dict() for b in unique]
