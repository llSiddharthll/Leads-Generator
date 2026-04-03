"""
Lead Engine Search - Multi-source business discovery with enrichment.

Pipeline:
1. DDG Maps (webscout) — structured data: phone, website, social, hours, coords
2. Google Local (tbm=lcl) — ratings, reviews, address, category
3. Merge — fuzzy name matching (handles accents, abbreviations)
4. Enrich missing — for businesses still lacking website/phone, search Google
5. Crawl websites — extract social links + emails from business websites
"""

import hashlib
import re
import time
import unicodedata
import logging
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

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

    def is_valid(self) -> bool:
        if not self.name:
            return False
        low = self.name.lower().strip()
        if low in {"unknown", "none", "null", "", "na", "n/a", "yes", "no"} or len(low) <= 2:
            return False
        return True

    def to_dict(self) -> Dict:
        contact: Dict[str, str] = {}
        if self.phone:    contact["phone"] = self.phone
        if self.website:  contact["website"] = self.website
        if self.email:    contact["email"] = self.email
        if self.facebook: contact["facebook"] = self.facebook
        if self.instagram: contact["instagram"] = self.instagram
        if self.twitter:  contact["twitter"] = self.twitter
        if self.linkedin: contact["linkedin"] = self.linkedin
        if self.youtube:  contact["youtube"] = self.youtube

        address: Dict[str, str] = {}
        if self.address: address["street"] = self.address
        if self.city:    address["city"] = self.city

        d: Dict = {"name": self.name}
        if self.category:     d["category"] = self.category
        if contact:           d["contact"] = contact
        if address:           d["address"] = address
        if self.latitude:     d["latitude"] = self.latitude
        if self.longitude:    d["longitude"] = self.longitude
        if self.rating:       d["rating"] = self.rating
        if self.review_count: d["review_count"] = self.review_count
        if self.opening_hours: d["opening_hours"] = self.opening_hours
        if self.source:       d["source"] = self.source
        if self.image:        d["image"] = self.image
        if self.description:  d["description"] = self.description
        return d


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm_key(name: str) -> str:
    """Normalize a business name for fuzzy matching.
    Strips accents, lowercases, removes punctuation/extra spaces."""
    if not name:
        return ""
    # Decompose unicode, strip combining marks (accents)
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase, strip punctuation except alphanumeric/space
    cleaned = re.sub(r"[^a-z0-9 ]", "", ascii_name.lower())
    return " ".join(cleaned.split())


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


_ADDR_RE = re.compile(
    r"\b(?:road|rd|street|sector|sec|block|lane|nagar|colony|"
    r"market|phase|plot|floor|near|opp|chowk|marg|path|avenue|ave|sco|scf|nh)\b",
    re.I,
)

_GOOGLE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ---------------------------------------------------------------------------
# Source 1: DuckDuckGo Maps via webscout
# ---------------------------------------------------------------------------

def _search_ddg_maps(niche: str, lat: float, lon: float,
                     radius_km: float, location: str) -> List[Business]:
    if not HAS_WEBSCOUT:
        return []

    businesses: List[Business] = []
    queries = [
        f"{niche} in {location}",
        f"best {niche} near {location}",
        f"{niche} {location}",
    ]

    for query in queries:
        try:
            ddg = DuckDuckGoSearch()
            results = ddg.maps(
                query,
                place=location or None,
                latitude=str(lat),
                longitude=str(lon),
                radius=max(1, int(radius_km)),
                max_results=50,
            )
            for r in results:
                name = _clean_name(r.get("title"))
                if not name:
                    continue

                hours_raw = r.get("hours")
                opening_hours = hours_raw if isinstance(hours_raw, dict) else None

                raw_addr = r.get("address") or ""
                if raw_addr.startswith("·") or len(raw_addr) < 5:
                    raw_addr = ""

                raw_source = str(r.get("source", ""))
                if "tripadvisor" in raw_source.lower():
                    source = "tripadvisor"
                elif "apple" in raw_source.lower():
                    source = "apple_maps"
                elif raw_source and len(raw_source) < 30:
                    source = raw_source
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
# Source 2: Google Local (tbm=lcl) — ratings, reviews, category
# ---------------------------------------------------------------------------

def _search_google_local(niche: str, location: str) -> List[Business]:
    businesses: List[Business] = []
    queries = [
        f"{niche} in {location}",
        f"best {niche} {location}",
        f"top {niche} near {location}",
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

                # Rating from dedicated span
                rating = None
                rating_el = div.select_one(".yi40Hd, .Y0A0hc")
                if rating_el:
                    try:
                        r_val = round(float(rating_el.get_text().strip()), 1)
                        if 1.0 <= r_val <= 5.0:
                            rating = r_val
                    except ValueError:
                        pass
                if not rating:
                    rat_m = re.search(r"\b([1-5]\.\d)\b", text)
                    if rat_m:
                        try:
                            rating = round(float(rat_m.group(1)), 1)
                        except ValueError:
                            pass

                # Review count: "(3K)" "(1,234)" "(456)"
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

                # Phone from text
                phone_m = re.search(r"(\+?\d[\d\s\-]{8,}\d)", text)
                phone = _normalize_phone(phone_m.group(1)) if phone_m else None

                # Address
                addr_parts = [p.strip() for p in text.split("|")]
                address = None
                for part in addr_parts:
                    part_clean = part.strip().lstrip("·").strip()
                    if len(part_clean) < 5:
                        continue
                    if _ADDR_RE.search(part_clean):
                        address = part_clean
                        break

                # Category (e.g., "Cafe", "Beauty salon")
                gcat = niche
                for part in addr_parts:
                    pc = part.strip().lstrip("·").strip()
                    if not pc or len(pc) > 25 or len(pc) < 3:
                        continue
                    low = pc.lower()
                    if pc[0].isdigit() or low.startswith("("):
                        continue
                    if _ADDR_RE.search(pc):
                        continue
                    if any(kw in low for kw in ["open", "close", "dine", "take", "deliver", "rated", "₹", "$", "€"]):
                        continue
                    if low == name.lower():
                        continue
                    gcat = pc
                    break

                businesses.append(Business(
                    name=name, category=gcat, phone=phone, city=location,
                    rating=rating, review_count=review_count, address=address,
                    source="google_local",
                ))

            time.sleep(0.5)
        except Exception as e:
            logger.warning("Google local failed for %r: %s", query, e)
    return businesses


# ---------------------------------------------------------------------------
# Merge — fuzzy name matching (accent-insensitive)
# ---------------------------------------------------------------------------

def _merge_business(target: Business, source: Business):
    """Copy non-empty fields from source into target where target is empty."""
    if source.rating and not target.rating:
        target.rating = source.rating
    if source.review_count and not target.review_count:
        target.review_count = source.review_count
    if source.phone and not target.phone:
        target.phone = source.phone
    if source.website and not target.website:
        target.website = source.website
    if source.email and not target.email:
        target.email = source.email
    if source.address and not target.address:
        target.address = source.address
    if source.latitude and not target.latitude:
        target.latitude = source.latitude
    if source.longitude and not target.longitude:
        target.longitude = source.longitude
    if source.facebook and not target.facebook:
        target.facebook = source.facebook
    if source.instagram and not target.instagram:
        target.instagram = source.instagram
    if source.twitter and not target.twitter:
        target.twitter = source.twitter
    if source.opening_hours and not target.opening_hours:
        target.opening_hours = source.opening_hours
    if source.image and not target.image:
        target.image = source.image
    if source.description and not target.description:
        target.description = source.description
    if source.category and (not target.category or target.category == target.city):
        target.category = source.category


def _merge_lists(*lists: List[Business]) -> List[Business]:
    """Merge multiple lists using fuzzy name matching (accent-insensitive)."""
    by_key: Dict[str, Business] = {}

    for biz_list in lists:
        for b in biz_list:
            if not b.is_valid():
                continue
            key = _norm_key(b.name)
            if not key:
                continue

            if key in by_key:
                _merge_business(by_key[key], b)
            else:
                # Also check if a substring match exists (e.g., "cafe well being" in "cafe well being chandigarh")
                matched = False
                for existing_key in list(by_key.keys()):
                    if key in existing_key or existing_key in key:
                        if len(key) >= 6 and len(existing_key) >= 6:  # avoid tiny false matches
                            _merge_business(by_key[existing_key], b)
                            matched = True
                            break
                if not matched:
                    by_key[key] = b

    return list(by_key.values())


# ---------------------------------------------------------------------------
# Enrichment: find website/phone for businesses that are missing them
# ---------------------------------------------------------------------------

def _enrich_single(business: Business, location: str) -> Business:
    """Search Google for a single business to find its website and phone."""
    if business.website and business.phone:
        return business  # already has both

    query = f"{business.name} {location} contact"
    try:
        resp = requests.get(
            "https://www.google.com/search",
            params={"q": query, "hl": "en", "num": "5"},
            headers=_GOOGLE_HEADERS,
            timeout=10,
        )
        if resp.status_code != 200:
            return business

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text()

        # Extract phone from page
        if not business.phone:
            phone_m = re.search(r"(\+91[\s\-]?\d[\d\s\-]{8,}\d)", page_text)
            if not phone_m:
                phone_m = re.search(r"\b(0\d{2,4}[\s\-]\d{6,8})\b", page_text)
            if not phone_m:
                phone_m = re.search(r"\b(\d{10})\b", page_text)
            if phone_m:
                business.phone = _normalize_phone(phone_m.group(1))

        # Extract website from search results (skip directories)
        if not business.website:
            skip_domains = {
                "justdial", "sulekha", "yelp", "tripadvisor", "zomato", "swiggy",
                "indiamart", "tradeindia", "yellowpages", "facebook", "instagram",
                "twitter", "linkedin", "youtube", "wikipedia", "google", "bing",
                "quora", "reddit", "pinterest", "mapquest",
            }
            for a_tag in soup.select("a[href]"):
                href = a_tag.get("href", "")
                if not href.startswith("http"):
                    continue
                domain = urlparse(href).netloc.lower().replace("www.", "")
                if any(s in domain for s in skip_domains):
                    continue
                if domain and "." in domain:
                    business.website = _normalize_url(href)
                    break

        # Extract email
        if not business.email:
            email_m = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", page_text)
            if email_m:
                email = email_m.group(0).lower()
                if not any(s in email for s in ["noreply", "example", "test@", "wix", "squarespace"]):
                    business.email = email

    except Exception as e:
        logger.debug("Enrich failed for %s: %s", business.name, e)

    return business


def _enrich_missing(businesses: List[Business], location: str, max_workers: int = 5) -> List[Business]:
    """For businesses missing website or phone, search Google to find them."""
    to_enrich = [b for b in businesses if not b.website or not b.phone]
    if not to_enrich:
        return businesses

    # Limit to avoid rate-limiting (max 30 enrichment searches)
    to_enrich = to_enrich[:30]

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_enrich_single, b, location): b for b in to_enrich}
        for future in as_completed(futures, timeout=60):
            try:
                future.result()
            except Exception:
                pass

    return businesses


# ---------------------------------------------------------------------------
# Website crawling — extract social links + emails
# ---------------------------------------------------------------------------

_SOCIAL_PATTERNS = {
    "facebook":  re.compile(r"https?://(?:www\.)?facebook\.com/[\w.\-]+", re.I),
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/[\w.\-]+", re.I),
    "twitter":   re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[\w.\-]+", re.I),
    "linkedin":  re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in)/[\w.\-]+", re.I),
    "youtube":   re.compile(r"https?://(?:www\.)?youtube\.com/(?:@|channel/|c/)[\w.\-]+", re.I),
}

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_IGNORE_EMAILS = {"noreply", "no-reply", "support@wix", "support@squarespace",
                  "support@wordpress", "example", "test@", "user@"}


def _crawl_website(business: Business) -> Business:
    if not business.website:
        return business
    try:
        resp = requests.get(
            business.website,
            headers={"User-Agent": _GOOGLE_HEADERS["User-Agent"], "Accept": "text/html"},
            timeout=8,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return business

        html = resp.text[:200_000]

        for key, pattern in _SOCIAL_PATTERNS.items():
            if getattr(business, key):
                continue
            m = pattern.search(html)
            if m:
                setattr(business, key, m.group(0))

        if not business.email:
            for email in _EMAIL_RE.findall(html):
                low = email.lower()
                if any(s in low for s in _IGNORE_EMAILS):
                    continue
                if low.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
                    continue
                business.email = email
                break

        # Try to get phone from website if still missing
        if not business.phone:
            phone_m = re.search(r"(\+91[\s\-]?\d[\d\s\-]{7,}\d)", html)
            if not phone_m:
                phone_m = re.search(r'tel:(\+?\d[\d\-]{8,}\d)', html)
            if phone_m:
                business.phone = _normalize_phone(phone_m.group(1))

    except Exception:
        pass
    return business


def _crawl_websites(businesses: List[Business], max_workers: int = 10) -> List[Business]:
    to_crawl = [b for b in businesses if b.website]
    if not to_crawl:
        return businesses

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_crawl_website, b): b for b in to_crawl}
        for future in as_completed(futures, timeout=40):
            try:
                future.result()
            except Exception:
                pass

    return businesses


# ---------------------------------------------------------------------------
# Deduplication (final pass)
# ---------------------------------------------------------------------------

def _deduplicate(businesses: List[Business]) -> List[Business]:
    seen: Set[str] = set()
    unique: List[Business] = []
    for b in businesses:
        if not b.is_valid():
            continue
        key = _norm_key(b.name)
        if key in seen:
            continue
        seen.add(key)
        unique.append(b)
    return unique


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_businesses(lat: float, lon: float, radius_km: float,
                   niche: str, location: str = "") -> List[Dict]:
    lat, lon, radius_km = float(lat), float(lon), float(radius_km)
    if radius_km <= 0 or radius_km > 50:
        raise ValueError("Radius must be between 0.1 and 50 km")

    niche_clean = str(niche).strip().lower()

    # --- Phase 1: Discovery (parallel) ---
    ddg_results: List[Business] = []
    google_local: List[Business] = []

    with ThreadPoolExecutor(max_workers=2) as pool:
        f_ddg = pool.submit(_search_ddg_maps, niche_clean, lat, lon, radius_km, location)
        f_glocal = pool.submit(_search_google_local, niche_clean, location) if location else None

        try:
            ddg_results = f_ddg.result(timeout=50)
        except Exception as e:
            logger.error("DDG search failed: %s", e)

        if f_glocal:
            try:
                google_local = f_glocal.result(timeout=25)
            except Exception as e:
                logger.error("Google local failed: %s", e)

    logger.info("Discovery: DDG=%d  GLocal=%d", len(ddg_results), len(google_local))

    # --- Phase 2: Merge (fuzzy name matching) ---
    merged = _merge_lists(ddg_results, google_local)

    # --- Phase 3: Deduplicate ---
    unique = _deduplicate(merged)

    # --- Phase 4: Enrich missing website/phone via Google search ---
    unique = _enrich_missing(unique, location, max_workers=5)

    # --- Phase 5: Crawl websites for social + email + phone ---
    unique = _crawl_websites(unique, max_workers=10)

    # --- Phase 6: Sort (hottest leads first) ---
    def lead_sort_key(b: Business):
        gaps = 0
        if not b.website:  gaps += 3
        if not b.facebook and not b.instagram: gaps += 2
        if not b.phone:    gaps += 1
        if not b.email:    gaps += 1
        return (-gaps, -(b.rating or 0), (b.name or "").lower())

    unique.sort(key=lead_sort_key)

    return [b.to_dict() for b in unique]
