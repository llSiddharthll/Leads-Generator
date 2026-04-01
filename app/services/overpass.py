import requests
import hashlib
import time
import re
from typing import List, Dict, Optional, Set
from dataclasses import dataclass
from urllib.parse import urlparse

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
REQUEST_TIMEOUT = 45
MAX_RETRIES = 2

# Comprehensive niche → OSM tag mapping
# Each niche maps to a list of (tag_key, tag_value) pairs to search
NICHE_TAGS = {
    "restaurant": [("amenity", "restaurant"), ("amenity", "food_court"), ("amenity", "fast_food")],
    "cafe": [("amenity", "cafe"), ("amenity", "coffee_shop"), ("cuisine", "coffee")],
    "hotel": [("tourism", "hotel"), ("tourism", "motel"), ("tourism", "guest_house"), ("tourism", "hostel")],
    "salon": [("shop", "beauty"), ("shop", "hairdresser"), ("amenity", "beauty"), ("shop", "cosmetics")],
    "beauty": [("shop", "beauty"), ("shop", "hairdresser"), ("amenity", "beauty"), ("shop", "cosmetics"), ("shop", "nail")],
    "spa": [("amenity", "spa"), ("leisure", "spa"), ("shop", "beauty"), ("shop", "massage")],
    "gym": [("leisure", "fitness_centre"), ("leisure", "sports_centre"), ("sport", "fitness"), ("sport", "gym"), ("amenity", "gym"), ("shop", "sports")],
    "fitness": [("leisure", "fitness_centre"), ("leisure", "sports_centre"), ("sport", "fitness"), ("sport", "gym")],
    "clinic": [("amenity", "clinic"), ("amenity", "doctors"), ("healthcare", "clinic"), ("healthcare", "doctor")],
    "hospital": [("amenity", "hospital"), ("healthcare", "hospital"), ("amenity", "clinic")],
    "dentist": [("amenity", "dentist"), ("healthcare", "dentist")],
    "dental": [("amenity", "dentist"), ("healthcare", "dentist")],
    "pharmacy": [("amenity", "pharmacy"), ("shop", "chemist"), ("healthcare", "pharmacy")],
    "school": [("amenity", "school"), ("amenity", "college"), ("amenity", "training"), ("office", "educational_institution")],
    "coaching": [("amenity", "school"), ("amenity", "training"), ("amenity", "college"), ("office", "educational_institution")],
    "university": [("amenity", "university"), ("amenity", "college")],
    "real_estate": [("office", "estate_agent"), ("office", "property"), ("shop", "estate_agent")],
    "car": [("shop", "car"), ("shop", "car_repair"), ("shop", "car_parts"), ("amenity", "car_rental"), ("shop", "motorcycle")],
    "auto": [("shop", "car"), ("shop", "car_repair"), ("shop", "car_parts"), ("amenity", "car_rental")],
    "clothes": [("shop", "clothes"), ("shop", "fashion"), ("shop", "boutique"), ("shop", "shoes")],
    "boutique": [("shop", "clothes"), ("shop", "fashion"), ("shop", "boutique")],
    "pet": [("shop", "pet"), ("shop", "pet_grooming"), ("amenity", "veterinary")],
    "photography": [("shop", "photo"), ("craft", "photographer"), ("office", "photographer")],
    "supermarket": [("shop", "supermarket"), ("shop", "convenience"), ("shop", "general"), ("shop", "grocery")],
    "grocery": [("shop", "supermarket"), ("shop", "convenience"), ("shop", "grocery"), ("shop", "greengrocer")],
    "bank": [("amenity", "bank")],
    "atm": [("amenity", "atm")],
    "bakery": [("shop", "bakery"), ("amenity", "bakery")],
    "electronics": [("shop", "electronics"), ("shop", "computer"), ("shop", "mobile_phone")],
    "mobile": [("shop", "mobile_phone"), ("shop", "electronics")],
    "jewelry": [("shop", "jewelry"), ("shop", "jewellery")],
    "jewellery": [("shop", "jewelry"), ("shop", "jewellery")],
    "furniture": [("shop", "furniture"), ("shop", "interior_decoration")],
    "hardware": [("shop", "hardware"), ("shop", "doityourself")],
    "laundry": [("shop", "laundry"), ("shop", "dry_cleaning"), ("amenity", "laundry")],
    "bar": [("amenity", "bar"), ("amenity", "pub"), ("amenity", "nightclub")],
    "pub": [("amenity", "pub"), ("amenity", "bar")],
    "wedding": [("shop", "wedding"), ("amenity", "wedding"), ("office", "wedding_planner")],
    "travel": [("shop", "travel_agency"), ("office", "travel_agent")],
    "insurance": [("office", "insurance")],
    "lawyer": [("office", "lawyer"), ("office", "legal")],
    "accountant": [("office", "accountant"), ("office", "tax_advisor")],
    "architect": [("office", "architect")],
    "yoga": [("leisure", "fitness_centre"), ("sport", "yoga"), ("amenity", "yoga")],
    "swimming": [("leisure", "swimming_pool"), ("sport", "swimming")],
    "sports": [("leisure", "sports_centre"), ("shop", "sports"), ("leisure", "pitch")],
    "temple": [("amenity", "place_of_worship")],
    "church": [("amenity", "place_of_worship")],
    "mosque": [("amenity", "place_of_worship")],
    "hospital": [("amenity", "hospital"), ("healthcare", "hospital")],
    "optical": [("shop", "optician"), ("healthcare", "optometrist")],
    "florist": [("shop", "florist")],
    "stationery": [("shop", "stationery")],
    "books": [("shop", "books")],
    "toys": [("shop", "toys")],
    "tattoo": [("shop", "tattoo")],
}

# Name keywords to search in business names (broader catch)
NICHE_NAME_KEYWORDS = {
    "gym": ["gym", "fitness", "workout", "crossfit", "bodybuilding", "muscle"],
    "fitness": ["fitness", "gym", "workout", "crossfit", "yoga"],
    "salon": ["salon", "beauty", "parlour", "parlor", "hair", "makeover", "unisex"],
    "beauty": ["beauty", "salon", "parlour", "parlor", "makeover", "cosmetic"],
    "spa": ["spa", "massage", "wellness", "ayurved"],
    "restaurant": ["restaurant", "dhaba", "kitchen", "bistro", "diner", "eatery", "bhojanalaya"],
    "cafe": ["cafe", "coffee", "chai", "tea", "bakery", "patisserie"],
    "hotel": ["hotel", "resort", "lodge", "inn", "guest house", "homestay", "oyo"],
    "clinic": ["clinic", "hospital", "medical", "health", "diagnostic", "path lab", "polyclinic"],
    "dentist": ["dental", "dentist", "tooth", "orthodon"],
    "dental": ["dental", "dentist", "tooth", "orthodon"],
    "coaching": ["coaching", "academy", "institute", "classes", "tuition", "tutorial", "education"],
    "school": ["school", "academy", "institute", "vidyalaya", "public school"],
    "real_estate": ["real estate", "property", "realty", "builders", "developers", "housing"],
    "car": ["car", "auto", "motors", "automobile", "vehicle", "garage", "service center"],
    "photography": ["photo", "studio", "photography", "photographer", "camera"],
    "boutique": ["boutique", "fashion", "designer", "clothing", "garment"],
    "clothes": ["clothing", "fashion", "garment", "apparel", "wear", "boutique"],
    "pet": ["pet", "veterinary", "vet", "animal", "dog", "cat"],
    "wedding": ["wedding", "shaadi", "marriage", "event", "banquet"],
    "yoga": ["yoga", "meditation", "pilates", "wellness"],
    "bakery": ["bakery", "bake", "cake", "pastry", "confectioner"],
    "jewelry": ["jewel", "gold", "diamond", "ornament"],
    "jewellery": ["jewel", "gold", "diamond", "ornament"],
    "electronics": ["electronics", "computer", "laptop", "mobile", "gadget"],
    "mobile": ["mobile", "phone", "smartphone", "telecom"],
    "pharmacy": ["pharmacy", "chemist", "medical", "drug"],
    "supermarket": ["supermarket", "grocery", "mart", "kirana", "general store", "departmental"],
    "grocery": ["grocery", "kirana", "supermarket", "mart", "general store"],
    "travel": ["travel", "tour", "tourism", "holidays"],
    "bar": ["bar", "lounge", "pub", "brewery", "nightclub"],
    "laundry": ["laundry", "dry clean", "wash", "ironing"],
    "furniture": ["furniture", "sofa", "interior", "decor"],
    "hardware": ["hardware", "tools", "plumb", "electric"],
    "optical": ["optical", "optician", "eye", "lens", "spectacle"],
}


@dataclass
class Business:
    name: Optional[str] = None
    category: Optional[str] = None
    brand: Optional[str] = None
    contact: Optional[Dict[str, str]] = None
    opening_hours: Optional[str] = None
    address: Optional[Dict[str, str]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    source_id: Optional[str] = None
    rating: Optional[str] = None

    def get_hash_id(self) -> str:
        key_parts = [
            str(self.name).lower().strip() if self.name else "",
            str(self.category).lower().strip() if self.category else "",
        ]
        if self.address:
            key_parts.append(str(self.address.get('street', '')).lower().strip())
            key_parts.append(str(self.address.get('city', '')).lower().strip())
        if self.latitude and self.longitude:
            key_parts.append(f"{round(self.latitude, 4)},{round(self.longitude, 4)}")
        return hashlib.md5("|".join(key_parts).encode()).hexdigest()

    def is_valid(self) -> bool:
        if not self.name and not self.brand:
            return False
        generic = {'unknown', 'none', 'null', '', 'na', 'n/a', 'yes', 'no'}
        name_lower = (self.name or '').lower().strip()
        if name_lower in generic or len(name_lower) <= 2:
            return False
        return True


def normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    cleaned = ''.join(c for c in str(phone) if c.isdigit() or c == '+')
    if cleaned.startswith('00'):
        cleaned = '+' + cleaned[2:]
    if cleaned and len(cleaned) == 10 and not cleaned.startswith('+'):
        cleaned = '+91' + cleaned
    return cleaned if cleaned and len(cleaned) >= 10 else None


def normalize_website(website: Optional[str]) -> Optional[str]:
    if not website:
        return None
    website = str(website).strip()
    if len(website) < 4 or ' ' in website:
        return None
    if not website.startswith(('http://', 'https://')):
        website = 'https://' + website
    result = urlparse(website)
    if all([result.scheme, result.netloc]):
        return website
    return None


def clean_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    name = ' '.join(str(name).strip().split())
    if not name:
        return None
    name = re.sub(r'\s*\([^)]*\)\s*$', '', name)
    name = re.sub(r'\s*\[[^\]]*\]\s*$', '', name)
    return name.strip() or None


def deduplicate(businesses: List[Business]) -> List[Business]:
    seen_hashes: Set[str] = set()
    seen_names: Set[str] = set()
    unique: List[Business] = []
    for b in businesses:
        if not b.is_valid():
            continue
        h = b.get_hash_id()
        if h in seen_hashes:
            continue
        name_key = (b.name or '').lower().strip()
        addr_key = ''
        if b.address:
            addr_key = f"{b.address.get('street','')}|{b.address.get('city','')}".lower()
        combo = f"{name_key}|{addr_key}"
        if combo in seen_names and addr_key:
            continue
        seen_hashes.add(h)
        seen_names.add(combo)
        unique.append(b)
    return unique


def make_request(query: str, retry: int = 0) -> Dict:
    try:
        resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=REQUEST_TIMEOUT,
                             headers={'User-Agent': 'CreativeMonkLeadEngine/2.0'})
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        if retry < MAX_RETRIES:
            time.sleep(2 ** retry)
            return make_request(query, retry + 1)
        return {"elements": []}
    except requests.exceptions.HTTPError:
        if retry < MAX_RETRIES:
            time.sleep(10 * (retry + 1))
            return make_request(query, retry + 1)
        return {"elements": []}
    except Exception:
        return {"elements": []}


def _build_tag_query(tags: list, radius_m: int, lat: float, lon: float) -> str:
    """Build Overpass query lines for a list of (key, value) tag pairs."""
    lines = []
    for key, val in tags:
        for elem in ["node", "way", "relation"]:
            lines.append(f'{elem}["{key}"="{val}"](around:{radius_m},{lat},{lon});')
    return "\n      ".join(lines)


def _build_name_query(keywords: list, radius_m: int, lat: float, lon: float) -> str:
    """Build Overpass query that searches for keywords in business names."""
    # Build regex: keyword1|keyword2|keyword3
    regex = "|".join(re.escape(k) for k in keywords)
    lines = []
    for elem in ["node", "way", "relation"]:
        lines.append(f'{elem}["name"~"{regex}", i](around:{radius_m},{lat},{lon});')
    return "\n      ".join(lines)


def _parse_element(element: dict, niche: str) -> Optional[Business]:
    """Parse a single OSM element into a Business object."""
    tags = element.get("tags", {})

    if not tags.get("name") and not tags.get("brand") and not tags.get("operator"):
        has_info = any(tags.get(k) for k in ["contact:phone", "phone", "contact:website", "website", "addr:street"])
        if not has_info:
            return None

    raw_name = tags.get("name") or tags.get("brand") or tags.get("operator")
    name = clean_name(raw_name)

    category = (tags.get("amenity") or tags.get("shop") or tags.get("leisure")
                or tags.get("tourism") or tags.get("healthcare") or tags.get("office")
                or tags.get("craft") or tags.get("sport") or niche)

    phone = normalize_phone(tags.get("contact:phone") or tags.get("phone"))
    website = normalize_website(tags.get("contact:website") or tags.get("website"))
    email = tags.get("contact:email") or tags.get("email")
    facebook = tags.get("contact:facebook")
    instagram = tags.get("contact:instagram")

    contact = {}
    if phone: contact["phone"] = phone
    if website: contact["website"] = website
    if email: contact["email"] = email
    if facebook: contact["facebook"] = facebook
    if instagram: contact["instagram"] = instagram

    address = {}
    if tags.get("addr:street"): address["street"] = tags["addr:street"]
    city = tags.get("addr:city") or tags.get("addr:suburb") or tags.get("addr:town")
    if city: address["city"] = city
    if tags.get("addr:postcode"): address["postcode"] = tags["addr:postcode"]
    if tags.get("addr:full"): address["full"] = tags["addr:full"]

    lat_c = element.get("lat") or element.get("center", {}).get("lat")
    lon_c = element.get("lon") or element.get("center", {}).get("lon")

    return Business(
        name=name,
        category=category,
        brand=clean_name(tags.get("brand")),
        contact=contact if contact else None,
        opening_hours=tags.get("opening_hours"),
        address=address if address else None,
        latitude=float(lat_c) if lat_c else None,
        longitude=float(lon_c) if lon_c else None,
        source_id=f"{element['type']}_{element.get('id', '')}",
    )


def get_businesses(lat: float, lon: float, radius_km: float, niche: str) -> List[Dict]:
    """Fetch businesses with tag-based + name-based search and deduplication."""
    lat, lon, radius_km = float(lat), float(lon), float(radius_km)
    if radius_km <= 0 or radius_km > 50:
        raise ValueError("Radius must be between 0.1 and 50 km")

    niche_clean = str(niche).strip().lower()
    radius_m = int(radius_km * 1000)

    raw: List[Business] = []

    # Strategy 1: Tag-based search (fast, specific)
    tags = NICHE_TAGS.get(niche_clean, [])
    if not tags:
        tags = [
            ("amenity", niche_clean), ("shop", niche_clean),
            ("leisure", niche_clean), ("tourism", niche_clean),
            ("healthcare", niche_clean), ("office", niche_clean),
            ("craft", niche_clean), ("sport", niche_clean),
        ]

    tag_query_lines = _build_tag_query(tags, radius_m, lat, lon)
    tag_query = f"""[out:json][timeout:60];({tag_query_lines});out center tags;"""
    data = make_request(tag_query)
    for elem in data.get("elements", []):
        b = _parse_element(elem, niche_clean)
        if b:
            raw.append(b)

    # Strategy 2: Name-based search (broader, slower)
    # Use smaller radius for name search to keep it fast, or skip if tag search found plenty
    name_keywords = NICHE_NAME_KEYWORDS.get(niche_clean, [niche_clean])
    name_radius = min(radius_m, 20000)  # Cap name search at 20km to avoid Overpass timeout
    if len(raw) < 50:  # Only do name search if tag search found fewer than 50
        name_query_lines = _build_name_query(name_keywords, name_radius, lat, lon)
        name_query = f"""[out:json][timeout:45];({name_query_lines});out center tags;"""
        data2 = make_request(name_query)
        for elem in data2.get("elements", []):
            b = _parse_element(elem, niche_clean)
            if b:
                raw.append(b)

    unique = deduplicate(raw)
    unique.sort(key=lambda x: (
        0 if not (x.contact or {}).get('website') else 1,
        0 if not (x.contact or {}).get('phone') else 1,
        (x.name or '').lower()
    ))

    results = []
    for b in unique:
        d = {
            "name": b.name,
            "category": b.category,
            "brand": b.brand,
            "contact": b.contact,
            "opening_hours": b.opening_hours,
            "address": b.address,
            "latitude": b.latitude,
            "longitude": b.longitude,
            "source_id": b.source_id,
        }
        results.append({k: v for k, v in d.items() if v is not None})

    return results
