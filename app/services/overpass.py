import requests
import hashlib
import time
import re
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from urllib.parse import urlparse

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3


@dataclass
class Business:
    """Data class for business information"""
    name: Optional[str] = None
    category: Optional[str] = None
    brand: Optional[str] = None
    contact: Optional[Dict[str, str]] = None
    opening_hours: Optional[str] = None
    address: Optional[Dict[str, str]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    source_id: Optional[str] = None
    
    def get_hash_id(self) -> str:
        """Generate a unique hash for the business based on key attributes"""
        # Create a string from key identifiers
        key_parts = [
            str(self.name).lower().strip() if self.name else "",
            str(self.category).lower().strip() if self.category else "",
            str(self.brand).lower().strip() if self.brand else "",
        ]
        
        # Add address components if available
        if self.address:
            street = self.address.get('street', '')
            city = self.address.get('city', '')
            postcode = self.address.get('postcode', '')
            
            key_parts.extend([
                str(street).lower().strip() if street else "",
                str(city).lower().strip() if city else "",
                str(postcode).lower().strip() if postcode else ""
            ])
        
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def is_valid(self) -> bool:
        """Check if business has minimal required data"""
        # At minimum, we need a name or brand
        if not self.name and not self.brand:
            return False
        
        # Check if it's just a generic/placeholder name
        generic_names = {
            'unknown', 'none', 'null', '', 'na', 'n/a', '未命名', '無名',
            'restaurant', 'cafe', 'shop', 'store', 'business', 'company'
        }
        
        name_lower = (self.name or '').lower().strip()
        if name_lower in generic_names or len(name_lower) <= 2:
            return False
            
        return True


def normalize_phone(phone: Optional[str]) -> Optional[str]:
    """Normalize phone number format"""
    if not phone:
        return None
    
    try:
        # Remove all non-digit characters except +
        cleaned = ''.join(c for c in str(phone) if c.isdigit() or c == '+')
        
        # If it starts with 00, replace with +
        if cleaned.startswith('00'):
            cleaned = '+' + cleaned[2:]
        
        # Add country code if missing (assuming India +91 for example)
        # You might want to make this configurable based on location
        if cleaned and len(cleaned) == 10:
            cleaned = '+91' + cleaned
        
        return cleaned if cleaned else None
    except Exception:
        return None


def normalize_website(website: Optional[str]) -> Optional[str]:
    """Normalize website URL"""
    if not website:
        return None
    
    try:
        website = str(website).strip()
        
        # Skip if it's clearly not a URL
        if len(website) < 4 or ' ' in website:
            return None
        
        # Add http:// if no protocol specified
        if not website.startswith(('http://', 'https://')):
            website = 'https://' + website
        
        # Validate URL
        result = urlparse(website)
        if all([result.scheme, result.netloc]):
            return website.lower()
    except Exception:
        pass
    
    return None


def clean_business_name(name: Optional[str]) -> Optional[str]:
    """Clean business name from common issues"""
    if not name:
        return None
    
    try:
        # Remove extra whitespace
        name = ' '.join(str(name).strip().split())
        
        if not name:
            return None
        
        # Remove common suffixes in parentheses/brackets
        name = re.sub(r'\s*\([^)]*\)\s*$', '', name)  # Remove trailing (text)
        name = re.sub(r'\s*\[[^\]]*\]\s*$', '', name)  # Remove trailing [text]
        
        # Remove quotation marks
        name = name.replace('"', '').replace("'", "").strip()
        
        return name if name else None
    except Exception:
        return None


def get_address_string(business: Business) -> str:
    """Get normalized address string for deduplication"""
    if not business.address:
        return ""
    
    street = business.address.get('street', '')
    city = business.address.get('city', '')
    
    # Safely convert to string and normalize
    street_str = str(street).lower().strip() if street else ""
    city_str = str(city).lower().strip() if city else ""
    
    return f"{street_str}|{city_str}"


def deduplicate_businesses(businesses: List[Business]) -> List[Business]:
    """Remove duplicate businesses based on multiple criteria"""
    seen_hashes: Set[str] = set()
    seen_combinations: Set[str] = set()
    unique_businesses: List[Business] = []
    
    for business in businesses:
        if not business.is_valid():
            continue
        
        # Method 1: Use hash ID
        business_hash = business.get_hash_id()
        if business_hash in seen_hashes:
            continue
        
        # Method 2: Check name + address combination
        name_key = (business.name or '').lower().strip()
        address_key = get_address_string(business)
        
        combination_key = f"{name_key}|{address_key}"
        if combination_key in seen_combinations and address_key:
            continue
        
        # Method 3: Check for very similar names (fuzzy match)
        is_duplicate = False
        for existing in unique_businesses:
            existing_name = (existing.name or '').lower()
            current_name = (business.name or '').lower()
            
            if existing_name and current_name:
                # Skip if names are exactly the same
                if existing_name == current_name:
                    # Keep the one with more complete data
                    existing_data_score = sum(1 for v in [
                        existing.name, existing.contact, existing.address
                    ] if v)
                    current_data_score = sum(1 for v in [
                        business.name, business.contact, business.address
                    ] if v)
                    
                    if current_data_score <= existing_data_score:
                        is_duplicate = True
                        break
        
        if is_duplicate:
            continue
        
        seen_hashes.add(business_hash)
        seen_combinations.add(combination_key)
        unique_businesses.append(business)
    
    return unique_businesses


def make_overpass_request(query: str, retry_count: int = 0) -> Dict:
    """Make request to Overpass API with retry logic"""
    try:
        response = requests.post(
            OVERPASS_URL,
            data=query,
            timeout=REQUEST_TIMEOUT,
            headers={
                'User-Agent': 'BusinessFinder/1.0',
                'Accept': 'application/json'
            }
        )
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.Timeout:
        if retry_count < MAX_RETRIES:
            time.sleep(2 ** retry_count)  # Exponential backoff
            return make_overpass_request(query, retry_count + 1)
        raise Exception("Overpass API timeout after multiple retries")
        
    except requests.exceptions.HTTPError as e:
        if retry_count < MAX_RETRIES:
            wait_time = 30 * (retry_count + 1)
            print(f"HTTP error {e.response.status_code}. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            return make_overpass_request(query, retry_count + 1)
        raise Exception(f"Overpass API error: {e}")
        
    except requests.exceptions.RequestException as e:
        if retry_count < MAX_RETRIES:
            time.sleep(2)
            return make_overpass_request(query, retry_count + 1)
        raise Exception(f"Failed to connect to Overpass API: {e}")


def get_businesses(lat: float, lon: float, radius_km: float, niche: str) -> List[Dict]:
    """
    Fetch businesses from OpenStreetMap with deduplication and data cleaning
    
    Args:
        lat: Latitude of search center
        lon: Longitude of search center
        radius_km: Search radius in kilometers
        niche: Business type/category
    
    Returns:
        List of deduplicated and cleaned business dictionaries
    """
    # Input validation
    try:
        lat = float(lat)
        lon = float(lon)
        radius_km = float(radius_km)
    except (ValueError, TypeError):
        raise ValueError("Invalid coordinates or radius")
    
    if not niche or not str(niche).strip():
        raise ValueError("Niche parameter is required")
    
    if radius_km <= 0 or radius_km > 50:
        raise ValueError("Radius must be between 0.1 and 50 km")
    
    # Clean and prepare niche parameter
    niche = str(niche).strip().lower()
    
    # Map common business types to OSM amenities
    niche_mapping = {
        'restaurant': 'restaurant',
        'cafe': 'cafe',
        'coffee': 'cafe',
        'coffee shop': 'cafe',
        'hotel': 'hotel',
        'motel': 'motel',
        'pharmacy': 'pharmacy',
        'drugstore': 'pharmacy',
        'hospital': 'hospital',
        'clinic': 'clinic',
        'bank': 'bank',
        'atm': 'atm',
        'supermarket': 'supermarket',
        'grocery': 'supermarket',
        'mall': 'mall',
        'shopping': 'shop',
        'store': 'shop',
        'gas': 'fuel',
        'gas station': 'fuel',
        'petrol': 'fuel',
        'petrol pump': 'fuel',
        'school': 'school',
        'university': 'university',
        'college': 'college'
    }
    
    # Use mapping if available, otherwise use the input as-is
    amenity_filter = niche_mapping.get(niche, niche)
    
    radius_meters = int(radius_km * 1000)
    
    # More comprehensive query that includes alternative tags
    query = f"""
    [out:json][timeout:90];
    (
      // Primary search by amenity tag
      node["amenity"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
      way["amenity"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
      relation["amenity"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
      
      // Also search by shop tag for retail businesses
      node["shop"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
      way["shop"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
      relation["shop"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
      
      // Search by tourism tag for hotels, etc.
      node["tourism"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
      way["tourism"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
      relation["tourism"~"{amenity_filter}", i](around:{radius_meters},{lat},{lon});
    );
    out center tags;
    """
    
    try:
        data = make_overpass_request(query)
    except Exception as e:
        print(f"Error fetching from Overpass API: {e}")
        return []
    
    raw_businesses: List[Business] = []
    
    for element in data.get("elements", []):
        tags = element.get("tags", {})
        
        # Skip elements without a name or brand (unless they have other valuable info)
        if not tags.get("name") and not tags.get("brand") and not tags.get("operator"):
            # Check if it has at least contact info or address
            has_contact = any(tags.get(key) for key in [
                "contact:phone", "phone", "contact:website", "website",
                "addr:street", "addr:city"
            ])
            if not has_contact:
                continue
        
        # Clean business name
        raw_name = tags.get("name") or tags.get("brand") or tags.get("operator")
        clean_name = clean_business_name(raw_name)
        
        # Determine category
        category = tags.get("amenity") or tags.get("shop") or tags.get("tourism") or niche
        
        # Normalize contact information
        phone = normalize_phone(
            tags.get("contact:phone") or tags.get("phone")
        )
        website = normalize_website(
            tags.get("contact:website") or tags.get("website")
        )
        
        # Prepare contact dictionary only if there's data
        contact_data = {}
        if phone:
            contact_data["phone"] = phone
        if website:
            contact_data["website"] = website
        
        email = tags.get("contact:email") or tags.get("email")
        if email:
            contact_data["email"] = email
            
        facebook = tags.get("contact:facebook")
        if facebook:
            contact_data["facebook"] = facebook
            
        instagram = tags.get("contact:instagram")
        if instagram:
            contact_data["instagram"] = instagram
        
        contact = contact_data if contact_data else None
        
        # Prepare address dictionary only if there's data
        address_data = {}
        street = tags.get("addr:street")
        if street:
            address_data["street"] = street
            
        city = tags.get("addr:city") or tags.get("addr:suburb") or tags.get("addr:town")
        if city:
            address_data["city"] = city
            
        postcode = tags.get("addr:postcode")
        if postcode:
            address_data["postcode"] = postcode
            
        full_address = tags.get("addr:full")
        if full_address:
            address_data["full"] = full_address
        
        address = address_data if address_data else None
        
        # Get coordinates
        lat_coord = element.get("lat") or element.get("center", {}).get("lat")
        lon_coord = element.get("lon") or element.get("center", {}).get("lon")
        
        # Create Business object
        business = Business(
            name=clean_name,
            category=category,
            brand=clean_business_name(tags.get("brand")),
            contact=contact,
            opening_hours=tags.get("opening_hours"),
            address=address,
            latitude=float(lat_coord) if lat_coord is not None else None,
            longitude=float(lon_coord) if lon_coord is not None else None,
            source_id=f"{element['type']}_{element.get('id', 'unknown')}"
        )
        
        # Only add if it has at least a name
        if business.name or business.brand:
            raw_businesses.append(business)
    
    # Deduplicate businesses
    unique_businesses = deduplicate_businesses(raw_businesses)
    
    # Sort by name for consistent results
    unique_businesses.sort(key=lambda x: (x.name or '').lower())
    
    # Convert to dictionary format for API response
    results = []
    for business in unique_businesses:
        result_dict = {
            "name": business.name,
            "category": business.category,
            "brand": business.brand,
            "contact": business.contact,
            "opening_hours": business.opening_hours,
            "address": business.address,
            "latitude": business.latitude,
            "longitude": business.longitude,
            "source_id": business.source_id
        }
        
        # Remove None values from the dictionary
        result_dict = {k: v for k, v in result_dict.items() if v is not None}
        
        results.append(result_dict)
    
    print(f"Found {len(raw_businesses)} raw businesses, {len(unique_businesses)} after deduplication")
    return results


# Example usage function (for testing)
if __name__ == "__main__":
    try:
        # Test with Mumbai coordinates
        businesses = get_businesses(
            lat=19.0760,
            lon=72.8777,
            radius_km=2,
            niche="restaurant"
        )
        
        print(f"\nFound {len(businesses)} unique businesses:")
        for i, biz in enumerate(businesses[:10], 1):  # Show first 10
            print(f"\n{i}. {biz.get('name', 'Unknown')} - {biz.get('category', 'Unknown')}")
            if biz.get('contact', {}).get('phone'):
                print(f"   Phone: {biz['contact']['phone']}")
            if biz.get('address', {}).get('city'):
                print(f"   Location: {biz['address'].get('city')}")
                
    except Exception as e:
        print(f"Error: {e}")