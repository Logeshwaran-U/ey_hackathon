
import requests
import time
import urllib.parse
from typing import Optional, Dict, Any, List
from difflib import SequenceMatcher
from config.settings import GOOGLE_MAPS_API_KEY


class GoogleMapsService:
    GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
    PLACE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

    def __init__(self, api_key: Optional[str] = None, timeout: int = 6):
        self.api_key = api_key or GOOGLE_MAPS_API_KEY
        self.timeout = timeout

    
    # INTERNAL: SAFE HTTP GET WITH RETRIES + BACKOFF
    def _get(self, url: str, params: dict, retries: int = 2, backoff: float = 0.6) -> dict:
        for attempt in range(retries + 1):
            try:
                resp = requests.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                pass
            time.sleep(backoff * (attempt + 1))
        return {}

    # INTERNAL: FUZZY MATCH (Normalized)
    @staticmethod
    def _fuzzy_match(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        a = a.lower().strip()
        b = b.lower().strip()
        return SequenceMatcher(None, a, b).ratio()

    # 1. GEOCODING (Address → Lat/Lng)
    def geocode_address(self, address: str) -> Dict[str, Any]:
        if not address:
            return {"match": False}

        params = {"address": address, "key": self.api_key}
        data = self._get(self.GEOCODE_URL, params)

        if not data.get("results"):
            return {"match": False}

        r = data["results"][0]
        formatted = r.get("formatted_address", "")
        location = r["geometry"]["location"]

        return {
            "match": True,
            "formatted_address": formatted,
            "lat": location.get("lat"),
            "lng": location.get("lng"),
            "partial_match": r.get("partial_match", False),
            "proof_link": f"https://www.google.com/maps/search/{urllib.parse.quote(formatted)}"
        }

    # 2. PLACE SEARCH (Clinic Name → Place ID)
    def find_clinic(self, query: str) -> Dict[str, Any]:
        if not query:
            return {"found": False}

        params = {"query": query, "key": self.api_key}
        data = self._get(self.PLACE_SEARCH_URL, params)

        if not data.get("results"):
            return {"found": False}

        r = data["results"][0]
        place_id = r.get("place_id")

        return {
            "found": True,
            "place_id": place_id,
            "name": r.get("name"),
            "address": r.get("formatted_address"),
            "types": r.get("types", []),
            "rating": r.get("rating"),
            "proof_link": f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        }

    # 3. PLACE DETAILS (Get phone, website, formatted addr)
    def get_place_details(self, place_id: str) -> Dict[str, Any]:
        if not place_id:
            return {"details_found": False}

        params = {
            "place_id": place_id,
            "fields": "formatted_phone_number,international_phone_number,website,formatted_address,geometry,name",
            "key": self.api_key,
        }
        data = self._get(self.PLACE_DETAILS_URL, params)

        result = data.get("result")
        if not result:
            return {"details_found": False}

        location = result.get("geometry", {}).get("location", {})

        return {
            "details_found": True,
            "formatted_address": result.get("formatted_address"),
            "google_phone": result.get("formatted_phone_number")
                            or result.get("international_phone_number"),
            "website": result.get("website"),
            "lat": location.get("lat"),
            "lng": location.get("lng"),
            "proof_link": f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        }

    # 4. MASTER ENRICHMENT FUNCTION FOR A PROVIDER
    def enrich_provider_location(self, name: str, address: str) -> Dict[str, Any]:
        """
        High-level enrichment logic:
        - Place search (clinic existence)
        - Geocode fallback
        - Place details (phone, website)
        - Match scoring
        """

        # STEP 1 → COMBINE NAME + ADDRESS QUERY
        search_query = f"{name} {address}".strip()
        place = self.find_clinic(search_query)

        enrichment: Dict[str, Any] = {
            "google_place_found": place.get("found", False),
            "google_place_name": place.get("name"),
            "google_place_address": place.get("address"),
            "google_place_types": place.get("types"),
            "google_place_rating": place.get("rating"),
            "google_place_proof": place.get("proof_link"),
            "match_score": 0.0,
        }

        # STEP 2 → NO PLACE FOUND → TRY GEOCODE
        if not place.get("found"):
            geo = self.geocode_address(address)
            if geo.get("match"):
                enrichment.update({
                    "lat": geo.get("lat"),
                    "lng": geo.get("lng"),
                    "google_formatted_address": geo.get("formatted_address"),
                    "google_geocode_proof": geo.get("proof_link"),
                    "match_score": 0.35  # weak match
                })
            return enrichment

        # STEP 3 → PLACE DETAILS
        place_id = place.get("place_id")
        details = self.get_place_details(place_id)
        enrichment.update(details)

        # STEP 4 → MATCH CONFIDENCE CALCULATION
        score = 0.0

        # Clinic found on Google (major trust)
        score += 0.40

        # Fuzzy name match
        name_match = self._fuzzy_match(name, place.get("name", ""))
        score += (name_match * 0.20)

        # Fuzzy address match
        addr_match = self._fuzzy_match(address, details.get("formatted_address", ""))
        score += (addr_match * 0.25)

        # Website exists → minor boost
        if details.get("website"):
            score += 0.15

        enrichment["name_fuzzy_match"] = round(name_match, 3)
        enrichment["address_fuzzy_match"] = round(addr_match, 3)
        enrichment["match_score"] = min(score, 1.0)

        return enrichment
