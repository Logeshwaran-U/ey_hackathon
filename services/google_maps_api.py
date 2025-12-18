import requests
import time
import urllib.parse
import json
from typing import Optional, Dict, Any
from difflib import SequenceMatcher

try:
    from config.settings import GOOGLE_MAPS_API_KEY
except Exception:
    GOOGLE_MAPS_API_KEY = None


class GoogleMapsService:
    GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
    PLACE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

    def __init__(self, api_key: Optional[str] = None, timeout: int = 6):
        if not api_key and not GOOGLE_MAPS_API_KEY:
            raise ValueError("Google Maps API key missing. Set GOOGLE_MAPS_API_KEY in config/settings.py")
        self.api_key = api_key or GOOGLE_MAPS_API_KEY
        self.timeout = timeout

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

    @staticmethod
    def _fuzzy_match(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

    def geocode_address(self, address: str) -> Dict[str, Any]:
        if not address:
            return {"match": False}

        params = {"address": address, "key": self.api_key}
        data = self._get(self.GEOCODE_URL, params)

        if not data.get("results"):
            return {"match": False}

        r = data["results"][0]
        return {
            "match": True,
            "formatted_address": r.get("formatted_address"),
            "lat": r["geometry"]["location"].get("lat"),
            "lng": r["geometry"]["location"].get("lng"),
            "partial_match": r.get("partial_match", False),
            "proof_link": f"https://www.google.com/maps/search/{urllib.parse.quote(r.get('formatted_address',''))}"
        }

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

        loc = result.get("geometry", {}).get("location", {})

        return {
            "details_found": True,
            "formatted_address": result.get("formatted_address"),
            "google_phone": result.get("formatted_phone_number")
                            or result.get("international_phone_number"),
            "website": result.get("website"),
            "lat": loc.get("lat"),
            "lng": loc.get("lng"),
            "proof_link": f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        }

    def enrich_provider_location(self, name: str, address: str) -> Dict[str, Any]:
        search_query = f"{name} {address}".strip()
        place = self.find_clinic(search_query)

        result = {
            "google_place_found": place.get("found", False),
            "google_place_name": place.get("name"),
            "google_place_address": place.get("address"),
            "google_place_proof": place.get("proof_link"),
            "match_score": 0.0
        }

        if not place.get("found"):
            geo = self.geocode_address(address)
            if geo.get("match"):
                geo["match_score"] = 0.35
                result.update(geo)
            return result

        details = self.get_place_details(place.get("place_id"))
        result.update(details)

        score = 0.4
        name_match = self._fuzzy_match(name, place.get("name", ""))
        addr_match = self._fuzzy_match(address, details.get("formatted_address", ""))

        score += name_match * 0.2
        score += addr_match * 0.25

        if details.get("website"):
            score += 0.15

        result["name_fuzzy_match"] = round(name_match, 3)
        result["address_fuzzy_match"] = round(addr_match, 3)
        result["match_score"] = min(score, 1.0)

        return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m services.google_maps_api \"Ruban Hospital\"")
        sys.exit(1)

    query = sys.argv[1]
    svc = GoogleMapsService()

    out = svc.find_clinic(query)
    print(json.dumps(out, indent=2, ensure_ascii=False))
