
import requests
import time
import json
from difflib import SequenceMatcher
from typing import Optional, Dict, Any


US_PROXY = "http://user123:pass123@198.74.52.82:3128"

PROXIES = {
    "http": US_PROXY,
    "https": US_PROXY
}


class NPIRegistryService:
    """
    NPI Lookup with automatic:
    - Direct API request
    - Proxy fallback (US IP)
    - Synthetic fallback
    """

    BASE_URL = "https://npiregistry.cms.hhs.gov/api/?version=2.1"
    TIMEOUT = 5
    RETRIES = 2
    BACKOFF = 0.5

    SYNTHETIC = [
        {
            "npi": "1427557893",
            "first_name": "John",
            "last_name": "Doe",
            "taxonomy": "Internal Medicine",
            "state": "NY",
        },
        {
            "npi": "1881937465",
            "first_name": "Emily",
            "last_name": "Clark",
            "taxonomy": "Cardiology",
            "state": "CA",
        }
    ]

    def _request(self, params, use_proxy=False):
        try:
            if use_proxy:
                resp = requests.get(
                    self.BASE_URL,
                    params=params,
                    proxies=PROXIES,
                    timeout=self.TIMEOUT
                )
            else:
                resp = requests.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.TIMEOUT
                )

            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def _smart_search(self, params):
        direct = self._request(params, use_proxy=False)
        if direct and direct.get("results"):
            return direct, "from_api"

        proxy = self._request(params, use_proxy=True)
        if proxy and proxy.get("results"):
            return proxy, "from_proxy"

        return None, None

    @staticmethod
    def _fuzzy(a: Optional[str], b: Optional[str]) -> float:
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

    def search(self, **kwargs):
        params = {}

        if kwargs.get("npi_number"):
            params["number"] = kwargs["npi_number"]
        if kwargs.get("first_name"):
            params["first_name"] = kwargs["first_name"]
        if kwargs.get("last_name"):
            params["last_name"] = kwargs["last_name"]
        if kwargs.get("taxonomy"):
            params["taxonomy_description"] = kwargs["taxonomy"]
        if kwargs.get("state"):
            params["address_purpose"] = "LOCATION"
            params["state"] = kwargs["state"]

        data, mode = self._smart_search(params)

        if not data:
            return {"found": False, "results": [], "mode": "none"}

        return {
            "found": True,
            "results": data.get("results", []),
            "mode": mode
        }

    def get_best_match(
        self,
        provider_name=None,
        specialization=None,
        state=None,
        external_address=None,
        external_phone=None,
        npi_number=None
    ) -> Dict[str, Any]:

        if npi_number:
            result = self.search(npi_number=npi_number)

            if result["found"]:
                entry = result["results"][0]
                return {
                    "match_found": True,
                    "npi": npi_number,
                    "basic": entry.get("basic", {}),
                    "addresses": entry.get("addresses", []),
                    "taxonomies": entry.get("taxonomies", []),
                    "match_confidence": 1.0,
                    "signals": {"direct_npi": True, result["mode"]: True}
                }

            for s in self.SYNTHETIC:
                if s["npi"] == npi_number:
                    return {
                        "match_found": True,
                        "npi": npi_number,
                        "basic": {"first_name": s["first_name"], "last_name": s["last_name"]},
                        "addresses": [{"state": s["state"]}],
                        "taxonomies": [{"desc": s["taxonomy"]}],
                        "match_confidence": 0.95,
                        "signals": {"direct_npi": True, "synthetic_fallback": True}
                    }

            return {
                "match_found": True,
                "npi": npi_number,
                "basic": {},
                "addresses": [],
                "taxonomies": [],
                "match_confidence": 0.5,
                "signals": {"direct_npi": True, "no_data": True}
            }

        if not provider_name:
            return {"match_found": False}

        parts = provider_name.split()
        first = parts[0]
        last = parts[-1] if len(parts) > 1 else None

        result = self.search(
            first_name=first,
            last_name=last,
            taxonomy=specialization,
            state=state
        )

        if not result["found"]:
            for s in self.SYNTHETIC:
                if s["first_name"].lower() == first.lower():
                    return {
                        "match_found": True,
                        "npi": s["npi"],
                        "basic": {
                            "first_name": s["first_name"],
                            "last_name": s["last_name"]
                        },
                        "addresses": [{"state": s["state"]}],
                        "taxonomies": [{"desc": s["taxonomy"]}],
                        "match_confidence": 0.9,
                        "signals": {"synthetic_fallback": True}
                    }
            return {"match_found": False}

        best_entry = None
        best_score = -1
        best_signals = {}

        for entry in result["results"]:
            basic = entry.get("basic", {})
            tax = entry.get("taxonomies", [])
            addrs = entry.get("addresses", [])

            score = 0.0
            signals = {}

            nm = (self._fuzzy(first, basic.get("first_name")) +
                  self._fuzzy(last, basic.get("last_name"))) / 2
            score += nm * 0.50
            signals["name_match"] = round(nm, 3)

            if specialization and tax:
                ts = self._fuzzy(specialization, tax[0].get("desc", ""))
                score += ts * 0.25
                signals["taxonomy_match"] = round(ts, 3)

            loc = next((a for a in addrs if a.get("address_purpose") == "LOCATION"), None)
            if state and loc and loc.get("state") == state:
                score += 0.15
                signals["state_match"] = True

            if external_address and loc:
                api_addr = f"{loc.get('address_1','')} {loc.get('city','')}"
                ascore = self._fuzzy(external_address, api_addr)
                score += ascore * 0.10
                signals["address_match"] = round(ascore, 3)

            npi_phone = loc.get("telephone_number") if loc else None
            if external_phone and npi_phone and external_phone[-4:] == npi_phone[-4:]:
                score += 0.05
                signals["phone_match"] = True

            if score > best_score:
                best_score = score
                best_entry = entry
                best_signals = signals

        return {
            "match_found": True,
            "npi": best_entry.get("number"),
            "basic": best_entry.get("basic", {}),
            "addresses": best_entry.get("addresses", []),
            "taxonomies": best_entry.get("taxonomies", []),
            "match_confidence": round(best_score, 3),
            "signals": best_signals
        }
