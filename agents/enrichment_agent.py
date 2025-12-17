#!/usr/bin/env python3
# agents/enrichment_agent.py
# EY Hackathon – FINAL STABLE ASYNC ENRICHMENT AGENT (Windows-safe)

from __future__ import annotations
import os
import sys
import json
import asyncio
import tempfile
from datetime import datetime, timezone
from typing import Dict, Any

# ---------------- PATH FIX ----------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---------------- SAFE IMPORTS ----------------
try:
    from services.npi_api import NPIRegistryService
    from services.google_maps_api import GoogleMapsService
    from services.website_scraper import WebsiteScraper
except Exception:
    raise RuntimeError("❌ services/* imports failed. Run from project root.")

# ---------------- CONFIG ----------------
PROCESSED_DIR = os.path.join("data", "processed")
VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
ENRICHED_JSON = os.path.join(PROCESSED_DIR, "enriched_data.json")

os.makedirs(PROCESSED_DIR, exist_ok=True)

WEIGHTS = {"npi": 0.4, "maps": 0.35, "website": 0.25}
MAX_CONCURRENCY = 6

# ---------------- SAFE IO ----------------
_file_lock = asyncio.Lock()

def load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read().strip()
            if not txt:
                return {}
            return json.loads(txt)
    except Exception:
        return {}

async def atomic_write(path: str, data: Dict[str, Any]):
    async with _file_lock:
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            if os.path.exists(path):
                os.remove(path)
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass

# ---------------- HELPERS ----------------
def now_utc():
    return datetime.now(timezone.utc).isoformat()

def valid_http_url(u: str) -> bool:
    return isinstance(u, str) and u.startswith(("http://", "https://"))

# ---------------- ENRICHER ----------------
class Enricher:
    def __init__(self):
        self.npi = NPIRegistryService()
        self.maps = GoogleMapsService()
        self.scraper = WebsiteScraper()

    async def enrich_one(self, pid: str, rec: Dict[str, Any]) -> Dict[str, Any]:
        norm = rec.get("normalized", {})

        name = norm.get("name")
        address = norm.get("address")
        phone = norm.get("phone")
        npi_number = norm.get("npi")

        enriched = {
            "provider_id": pid,
            "timestamp_utc": now_utc(),
            "base_validation": rec,
            "enriched": {},
            "signals": {},
            "confidence": 0.0,
            "status": "REVIEW"
        }

        score = 0.0

        # ---------- NPI ----------
        try:
            npi_data = await asyncio.to_thread(
                self.npi.get_best_match,
                provider_name=name,
                external_address=address,
                external_phone=phone,
                npi_number=npi_number
            )
            if npi_data and npi_data.get("match_found"):
                enriched["enriched"]["npi"] = npi_data
                score += WEIGHTS["npi"] * float(npi_data.get("match_confidence", 0))
        except Exception:
            pass

        # ---------- MAPS ----------
        try:
            maps_data = await asyncio.to_thread(
                self.maps.enrich_provider_location,
                name or "",
                address or ""
            )
            if maps_data:
                enriched["enriched"]["maps"] = maps_data
                score += WEIGHTS["maps"] * float(maps_data.get("match_score", 0))
        except Exception:
            pass

        # ---------- WEBSITE ----------
        website_url = None
        if maps_data:
            website_url = maps_data.get("website")

        if valid_http_url(website_url):
            try:
                web_data = await asyncio.to_thread(
                    self.scraper.scrape,
                    website_url,
                    pid,
                    name,
                    None,
                    False
                )
                enriched["enriched"]["website"] = web_data
                wscore = float(web_data.get("website_trust_score", 0))
                score += WEIGHTS["website"] * wscore
            except Exception:
                pass

        enriched["confidence"] = round(min(score, 1.0), 3)

        if enriched["confidence"] >= 0.85:
            enriched["status"] = "VERIFIED"
        elif enriched["confidence"] >= 0.5:
            enriched["status"] = "REVIEW"
        else:
            enriched["status"] = "PENDING"

        return enriched

# ---------------- RUNNER ----------------
async def run():
    validated = load_json(VALIDATED_JSON)
    if not validated:
        print("❌ No validated data found")
        return

    enriched_existing = load_json(ENRICHED_JSON)
    enricher = Enricher()

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    tasks = []

    async def worker(pid, rec):
        async with sem:
            out = await enricher.enrich_one(pid, rec)
            enriched_existing[pid] = out
            await atomic_write(ENRICHED_JSON, enriched_existing)
            print(f"✅ Enriched {pid} → {out['confidence']}")

    for pid, rec in validated.items():
        tasks.append(worker(pid, rec))

    await asyncio.gather(*tasks)

# ---------------- MAIN ----------------
if __name__ == "__main__":
    asyncio.run(run())
