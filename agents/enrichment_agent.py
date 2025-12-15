#!/usr/bin/env python3
# agents/enrichment_agent.py
"""
Enrichment agent:
- reads validated JSON (mapping provider_id -> validated_record)
- calls NPIRegistryService, GoogleMapsService, WebsiteScraper
- writes enriched JSON (mapping provider_id -> enriched_record)

Usage:
    python agents/enrichment_agent.py --validated data/processed/validated_data.json --out data/processed/enriched_data.json --workers 6 --force
"""

from __future__ import annotations
import os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# ---- Project settings import (robust fallback) ----
try:
    from config import settings as SETTINGS
except Exception:
    # fallback defaults (match your project tree)
    class _S:
        PROCESSED_DIR = os.path.join("data", "processed")
        VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
        ENRICHED_JSON = os.path.join(PROCESSED_DIR, "enriched_data.json")
        # weights for combining signals
        ENRICH_WEIGHTS = {"npi": 0.4, "maps": 0.35, "scraper": 0.25}
        # service toggles
        ENABLE_NPI = True
        ENABLE_MAPS = True
        ENABLE_SCRAPER = True
    SETTINGS = _S()

# ---- try import services using both absolute and relative patterns ----
_import_errors = []
try:
    from services.npi_api import NPIRegistryService
    from services.google_maps_api import GoogleMapsService
    from services.website_scraper import WebsiteScraper
except Exception as e:
    _import_errors.append(e)
    # try relative imports (when running from agents/ directly)
    try:
        from ..services.npi_api import NPIRegistryService  # type: ignore
        from ..services.google_maps_api import GoogleMapsService  # type: ignore
        from ..services.website_scraper import WebsiteScraper  # type: ignore
    except Exception as e2:
        _import_errors.append(e2)
        # final fallback: raise descriptive import error later

# ---- logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("enrichment_agent")

# ---- constants / paths ----
VALIDATED_JSON_PATH = getattr(SETTINGS, "VALIDATED_JSON", os.path.join("data", "processed", "validated_data.json"))
ENRICHED_JSON_PATH = getattr(SETTINGS, "ENRICHED_JSON", os.path.join("data", "processed", "enriched_data.json"))
ENRICH_WEIGHTS = getattr(SETTINGS, "ENRICH_WEIGHTS", {"npi": 0.4, "maps": 0.35, "scraper": 0.25})
ENABLE_NPI = getattr(SETTINGS, "ENABLE_NPI", True)
ENABLE_MAPS = getattr(SETTINGS, "ENABLE_MAPS", True)
ENABLE_SCRAPER = getattr(SETTINGS, "ENABLE_SCRAPER", True)

# ---- atomic write helper ----
import tempfile
def _atomic_write(path: str, data: Dict[str, Any]) -> None:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="tmp_enrich_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass

# ---- load/save helpers ----
def _load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception as e:
        logger.warning("Failed to load JSON %s: %s", path, e)
        return {}

# ---- small util ----
def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

# ---- enrichment logic per provider ----
class Enricher:
    def __init__(self,
                 npi_service: Optional[Any] = None,
                 maps_service: Optional[Any] = None,
                 scraper_service: Optional[Any] = None,
                 weights: Optional[Dict[str, float]] = None):
        if _import_errors:
            logger.debug("Service import attempts: %s", _import_errors)
        self.npi = npi_service or (NPIRegistryService() if "NPIRegistryService" in globals() else None)
        self.maps = maps_service or (GoogleMapsService() if "GoogleMapsService" in globals() else None)
        self.scraper = scraper_service or (WebsiteScraper() if "WebsiteScraper" in globals() else None)
        self.weights = weights or ENRICH_WEIGHTS

    def _pick_candidate_urls(self, validated_record: Dict[str, Any]) -> list:
        """
        Source order to try for website scraping:
        1. raw_extracted (if contains website/url)
        2. csv raw (if present in source.raw_csv)
        3. google maps proof link (we will construct candidate from place proof)
        """
        candidates = []
        # examine source raw_extracted and raw_csv
        src = validated_record.get("source", {})
        raw_extracted = src.get("raw_extracted") or {}
        raw_csv = src.get("raw_csv") or {}

        # common keys
        keys = ["website", "url", "clinic_website", "site", "hospital_website"]
        for k in keys:
            v = None
            if isinstance(raw_extracted, dict):
                v = raw_extracted.get(k)
            if not v and isinstance(raw_csv, dict):
                v = raw_csv.get(k)
            if v and isinstance(v, str) and v.strip():
                candidates.append(v.strip())

        # check normalized fields for urls
        norm = validated_record.get("normalized", {})
        for k in ["website", "url"]:
            v = norm.get(k)
            if v and isinstance(v, str):
                candidates.append(v.strip())

        # de-duplicate, preserve order
        out = []
        seen = set()
        for u in candidates:
            if not u:
                continue
            # normalize basic
            u2 = u.strip()
            if u2 not in seen:
                seen.add(u2)
                out.append(u2)
        return out

    def _safe_call(self, func, *args, **kwargs):
        """Call a service function with exception handling and small delay to prevent bursts."""
        try:
            res = func(*args, **kwargs)
            # small pause to avoid rapid-fire external calls (tune if needed)
            time.sleep(0.05)
            return res
        except Exception as e:
            logger.exception("Service call failed: %s", e)
            return None

    def enrich_provider(self, provider_id: str, validated_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich single provider. Returns enriched_record dict (with confidence and signals).
        """
        validated_record = deepcopy(validated_record or {})
        out: Dict[str, Any] = {
            "provider_id": provider_id,
            "timestamp_utc": _now_iso_utc(),
            "validated_record": validated_record,
            "npi": None,
            "npi_signals": {},
            "maps": None,
            "maps_signals": {},
            "scraper": None,
            "scraper_signals": {},
            "combined_confidence": 0.0,
            "component_confidences": {"npi": 0.0, "maps": 0.0, "scraper": 0.0},
        }

        # extract useful fields from validated_record
        norm = validated_record.get("normalized", {}) or {}
        src = validated_record.get("source", {}) or {}

        provider_name = norm.get("name") or (validated_record.get("normalized") or {}).get("name") or None
        specialization = ",".join(validated_record.get("normalized", {}).get("specializations") or []) or None
        address = norm.get("address") or None
        phone = norm.get("phone") or None

        # 1) NPI lookup
        npi_score = 0.0
        npi_result = None
        if ENABLE_NPI and self.npi:
            try:
                # if a registration number is present in validated_record, try as direct override
                reg = validated_record.get("normalized", {}).get("registration_number") or validated_record.get("source", {}).get("raw_extracted", {}).get("registration_number")
                npi_result = self._safe_call(
                    self.npi.get_best_match,
                    provider_name,
                    specialization,
                    state=None,
                    external_address=address,
                    external_phone=phone,
                    npi_number=reg
                )
                if isinstance(npi_result, dict):
                    # The service returns match_confidence in 0..1 when available
                    npi_score = float(npi_result.get("match_confidence") or 0.0)
                else:
                    npi_score = 0.0
            except Exception:
                npi_score = 0.0
        out["npi"] = npi_result
        out["component_confidences"]["npi"] = round(npi_score, 3)
        out["npi_signals"] = {"has_npi_result": bool(npi_result)}

        # 2) Google Maps enrichment
        maps_score = 0.0
        maps_result = None
        if ENABLE_MAPS and self.maps:
            try:
                maps_result = self._safe_call(self.maps.enrich_provider_location, provider_name or "", address or "")
                if isinstance(maps_result, dict):
                    maps_score = float(maps_result.get("match_score") or 0.0)
                else:
                    maps_score = 0.0
            except Exception:
                maps_score = 0.0
        out["maps"] = maps_result
        out["component_confidences"]["maps"] = round(maps_score, 3)
        out["maps_signals"] = {"place_found": bool(maps_result and maps_result.get("match_score"))}

        # 3) Website scraping
        scraper_score = 0.0
        scraper_result = None
        if ENABLE_SCRAPER and self.scraper:
            # pick candidate urls (prefer explicit sites from validated record)
            candidate_urls = self._pick_candidate_urls(validated_record)
            # if maps provides a proof link use it (convert to place url)
            if maps_result and isinstance(maps_result, dict):
                proof = maps_result.get("proof_link")
                if proof and proof not in candidate_urls:
                    candidate_urls.append(proof)

            # try candidate urls until a good scrape result found
            for url in candidate_urls + []:
                try:
                    # prefer saving the result and short-circuit if doctor match strong
                    res = self._safe_call(self.scraper.scrape, url, provider_id,
                                          provider_name, specialization, False)
                    if not res:
                        continue
                    # score heuristics: combine doctor/hospital scores if present
                    ds = float(res.get("doctor", {}).get("score", 0.0) or 0.0)
                    hs = float(res.get("hospital", {}).get("score", 0.0) or 0.0)
                    # combined scraper score: doctor weight heavier if doctor page found
                    score = ds * 0.7 + hs * 0.3
                    # if structured_profile exists, boost
                    if res.get("doctor", {}).get("structured_profile"):
                        score = min(1.0, score + 0.1)
                    scraper_result = res
                    scraper_score = score
                    # break on good match
                    if score >= 0.5:
                        break
                except Exception:
                    continue

            # fallback: if no candidates, attempt home page extraction once
            if not scraper_result and candidate_urls == [] and norm.get("website"):
                try:
                    res = self._safe_call(self.scraper.scrape, norm.get("website"), provider_id,
                                          provider_name, specialization, False)
                    if res:
                        ds = float(res.get("doctor", {}).get("score", 0.0) or 0.0)
                        hs = float(res.get("hospital", {}).get("score", 0.0) or 0.0)
                        scraper_score = ds * 0.7 + hs * 0.3
                        scraper_result = res
                except Exception:
                    scraper_score = 0.0

        out["scraper"] = scraper_result
        out["component_confidences"]["scraper"] = round(scraper_score, 3)
        out["scraper_signals"] = {"scrape_found": bool(scraper_result)}

        # 4) combine confidences
        w_n = float(self.weights.get("npi", 0.0))
        w_m = float(self.weights.get("maps", 0.0))
        w_s = float(self.weights.get("scraper", 0.0))
        combined = (w_n * npi_score) + (w_m * maps_score) + (w_s * scraper_score)
        out["combined_confidence"] = round(min(1.0, combined), 3)

        # 5) merge some canonical outputs for convenience
        # prefer NPI basic, else normalized validated, else scraper
        canonical = {}
        canonical["name"] = provider_name or (validated_record.get("normalized") or {}).get("name") or ""
        # phone preference: validated normalized -> maps -> scraper
        canonical["phone"] = norm.get("phone") or (maps_result or {}).get("google_phone") or (scraper_result or {}).get("doctor", {}).get("doctor_phones", [])
        canonical["email"] = (validated_record.get("normalized") or {}).get("email") or (scraper_result or {}).get("doctor", {}).get("doctor_emails", [])
        canonical["address"] = norm.get("address") or (maps_result or {}).get("google_formatted_address") or (scraper_result or {}).get("hospital", {}).get("addresses", [])
        canonical["npi"] = (npi_result or {}).get("npi") if isinstance(npi_result, dict) else None
        out["canonical"] = canonical

        # attach simple decision: status
        status = "UNCONFIRMED"
        if out["combined_confidence"] >= 0.85:
            status = "VERIFIED"
        elif out["combined_confidence"] >= 0.5:
            status = "REVIEW"
        else:
            status = "PENDING"
        out["status"] = status

        return out

# ---- batch runner ----
def run_batch(validated_json_path: str = VALIDATED_JSON_PATH,
              enriched_json_path: str = ENRICHED_JSON_PATH,
              workers: int = 6,
              force: bool = False) -> Dict[str, Any]:

    # guard imports
    if _import_errors and ("NPIRegistryService" not in globals() or "GoogleMapsService" not in globals() or "WebsiteScraper" not in globals()):
        logger.warning("Some service imports failed: %s", _import_errors)
        # still continue if toggles disable services, else raise
        if ENABLE_NPI or ENABLE_MAPS or ENABLE_SCRAPER:
            # we will proceed but services missing -> won't be used
            logger.info("Proceeding; missing services will be skipped.")

    validated = _load_json(validated_json_path)
    if not validated:
        logger.info("No validated records found at %s", validated_json_path)
        return {}

    enriched_existing = _load_json(enriched_json_path)
    if not isinstance(enriched_existing, dict):
        enriched_existing = {}

    enricher = Enricher()

    provider_ids = list(validated.keys()) if isinstance(validated, dict) else []
    logger.info("Found %d validated providers to enrich (workers=%d)", len(provider_ids), workers)

    results = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = {}
        for pid in provider_ids:
            if (pid in enriched_existing) and not force:
                logger.info("Skipping %s (already enriched). Use --force to override", pid)
                results[pid] = enriched_existing.get(pid)
                continue

            rec = validated.get(pid) or validated[pid]
            # schedule enrichment
            fut = ex.submit(enricher.enrich_provider, pid, rec)
            futures[fut] = pid

        # collect
        for fut in as_completed(futures):
            pid = futures[fut]
            try:
                enriched_rec = fut.result()
                # merge & save immediately (atomic)
                enriched_existing[pid] = enriched_rec
                # write after each provider to persist progress
                _atomic_write(enriched_json_path, enriched_existing)
                results[pid] = enriched_rec
                logger.info("Enriched %s -> confidence %.3f", pid, enriched_rec.get("combined_confidence", 0.0))
            except Exception as e:
                logger.exception("Enrichment failed for %s: %s", pid, e)

    # final write (redundant but ensures complete)
    _atomic_write(enriched_json_path, enriched_existing)
    logger.info("Batch enrichment complete. Wrote %d records to %s", len(enriched_existing), enriched_json_path)
    return results

# ---- CLI ----
def main():
    parser = argparse.ArgumentParser(prog="enrichment_agent", description="Enrich validated providers.")
    parser.add_argument("--validated", "-v", default=VALIDATED_JSON_PATH, help="Path to validated JSON (input).")
    parser.add_argument("--out", "-o", default=ENRICHED_JSON_PATH, help="Path to enriched JSON (output).")
    parser.add_argument("--workers", "-w", type=int, default=6, help="Number of worker threads.")
    parser.add_argument("--force", "-f", action="store_true", help="Force re-enrichment even if already present.")
    args = parser.parse_args()

    run_batch(validated_json_path=args.validated, enriched_json_path=args.out, workers=args.workers, force=args.force)

if __name__ == "__main__":
    main()
