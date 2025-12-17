#!/usr/bin/env python3
# agents/quality_assurance_agent.py
# EY Hackathon – Production-ready QA Agent

from __future__ import annotations
import os
import sys
import json
import argparse
from datetime import datetime, timezone
from typing import Dict, Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---------------- CONFIG ----------------
PROCESSED_DIR = os.path.join("data", "processed")
ENRICHED_JSON = os.path.join(PROCESSED_DIR, "enriched_data.json")
QA_JSON = os.path.join(PROCESSED_DIR, "qa_results.json")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# ---------------- HELPERS ----------------
def utc_now():
    return datetime.now(timezone.utc).isoformat()

def load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def atomic_write(path: str, data: Dict[str, Any]):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

# ---------------- QA RULE ENGINE ----------------
class QualityAssuranceAgent:

    VERIFY_THRESHOLD = 0.80
    REVIEW_THRESHOLD = 0.45

    def classify(self, enriched: Dict[str, Any]) -> Dict[str, Any]:
        combined = float(enriched.get("combined_confidence", 0.0))

        base = enriched.get("base_validation", {})
        validation_status = base.get("validation_status", "UNKNOWN")

        issues = []

        # -------- Hard failures --------
        norm = base.get("normalized", {})
        if norm.get("license_status", "").upper() not in ("ACTIVE", ""):
            issues.append("license_not_active")

        if not norm.get("registration_number"):
            issues.append("missing_license_number")

        # -------- Signal consistency --------
        npi = enriched.get("enriched", {}).get("npi")
        maps = enriched.get("enriched", {}).get("maps")

        if npi and maps:
            addr_npi = ""
            try:
                addr_npi = npi.get("addresses", [{}])[0].get("address_1", "")
            except Exception:
                pass
            addr_maps = maps.get("formatted_address", "")
            if addr_npi and addr_maps and addr_npi.lower()[:10] not in addr_maps.lower():
                issues.append("address_mismatch_npi_maps")

        # -------- Decision --------
        if issues:
            final_status = "FAIL_QA"
        elif combined >= self.VERIFY_THRESHOLD and validation_status == "PASS":
            final_status = "VERIFIED"
        elif combined >= self.REVIEW_THRESHOLD:
            final_status = "NEEDS_REVIEW"
        else:
            final_status = "REJECTED"

        return {
            "provider_id": enriched.get("provider_id"),
            "qa_timestamp_utc": utc_now(),
            "combined_confidence": combined,
            "final_status": final_status,
            "issues": issues,
            "signals": {
                "validation_status": validation_status,
                "confidence_bucket": (
                    "HIGH" if combined >= 0.8 else
                    "MEDIUM" if combined >= 0.45 else
                    "LOW"
                )
            }
        }

# ---------------- BATCH RUNNER ----------------
def run():
    enriched_all = load_json(ENRICHED_JSON)
    if not enriched_all:
        print("❌ No enriched data found")
        return

    qa = QualityAssuranceAgent()
    results = {}

    for pid, record in enriched_all.items():
        qa_result = qa.classify(record)
        results[pid] = qa_result
        print(f"QA {pid} → {qa_result['final_status']} ({qa_result['combined_confidence']})")

    atomic_write(QA_JSON, results)
    print(f"\n✅ QA completed for {len(results)} providers")
    print(f"📄 Output: {QA_JSON}")

# ---------------- CLI ----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quality Assurance Agent")
    parser.parse_args()
    run()
