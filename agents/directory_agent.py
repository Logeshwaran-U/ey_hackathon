#!/usr/bin/env python3
# agents/directory_agent.py
# EY Hackathon – FINAL Directory Management Agent (Production)

from __future__ import annotations
import os
import sys
import json
import csv
from typing import Dict, Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ---------------- PATHS ----------------
PROCESSED_DIR = os.path.join("data", "processed")
ENRICHED_JSON = os.path.join(PROCESSED_DIR, "enriched_data.json")
QA_JSON = os.path.join(PROCESSED_DIR, "qa_results.json")

OUT_ALL = os.path.join(PROCESSED_DIR, "providers_all.csv")
OUT_PASS = os.path.join(PROCESSED_DIR, "providers_verified.csv")
OUT_FAIL = os.path.join(PROCESSED_DIR, "providers_failed.csv")

os.makedirs(PROCESSED_DIR, exist_ok=True)

CSV_FIELDS = [
    "provider_id",
    "name",
    "phone",
    "address",
    "npi",
    "final_status",
    "final_confidence"
]

# ---------------- HELPERS ----------------
def load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def write_csv(path: str, rows: list[dict]):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in CSV_FIELDS})

# ---------------- DIRECTORY AGENT ----------------
def run():
    enriched_all = load_json(ENRICHED_JSON)
    qa_all = load_json(QA_JSON)

    if not enriched_all or not qa_all:
        print("❌ Missing enriched or QA data")
        return

    all_rows = []
    pass_rows = []
    fail_rows = []

    for pid, enriched in enriched_all.items():
        qa = qa_all.get(pid, {})

        base = enriched.get("base_validation", {})
        norm = base.get("normalized", {})

        row = {
            "provider_id": pid,
            "name": norm.get("name", ""),
            "phone": norm.get("phone", ""),
            "address": norm.get("address", ""),
            "npi": norm.get("npi", ""),
            "final_status": qa.get("final_status", "UNKNOWN"),
            "final_confidence": round(float(qa.get("combined_confidence", 0.0)), 3)
        }

        all_rows.append(row)

        if row["final_status"] == "VERIFIED":
            pass_rows.append(row)
        else:
            fail_rows.append(row)

    write_csv(OUT_ALL, all_rows)
    write_csv(OUT_PASS, pass_rows)
    write_csv(OUT_FAIL, fail_rows)

    print(f"✅ Directory generation complete")
    print(f"📄 All providers   → {OUT_ALL} ({len(all_rows)})")
    print(f"✅ Verified only   → {OUT_PASS} ({len(pass_rows)})")
    print(f"❌ Failed / Review → {OUT_FAIL} ({len(fail_rows)})")

# ---------------- CLI ----------------
if __name__ == "__main__":
    run()
