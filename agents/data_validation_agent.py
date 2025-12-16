# agents/data_validation_agent.py  (EY-HACKATHON FINAL – DECISION-CORRECT)

from __future__ import annotations
import json, os, re, tempfile
from datetime import datetime

PROCESSED_DIR = "data/processed"
VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
os.makedirs(PROCESSED_DIR, exist_ok=True)

DIGITS = re.compile(r"\d+")

def norm_name(x): return re.sub(r"\s+", " ", str(x or "")).strip().title()
def norm_phone(x): return "".join(DIGITS.findall(str(x))) if x else ""
def norm_addr(x): return re.sub(r"\s+", " ", str(x or "")).strip()
def safe(x): return str(x).strip() if x else ""

def _atomic_write(path, data):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)

class DataValidationAgent:

    def _load(self):
        if not os.path.exists(VALIDATED_JSON): return {}
        try:
            with open(VALIDATED_JSON, "r", encoding="utf-8") as f:
                t = f.read().strip()
                return json.loads(t) if t else {}
        except:
            return {}

    def run(self, provider_id: str, csv_row: dict, pdf_row: dict | None):

        name = norm_name(
            (pdf_row or {}).get("registered_name") or
            csv_row.get("Name") or csv_row.get("name")
        )

        phone = norm_phone(
            (pdf_row or {}).get("phone") or
            csv_row.get("Phone_No") or csv_row.get("phone")
        )

        address = norm_addr(
            (pdf_row or {}).get("registered_address") or
            csv_row.get("Address") or csv_row.get("address")
        )

        npi = safe(
            (pdf_row or {}).get("npi") or
            csv_row.get("NPI_ID")
        )

        # ---- LICENSE FIELDS (ONLY REQUIRED IF PDF EXISTS) ----
        registration = safe((pdf_row or {}).get("license_number"))
        license_status = safe((pdf_row or {}).get("license_status"))
        issue_date = safe((pdf_row or {}).get("issue_date"))
        expiry_date = safe((pdf_row or {}).get("expiry_date"))

        missing = []

        if not name: missing.append("name")
        if not phone: missing.append("phone")
        if not address: missing.append("address")

        if pdf_row:
            if not registration: missing.append("registration_number")
            if not license_status: missing.append("license_status")
            if not issue_date: missing.append("issue_date")
            if not expiry_date: missing.append("expiry_date")

        score = 0.0
        score += 0.25 if name else 0
        score += 0.20 if phone else 0
        score += 0.25 if address else 0
        score += 0.15 if npi else 0
        if pdf_row:
            score += 0.15 if registration else 0

        score = round(score, 2)

        if score >= 0.8 and not missing:
            status = "PASS"
        elif score >= 0.5:
            status = "PASS_WITH_GAPS"
        else:
            status = "FAIL_NEEDS_REVIEW"

        record = {
            "provider_id": provider_id,
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "normalized": {
                "name": name,
                "phone": phone,
                "address": address,
                "registration_number": registration,
                "license_status": license_status,
                "issue_date": issue_date,
                "expiry_date": expiry_date,
                "npi": npi
            },
            "missing_fields": missing,
            "overall_confidence": score,
            "validation_status": status
        }

        data = self._load()
        data[provider_id] = record
        _atomic_write(VALIDATED_JSON, data)
        return record
                                               