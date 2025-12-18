
from __future__ import annotations
import json, os, re, tempfile
from datetime import datetime, date

PROCESSED_DIR = "data/processed"
VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
os.makedirs(PROCESSED_DIR, exist_ok=True)

DIGITS = re.compile(r"\d+")

def norm_name(x): 
    return re.sub(r"\s+", " ", str(x or "")).strip().title()

def norm_phone(x): 
    return "".join(DIGITS.findall(str(x))) if x else ""

def norm_addr(x): 
    return re.sub(r"\s+", " ", str(x or "")).strip()

def safe(x): 
    x = str(x).strip() if x else ""
    return x if x else ""

def _atomic_write(path, data):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

class DataValidationAgent:

    LICENSE_REQUIRED = ["license_number", "license_status", "expiry_date"]
    ENRICHABLE_FIELDS = ["npi", "issue_date"]

    def _load(self):
        if not os.path.exists(VALIDATED_JSON):
            return {}
        try:
            with open(VALIDATED_JSON, "r", encoding="utf-8") as f:
                return json.loads(f.read() or "{}")
        except:
            return {}

    def _is_expired(self, expiry_date_str: str) -> bool:
        try:
            exp = datetime.strptime(expiry_date_str, "%B %d, %Y").date()
            return exp < date.today()
        except:
            return True  # invalid date = unsafe

    def run(self, provider_id: str, csv_row: dict, pdf_row: dict | None):

        name = norm_name((pdf_row or {}).get("registered_name") or csv_row.get("Name"))
        phone = norm_phone((pdf_row or {}).get("phone") or csv_row.get("Phone_No"))
        address = norm_addr((pdf_row or {}).get("registered_address") or csv_row.get("Address"))

        registration = safe((pdf_row or {}).get("license_number"))
        license_status = safe((pdf_row or {}).get("license_status"))
        issue_date = safe((pdf_row or {}).get("issue_date"))
        expiry_date = safe((pdf_row or {}).get("expiry_date"))
        npi = safe((pdf_row or {}).get("npi") or csv_row.get("NPI_ID"))

        missing = []
        if not registration:
            missing.append("license_number")
        if not license_status:
            missing.append("license_status")
        if not expiry_date:
            missing.append("expiry_date")

        if not npi:
            missing.append("npi")
        if not issue_date:
            missing.append("issue_date")

        if (
            "license_number" in missing or
            "license_status" in missing or
            "expiry_date" in missing or
            license_status.upper() != "ACTIVE" or
            self._is_expired(expiry_date)
        ):
            status = "FAIL_NEEDS_REVIEW"

        elif missing:
            status = "PASS_WITH_GAPS"

        else:
            status = "PASS"

        confidence = round(
            (1.0 - (len(missing) * 0.15)), 2
        )
        confidence = max(0.0, min(confidence, 1.0))

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
            "overall_confidence": confidence,
            "validation_status": status
        }

        data = self._load()
        data[provider_id] = record
        _atomic_write(VALIDATED_JSON, data)
        return record
