# agents/data_validation_agent.py  (EY-CORRECT, FROM SCRATCH)

import os, json, re, tempfile
from datetime import datetime, date

PROCESSED_DIR = "data/processed"
VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ---------- helpers ----------
def clean(x): return x.strip() if isinstance(x, str) else ""
def is_present(x): return bool(clean(x))
def parse_date(x):
    try:
        return datetime.strptime(x.strip(), "%B %d, %Y").date()
    except Exception:
        return None

def atomic_write(path, data):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)

def safe_load(path):
    if not os.path.exists(path): return {}
    try:
        txt = open(path, "r", encoding="utf-8").read().strip()
        return json.loads(txt) if txt else {}
    except Exception:
        return {}

# ---------- CORE AGENT ----------
class DataValidationAgent:
    """
    EY RULES:
    PASS:
      - license_number present
      - license_status == ACTIVE
      - expiry_date >= today

    PASS_WITH_GAPS:
      - license valid
      - optional fields missing (NPI, phone, address)

    FAIL_NEEDS_REVIEW:
      - license missing OR
      - license_status != ACTIVE OR
      - expiry_date expired
    """

    def run(self, provider_id, csv_row, pdf_row):
        src = {}
        if csv_row: src.update(csv_row)
        if pdf_row: src.update(pdf_row)

        # -------- normalized --------
        name = clean(src.get("name") or src.get("Name") or src.get("registered_name"))
        phone = re.sub(r"\D", "", clean(src.get("phone") or src.get("Phone_No")))
        address = clean(src.get("address") or src.get("Address") or src.get("registered_address"))
        npi = clean(src.get("npi") or src.get("NPI_ID") or src.get("National Provider Identifier"))

        license_number = clean(src.get("license_number") or src.get("registration_number"))
        license_status = clean(src.get("license_status"))
        issue_date_raw = clean(src.get("issue_date"))
        expiry_date_raw = clean(src.get("expiry_date"))

        issue_date = parse_date(issue_date_raw)
        expiry_date = parse_date(expiry_date_raw)
        today = date.today()

        # -------- mandatory license checks --------
        license_missing = []
        if not is_present(license_number): license_missing.append("license_number")
        if not is_present(license_status): license_missing.append("license_status")
        if not issue_date: license_missing.append("issue_date")
        if not expiry_date: license_missing.append("expiry_date")

        license_invalid = (
            is_present(license_status) and license_status.upper() != "ACTIVE"
        ) or (
            expiry_date and expiry_date < today
        )

        # -------- decision --------
        if license_missing or license_invalid:
            status = "FAIL_NEEDS_REVIEW"
        else:
            optional_missing = []
            if not is_present(npi): optional_missing.append("npi")
            if not is_present(phone): optional_missing.append("phone")
            if not is_present(address): optional_missing.append("address")

            status = "PASS" if not optional_missing else "PASS_WITH_GAPS"

        # -------- missing fields (STRICT) --------
        missing_fields = []
        if status != "PASS":
            if not is_present(license_number): missing_fields.append("license_number")
            if not is_present(license_status): missing_fields.append("license_status")
            if not issue_date: missing_fields.append("issue_date")
            if not expiry_date: missing_fields.append("expiry_date")

        # -------- confidence (simple & honest) --------
        overall_confidence = (
            1.0 if status == "PASS" else
            0.7 if status == "PASS_WITH_GAPS" else
            0.2
        )

        record = {
            "provider_id": provider_id,
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "normalized": {
                "name": name,
                "phone": phone,
                "address": address,
                "npi": npi,
                "license_number": license_number,
                "license_status": license_status,
                "issue_date": issue_date_raw,
                "expiry_date": expiry_date_raw,
            },
            "missing_fields": missing_fields,
            "overall_confidence": overall_confidence,
            "validation_status": status
        }

        data = safe_load(VALIDATED_JSON)
        data[provider_id] = record
        atomic_write(VALIDATED_JSON, data)
        return record
