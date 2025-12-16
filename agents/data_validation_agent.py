# agents/data_validation_agent.py  (FULLY CORRECTED + SAFE)

from __future__ import annotations
import argparse, json, os, re, tempfile
from datetime import datetime

# ---------------- SETTINGS ----------------
try:
    from config import settings as SETTINGS
except Exception:
    class _S:
        PROCESSED_DIR = os.path.join("data", "processed")
        VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
    SETTINGS = _S()

os.makedirs(SETTINGS.PROCESSED_DIR, exist_ok=True)

# ---------------- REGEX ----------------
DIGITS_RE = re.compile(r"\d+")
REGNO_RE = re.compile(r"[A-Za-z0-9\/\-\s]+")

# ---------------- NORMALIZERS ----------------
def normalize_name(x):
    return re.sub(r"\s+", " ", re.sub(r"\b(dr|mr|ms|mrs|prof)\b\.?", "", (x or ""), flags=re.I)).strip().title()

def normalize_phone(x):
    return "".join(DIGITS_RE.findall(str(x))) if x else ""

def normalize_address(x):
    return re.sub(r"\s+", " ", str(x)).strip() if x else ""

def normalize_reg(x):
    return "".join(REGNO_RE.findall(str(x))) if x else ""

# ---------------- ATOMIC WRITE ----------------
def _atomic_write(path, data):
    d = os.path.dirname(path)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

# ---------------- AGENT ----------------
class DataValidationAgent:

    def __init__(self, out_path=SETTINGS.VALIDATED_JSON):
        self.out_path = out_path

    def _load(self):
        if not os.path.exists(self.out_path):
            return {}
        try:
            with open(self.out_path, "r", encoding="utf-8") as f:
                txt = f.read().strip()
                return json.loads(txt) if txt else {}
        except Exception:
            return {}

    def run(self, provider_id: str, csv_row: dict, pdf_row: dict):

        src = {}
        if csv_row: src.update(csv_row)
        if pdf_row: src.update(pdf_row)

        name = normalize_name(src.get("Name") or src.get("name") or src.get("registered_name"))
        phone = normalize_phone(src.get("Phone_No") or src.get("phone"))
        address = normalize_address(src.get("Address") or src.get("address") or src.get("registered_address"))
        reg = normalize_reg(src.get("registration_number"))

        missing = [k for k, v in {
            "name": name,
            "phone": phone,
            "address": address,
            "registration": reg
        }.items() if not v]

        overall = round(
            (1 if name else 0) * 0.3 +
            (1 if phone else 0) * 0.2 +
            (1 if address else 0) * 0.3 +
            (1 if reg else 0) * 0.2,
            2
        )

        if overall >= 0.8 and not missing:
            status = "PASS"
        elif overall >= 0.5:
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
                "registration_number": reg
            },
            "missing_fields": missing,
            "overall_confidence": overall,
            "validation_status": status
        }

        data = self._load()
        data[provider_id] = record
        _atomic_write(self.out_path, data)
        return record

# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("provider_id")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--pdf")
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()

    csv_row = json.load(open(args.csv))
    pdf_row = json.load(open(args.pdf)) if args.pdf else None

    out = DataValidationAgent().run(args.provider_id, csv_row, pdf_row)
    if args.show:
        print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
