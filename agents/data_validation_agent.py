# agents/data_validation_agent.py
"""
Data Validation Agent
- Merge CSV / provider row + extracted JSON from PDF (pdf_vlm_extractor)
- Clean / normalize fields
- Validate basic formats (email, phone, reg no, experience)
- Produce validation_flags, missing_fields, field_confidence
- Save results to settings.VALIDATED_JSON

Usage (example):
from agents.data_validation_agent import DataValidationAgent
agent = DataValidationAgent()
validated = agent.run(provider_id="SAMPLE1", csv_row=csv_row_dict, extracted_json=pdf_json)
"""

import os
import re
import json
import tempfile
from typing import Dict, Any, Optional
from datetime import datetime

from config import settings

# --- CONFIG ---
DEFAULT_COUNTRY_CODE = "+91"  # Change if you prefer other defaults for phone normalization
MIN_PHONE_DIGITS = 7
PREFERRED_SOURCE = ("csv", "pdf")  # csv preferred over pdf when merging; configurable

# --- HELPER REGEX ---
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
REG_NO_RE = re.compile(r"[A-Za-z0-9\-\/]+")  # simple alnum + dash/slash
YEAR_RE = re.compile(r"(\d{1,2})\s*(?:yrs?|years?)", re.IGNORECASE)
DIGITS_RE = re.compile(r"\d+")


def _atomic_write(path: str, data: Dict[str, Any]):
    """Write JSON atomically to avoid partial writes."""
    d = os.path.dirname(path)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="tmp_valid_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


class DataValidationAgent:
    def __init__(self, preferred_source: tuple = PREFERRED_SOURCE):
        self.preferred_source = preferred_source

    # -------------------------
    # Normalizers & validators
    # -------------------------
    def normalize_name(self, name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        # remove common prefixes/titles and extra whitespace
        name = re.sub(r"\b(Dr|Dr\.|Prof|Mr|Ms|Mrs|Drs|Drs\.)\b", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[^\w\s\-\.'’]", " ", name)  # remove odd punctuation
        parts = [p.strip().title() for p in name.split() if p.strip()]
        return " ".join(parts) if parts else None

    def normalize_email(self, email: Optional[str]) -> Optional[str]:
        if not email:
            return None
        email = email.strip().lower()
        email = re.sub(r"\s+", "", email)
        return email if EMAIL_RE.match(email) else None

    def normalize_phone(self, phone: Optional[str]) -> Optional[str]:
        if not phone:
            return None
        # extract digits
        digits = "".join(re.findall(r"\d", str(phone)))
        if len(digits) < MIN_PHONE_DIGITS:
            return None
        # heuristic: if 10 digits -> assume Indian local, prepend +91 (configurable)
        if len(digits) == 10:
            return f"{DEFAULT_COUNTRY_CODE} {digits}"
        # if already contains country code (11+ digits)
        if len(digits) > 10 and digits.startswith("0"):
            # strip leading zero and assume default country code
            digits = digits.lstrip("0")
            return f"{DEFAULT_COUNTRY_CODE} {digits}"
        if len(digits) > 10:
            return f"+{digits}"
        # fallback
        return digits

    def normalize_address(self, addr: Optional[str]) -> Optional[str]:
        if not addr:
            return None
        s = re.sub(r"[\r\n\t]+", " ", addr)
        s = re.sub(r"\s{2,}", " ", s)
        s = s.strip()
        return s if len(s) >= 5 else None

    def normalize_qualifications(self, q: Optional[str]):
        if not q:
            return []
        parts = [p.strip().upper().replace(".", "") for p in re.split(r"[,/;]+", q) if p.strip()]
        return parts

    def normalize_registration(self, reg: Optional[str]) -> Optional[str]:
        if not reg:
            return None
        m = REG_NO_RE.findall(reg)
        if not m:
            return None
        return "".join(m)

    def extract_experience_years(self, exp: Optional[str]) -> Optional[int]:
        if not exp:
            return None
        # numeric value directly?
        digits = DIGITS_RE.findall(exp)
        if digits:
            # pick the first reasonable number
            for d in digits:
                try:
                    val = int(d)
                    if 0 <= val < 120:
                        return val
                except:
                    continue
        # pattern like "5 years", "10 yrs"
        m = YEAR_RE.search(exp)
        if m:
            try:
                val = int(m.group(1))
                return val
            except:
                pass
        return None

    # -------------------------
    # Merge logic
    # -------------------------
    def merge_records(self, provider_id: str, csv_row: Optional[Dict[str, Any]], pdf_json: Optional[Dict[str, Any]]):
        """
        Merge CSV row (source: csv_row) and PDF-extracted JSON (pdf_json).
        Preferred source order = self.preferred_source
        """
        merged = {}

        # keys we manage
        keys = [
            "name",
            "qualifications",
            "clinic_address",
            "phone",
            "email",
            "specializations",
            "experience_years",
            "registration_number",
        ]

        sources = {"csv": csv_row or {}, "pdf": pdf_json or {}}

        for k in keys:
            value = None
            for src in self.preferred_source:
                if src in sources and sources[src] and (k in sources[src]) and sources[src].get(k) not in (None, "", [], {}):
                    value = sources[src].get(k)
                    break
            merged[k] = value

        # metadata
        merged["_provider_id"] = provider_id
        merged["_merged_at"] = datetime.utcnow().isoformat() + "Z"
        return merged

    # -------------------------
    # Confidence helpers
    # -------------------------
    def _field_confidence(self, field_name: str, value, csv_row: Optional[Dict[str, Any]], pdf_json: Optional[Dict[str, Any]]):
        """
        Simple heuristic scoring:
        - If value came from CSV => +0.7
        - If comes from PDF => +0.5
        - If both present and match -> +0.9
        - If value missing -> 0.0
        """
        csv_val = (csv_row or {}).get(field_name) if csv_row else None
        pdf_val = (pdf_json or {}).get(field_name) if pdf_json else None

        if value in (None, "", [], {}):
            return 0.0

        if csv_val and pdf_val:
            # compare simple normalized strings
            try:
                if str(csv_val).strip().lower() == str(pdf_val).strip().lower():
                    return 0.92
            except:
                pass
            return 0.72

        if csv_val and not pdf_val:
            return 0.80
        if pdf_val and not csv_val:
            return 0.6
        return 0.5

    # -------------------------
    # Main run
    # -------------------------
    def run(self, provider_id: str, csv_row: Optional[Dict[str, Any]] = None, extracted_json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Validate and normalize a single provider record.
        - provider_id: unique id (from CSV filename or CSV column)
        - csv_row: dict (may be None)
        - extracted_json: dict from pdf_vlm_extractor (may be None)
        Returns validated record (and writes it to settings.VALIDATED_JSON).
        """
        # 1) Merge
        merged = self.merge_records(provider_id, csv_row, extracted_json)

        # 2) Normalize fields
        raw_name = merged.get("name")
        raw_qual = merged.get("qualifications")
        raw_addr = merged.get("clinic_address")
        raw_phone = merged.get("phone")
        raw_email = merged.get("email")
        raw_specs = merged.get("specializations")
        raw_exp = merged.get("experience_years")
        raw_reg = merged.get("registration_number")

        name = self.normalize_name(raw_name)
        qualifications = self.normalize_qualifications(raw_qual) if raw_qual else []
        address = self.normalize_address(raw_addr)
        phone = self.normalize_phone(raw_phone)
        email = self.normalize_email(raw_email)
        specializations = raw_specs if isinstance(raw_specs, list) else ([s.strip() for s in re.split(r"[,/;]+", str(raw_specs)) if s.strip()] if raw_specs else [])
        experience_years = self.extract_experience_years(raw_exp) if raw_exp else None
        registration_number = self.normalize_registration(raw_reg)

        # 3) Flags & missing fields
        missing = []
        flags = []

        # mandatory-ish: name, address, phone or email (at least one contact)
        if not name:
            missing.append("name")
            flags.append("MISSING_NAME")
        if not address:
            missing.append("clinic_address")
            flags.append("MISSING_ADDRESS")
        if not phone and not email:
            missing.append("contact")
            flags.append("MISSING_CONTACT")

        # validation flags
        if raw_email and not email:
            flags.append("INVALID_EMAIL")
        if raw_phone and not phone:
            flags.append("INVALID_PHONE")
        if raw_reg and not registration_number:
            flags.append("INVALID_REGISTRATION")
        if experience_years is None and raw_exp:
            # provided but couldn't parse
            flags.append("INVALID_EXPERIENCE")

        # 4) Field-wise confidence
        field_confidence = {
            "name": self._field_confidence("name", name, csv_row, extracted_json),
            "clinic_address": self._field_confidence("clinic_address", address, csv_row, extracted_json),
            "phone": self._field_confidence("phone", phone, csv_row, extracted_json),
            "email": self._field_confidence("email", email, csv_row, extracted_json),
            "specializations": self._field_confidence("specializations", specializations, csv_row, extracted_json),
            "experience_years": self._field_confidence("experience_years", experience_years, csv_row, extracted_json),
            "registration_number": self._field_confidence("registration_number", registration_number, csv_row, extracted_json),
        }

        # 5) Create validated record
        validated = {
            "provider_id": provider_id,
            "name": name or "",
            "qualifications": qualifications,
            "clinic_address": address or "",
            "phone": phone or "",
            "email": email or "",
            "specializations": specializations or [],
            "experience_years": experience_years if experience_years is not None else "",
            "registration_number": registration_number or "",
            "validation_flags": flags,
            "missing_fields": missing,
            "field_confidence": field_confidence,
            "merged_source": {
                "csv_present": bool(csv_row),
                "pdf_present": bool(extracted_json)
            },
            "validated_at": datetime.utcnow().isoformat() + "Z"
        }

        # 6) Persist to processed/validated_data.json (append / update keyed by provider_id)
        validated_path = settings.VALIDATED_JSON
        current = {}
        if os.path.exists(validated_path):
            try:
                with open(validated_path, "r", encoding="utf-8") as f:
                    current = json.load(f) or {}
            except Exception:
                current = {}

        current[provider_id] = validated
        _atomic_write(validated_path, current)

        return validated


# -------------------------
# CLI TESTING
# -------------------------
if __name__ == "__main__":
    import sys

    agent = DataValidationAgent()

    # Usage: python data_validation_agent.py <provider_id> [csv_json_path] [extracted_json_path]
    if len(sys.argv) < 2:
        print("Usage: python agents/data_validation_agent.py <provider_id> [csv_json_path] [extracted_json_path]")
        sys.exit(1)

    pid = sys.argv[1]
    csv_row = None
    extracted = None

    if len(sys.argv) >= 3:
        try:
            csv_row = json.load(open(sys.argv[2], "r", encoding="utf-8"))
        except Exception as e:
            print("Could not load csv row JSON:", e)

    if len(sys.argv) >= 4:
        try:
            extracted = json.load(open(sys.argv[3], "r", encoding="utf-8"))
        except Exception as e:
            print("Could not load extracted JSON:", e)

    out = agent.run(pid, csv_row, extracted)
    print(json.dumps(out, indent=2, ensure_ascii=False))
