from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from copy import deepcopy
from datetime import datetime
from typing import Dict, Any, Optional

# -------------------------------------------------------------
# FIXED SETTINGS IMPORT  ✅ (Correct Import)
# -------------------------------------------------------------
try:
    from config import settings as SETTINGS
except Exception:
    class _S:
        PROCESSED_DIR = os.path.join("data", "processed")

        VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
        DEFAULT_REGION = "IN"
        FINAL_CONF_WEIGHTS = {"source": 0.5, "format": 0.5}
    SETTINGS = _S()

# Ensure processed dir exists
os.makedirs(getattr(SETTINGS, "PROCESSED_DIR", "data/processed"), exist_ok=True)

# File paths
VALIDATED_JSON_PATH = getattr(SETTINGS, "VALIDATED_JSON",
                              os.path.join(SETTINGS.PROCESSED_DIR, "validated_data.json"))

FINAL_CONF_WEIGHTS = getattr(SETTINGS, "FINAL_CONF_WEIGHTS",
                             {"source": 0.5, "format": 0.5})

DEFAULT_REGION = getattr(SETTINGS, "DEFAULT_REGION", "IN")

# -------------------------------------------------------------
# REGEXES
# -------------------------------------------------------------
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
REGNO_RE = re.compile(r"[A-Za-z0-9\/\-\s]+")
YEAR_RE = re.compile(r"(\d{1,2})\s*(?:yrs?|years?)", re.IGNORECASE)
DIGITS_RE = re.compile(r"\d+")

# -------------------------------------------------------------
# LOGGER FALLBACK
# -------------------------------------------------------------
try:
    from utils.logger import get_logger
    logger = get_logger(__name__)
except Exception:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("data_validation_agent")

# -------------------------------------------------------------
# OPTIONAL phonenumbers LIB
# -------------------------------------------------------------
try:
    import phonenumbers
    PHONENUMBERS_AVAILABLE = True
except Exception:
    PHONENUMBERS_AVAILABLE = False


# -------------------------------------------------------------
# NORMALIZERS
# -------------------------------------------------------------
def normalize_name(name: Optional[str]) -> str:
    if not name:
        return ""
    s = re.sub(r"\b(Dr|Dr\.|Prof|Mr|Ms|Mrs|Drs|Drs\.)\b", "", name, flags=re.IGNORECASE)
    s = re.sub(r"[^\w\s\-\.'’]", " ", s)
    parts = [p.strip().title() for p in s.split() if p.strip()]
    return " ".join(parts)


def normalize_email(email: Optional[str]) -> str:
    if not email:
        return ""
    e = email.strip().lower()
    e = re.sub(r"\s+", "", e)
    return e


def normalize_phone(phone: Optional[str]) -> str:
    if not phone:
        return ""
    s = str(phone).strip()
    if "+" in s:
        s = "+" + "".join(re.findall(r"\d+", s))
    else:
        s = "".join(re.findall(r"\d+", s))
    return s


def normalize_address(addr: Optional[str]) -> str:
    if not addr:
        return ""
    s = re.sub(r"[\r\n\t]+", " ", str(addr))
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def normalize_qualifications(q: Optional[str]) -> list:
    if not q:
        return []
    return [p.strip().upper().replace(".", "") for p in re.split(r"[,/;]+", str(q)) if p.strip()]


def normalize_registration(reg: Optional[str]) -> str:
    if not reg:
        return ""
    m = REGNO_RE.findall(str(reg))
    return "".join(m).strip()


def extract_experience_years(exp: Optional[str]) -> Optional[int]:
    if not exp:
        return None
    digits = DIGITS_RE.findall(str(exp))
    if digits:
        for d in digits:
            try:
                val = int(d)
                if 0 <= val < 120:
                    return val
            except:
                continue
    m = YEAR_RE.search(str(exp))
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None


# -------------------------------------------------------------
# ATOMIC WRITE HELPERS
# -------------------------------------------------------------
def _atomic_write(path: str, data: Dict[str, Any]) -> None:
    d = os.path.dirname(path)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="tmp_valid_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except:
                pass


# -------------------------------------------------------------
# MAIN VALIDATION AGENT
# -------------------------------------------------------------
class DataValidationAgent:

    def __init__(self, validated_json_path: str = VALIDATED_JSON_PATH, default_region: str = DEFAULT_REGION):
        self.validated_json_path = validated_json_path
        self.default_region = default_region

    def _load_existing(self) -> Dict[str, Any]:
        if not os.path.exists(self.validated_json_path):
            return {}
        try:
            return json.load(open(self.validated_json_path, "r", encoding="utf-8"))
        except Exception:
            return {}

    # ---------------------------------------------------------
    # FORMAT CONFIDENCE CHECKS
    # ---------------------------------------------------------
    def _format_phone_confidence(self, phone: str) -> float:
        if not phone:
            return 0.0

        if PHONENUMBERS_AVAILABLE:
            try:
                parsed = phonenumbers.parse(phone, None) if phone.startswith("+") else phonenumbers.parse(phone, self.default_region)
                if phonenumbers.is_valid_number(parsed):
                    return 1.0
                if phonenumbers.is_possible_number(parsed):
                    return 0.8
            except:
                pass

        digits = re.sub(r"\D", "", phone)
        if 10 <= len(digits) <= 15:
            return 0.8
        if 7 <= len(digits) < 10:
            return 0.5
        return 0.0

    def _format_email_confidence(self, email: str) -> float:
        if not email:
            return 0.0
        if EMAIL_RE.match(email.strip()):
            return 1.0
        if "@" in email and "." in email.split("@")[-1]:
            return 0.6
        return 0.0

    def _format_name_confidence(self, name: str) -> float:
        if not name:
            return 0.0
        return 1.0 if len(name.split()) >= 2 else 0.6

    def _format_address_confidence(self, address: str) -> float:
        if not address:
            return 0.0
        score = 0.0
        if any(k in address.lower() for k in ["street", "road", "lane", "avenue", "sector", "block"]):
            score += 0.5
        if re.search(r"\d{1,5}", address):
            score += 0.3
        if "," in address:
            score += 0.2
        return min(score, 1.0)

    def _format_registration_confidence(self, reg: str) -> float:
        if not reg:
            return 0.0
        if re.search(r"[A-Z]{1,5}[-]?\s*\d{2,8}", reg, re.IGNORECASE):
            return 1.0
        if any(ch.isdigit() for ch in reg) and any(ch.isalpha() for ch in reg):
            return 0.6
        return 0.0

    # ---------------------------------------------------------
    # CSV / PDF SOURCE AGREEMENT CONFIDENCE
    # ---------------------------------------------------------
    def _source_confidence_for_field(self, field_name, csv_row, pdf_json, normalized_value):
        csv_val = csv_row.get(field_name) if csv_row else None
        pdf_val = None

        # PDF extractor may return phone, mobile, contact, phone_number
        possible_keys = [field_name, field_name.replace("clinic_", ""), "phone", "mobile", "contact", "phone_number"]

        if pdf_json:
            for k in possible_keys:
                if k in pdf_json and pdf_json[k] not in ("", None, []):
                    pdf_val = pdf_json[k]
                    break

        # Normalize
        def norm(x):
            return str(x).strip().lower() if x else None

        n_csv = norm(csv_val)
        n_pdf = norm(pdf_val)
        n_norm = norm(normalized_value)

        if n_csv and n_pdf:
            if n_csv == n_pdf:
                return 0.95
            if n_csv == n_norm or n_pdf == n_norm:
                return 0.85
            return 0.40

        if n_csv and not n_pdf:
            return 0.88 if n_csv == n_norm else 0.80

        if n_pdf and not n_csv:
            return 0.75 if n_pdf == n_norm else 0.60

        return 0.0

    # ---------------------------------------------------------
    # MAIN RUN
    # ---------------------------------------------------------
    def run(self, provider_id: str, csv_row=None, extracted_json=None):
        logger.info("Validating provider %s", provider_id)

        source = {}
        if extracted_json:
            source.update(deepcopy(extracted_json))
        if csv_row:
            source.update(deepcopy(csv_row))

        def get(keys, default=""):
            for k in keys:
                if k in source and source[k] not in (None, ""):
                    return source[k]
            return default

        # -----------------------------
        # FIXED FIELD PRIORITY (PDF)
        # -----------------------------
        raw_name = get(["name", "provider_name", "full_name"])
        raw_phone = get(["phone", "mobile", "contact", "phone_number"])      # FIX 3
        raw_email = get(["email", "contact_email", "email_address"])
        raw_address = get(["clinic_address", "address", "location", "addr"])  # FIXED priority
        raw_qual = get(["qualifications", "qualification", "degree"])
        raw_reg = get(["registration_number", "registration", "reg_no"])
        raw_specs = get(["specializations", "speciality", "specialties"])
        raw_exp = get(["experience_years", "experience", "exp"])

        # Normalize
        name = normalize_name(raw_name)
        phone = normalize_phone(raw_phone)
        email = normalize_email(raw_email)
        address = normalize_address(raw_address)
        qualifications = normalize_qualifications(raw_qual)
        registration_number = normalize_registration(raw_reg)
        specializations = raw_specs if isinstance(raw_specs, list) else (
            [s.strip() for s in re.split(r"[,/;]+", str(raw_specs)) if s.strip()] if raw_specs else []
        )
        experience_years = extract_experience_years(raw_exp)

        # Format confidence
        fmt = {
            "name": self._format_name_confidence(name),
            "phone": self._format_phone_confidence(phone),
            "email": self._format_email_confidence(email),
            "address": self._format_address_confidence(address),
            "registration": self._format_registration_confidence(registration_number),
            "qualifications": 1.0 if qualifications else 0.0,
            "experience_years": 1.0 if experience_years is not None else 0.0,
            "specializations": 1.0 if specializations else 0.0,
        }

        # Source confidence
        src = {
            "name": self._source_confidence_for_field("name", csv_row, extracted_json, name),
            "phone": self._source_confidence_for_field("phone", csv_row, extracted_json, phone),
            "email": self._source_confidence_for_field("email", csv_row, extracted_json, email),
            "clinic_address": self._source_confidence_for_field("clinic_address", csv_row, extracted_json, address),
            "registration": self._source_confidence_for_field("registration_number", csv_row, extracted_json, registration_number),
            "qualifications": self._source_confidence_for_field("qualifications", csv_row, extracted_json, ",".join(qualifications)),
            "experience_years": self._source_confidence_for_field("experience_years", csv_row, extracted_json, str(experience_years or "")),
            "specializations": self._source_confidence_for_field("specializations", csv_row, extracted_json, ",".join(specializations)),
        }

        # Combined confidence
        field_conf = {}
        validation_flags = {}
        missing_fields = []

        for field in fmt:
            final = FINAL_CONF_WEIGHTS["source"] * src.get(field, 0.0) + FINAL_CONF_WEIGHTS["format"] * fmt.get(field, 0.0)
            final = round(max(0.0, min(1.0, final)), 3)
            field_conf[field] = final

            if final < 0.5:
                validation_flags[field] = "low_confidence"
                if fmt[field] == 0.0:
                    validation_flags[field] += "|format_invalid"
                if src.get(field, 0.0) == 0.0:
                    validation_flags[field] += "|source_missing"
                if fmt[field] == 0.0 and src.get(field, 0.0) == 0.0:
                    missing_fields.append(field)

        # Weighted overall confidence
        weight_map = {
            "name": 0.25,
            "phone": 0.20,
            "email": 0.15,
            "address": 0.20,
            "registration": 0.10,
            "qualifications": 0.05
        }
        total_w = sum(weight_map.values())
        overall = sum(field_conf[k] * w for k, w in weight_map.items()) / total_w

        record = {
            "provider_id": provider_id,
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "source": {
                "csv_provided": bool(csv_row),
                "extracted_provided": bool(extracted_json),
                "raw_csv": csv_row or {},
                "raw_extracted": extracted_json or {},
            },
            "normalized": {
                "name": name,
                "phone": phone,
                "email": email,
                "address": address,
                "qualifications": qualifications,
                "registration_number": registration_number,
                "specializations": specializations,
                "experience_years": experience_years or "",
            },
            "format_confidence": fmt,
            "source_confidence": src,
            "field_confidence": field_conf,
            "validation_flags": validation_flags,
            "missing_fields": missing_fields,
            "overall_confidence": round(overall, 3),
            "status": "PENDING",
        }

        existing = self._load_existing()
        existing[provider_id] = record
        _atomic_write(self.validated_json_path, existing)

        logger.info("Validation complete for %s", provider_id)
        return record


# -------------------------------------------------------------
# CLI RUNNER
# -------------------------------------------------------------
def _load_json_file(path):
    if not path:
        return {}
    return json.load(open(path, "r", encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("provider_id")
    parser.add_argument("--csv")
    parser.add_argument("--extracted")
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args()

    agent = DataValidationAgent()

    if args.selftest:
        out = agent.self_test()
        print(json.dumps(out, indent=2))
        return

    csv_data = _load_json_file(args.csv) if args.csv else None
    extracted_data = _load_json_file(args.extracted) if args.extracted else None

    result = agent.run(args.provider_id, csv_data, extracted_data)

    if args.show:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
