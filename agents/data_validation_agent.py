
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from copy import deepcopy
from datetime import datetime
from typing import Dict, Any, Optional

# Try import project settings
try:
    from config.settings import SETTINGS
except Exception:
    class _S:
        DATA_PROCESSED_DIR = os.path.join("data", "processed")
        VALIDATED_JSON = os.path.join(DATA_PROCESSED_DIR, "validated_data.json")
        DEFAULT_REGION = "IN"  # for phonenumbers parse (India)
        FINAL_CONF_WEIGHTS = {"source": 0.5, "format": 0.5}
    SETTINGS = _S()

# Ensure processed dir exists
os.makedirs(getattr(SETTINGS, "DATA_PROCESSED_DIR", os.path.join("data", "processed")), exist_ok=True)

# fallback CONSTANTS
VALIDATED_JSON_PATH = getattr(SETTINGS, "VALIDATED_JSON", os.path.join(SETTINGS.DATA_PROCESSED_DIR, "validated_data.json"))
FINAL_CONF_WEIGHTS = getattr(SETTINGS, "FINAL_CONF_WEIGHTS", {"source": 0.5, "format": 0.5})
DEFAULT_REGION = getattr(SETTINGS, "DEFAULT_REGION", "IN")

# regexes
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
REGNO_RE = re.compile(r"[A-Za-z0-9\/\-\s]+")
YEAR_RE = re.compile(r"(\d{1,2})\s*(?:yrs?|years?)", re.IGNORECASE)
DIGITS_RE = re.compile(r"\d+")

# logger fallback
try:
    from utils.logger import get_logger
    logger = get_logger(__name__)
except Exception:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("data_validation_agent")

# optional phonenumbers
try:
    import phonenumbers  # type: ignore
    PHONENUMBERS_AVAILABLE = True
except Exception:
    PHONENUMBERS_AVAILABLE = False


# Normalizers (safe fallbacks)
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
    # keep plus and digits, otherwise digits only
    s = str(phone).strip()
    if "+" in s:
        s = "+" + "".join(re.findall(r"\d+", s))
    else:
        s = "".join(re.findall(r"\d+", s))
        # if local 10-digit and DEFAULT_REGION set, we may format later via phonenumbers
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
    parts = [p.strip().upper().replace(".", "") for p in re.split(r"[,/;]+", str(q)) if p.strip()]
    return parts


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
            except Exception:
                continue
    m = YEAR_RE.search(str(exp))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return None


# Helper: atomic write
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
            except Exception:
                pass


# Main Agent
class DataValidationAgent:
    def __init__(self, validated_json_path: str = VALIDATED_JSON_PATH, default_region: str = DEFAULT_REGION):
        self.validated_json_path = validated_json_path
        self.default_region = default_region

    # Load existing records
    def _load_existing(self) -> Dict[str, Any]:
        if not os.path.exists(self.validated_json_path):
            return {}
        try:
            with open(self.validated_json_path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception as e:
            logger.warning("Failed to load existing validated JSON (%s). Starting fresh. Error: %s", self.validated_json_path, e)
            return {}

    # Format validation (lib/regex)
    
    def _format_phone_confidence(self, phone: str) -> float:
        """Use phonenumbers when available for stronger format confidence"""
        if not phone:
            return 0.0
        # try parse with phonenumbers for stronger signal
        if PHONENUMBERS_AVAILABLE:
            try:
                # ensure leading + if present; else parse with default region
                if phone.startswith("+"):
                    parsed = phonenumbers.parse(phone, None)
                else:
                    parsed = phonenumbers.parse(phone, self.default_region)
                if phonenumbers.is_valid_number(parsed):
                    return 1.0
                if phonenumbers.is_possible_number(parsed):
                    return 0.8
            except Exception:
                pass
        # fallback heuristics: digits count
        digits = re.sub(r"\D", "", phone)
        if 10 <= len(digits) <= 15:
            return 0.8
        if 7 <= len(digits) < 10:
            return 0.5
        return 0.0

    def _format_email_confidence(self, email: str) -> float:
        if not email:
            return 0.0
        email = email.strip()
        if EMAIL_RE.match(email):
            return 1.0
        # contains basic structure
        if "@" in email and "." in email.split("@")[-1]:
            return 0.6
        return 0.0

    def _format_name_confidence(self, name: str) -> float:
        if not name:
            return 0.0
        parts = name.split()
        if len(parts) >= 2 and all(len(p) > 1 for p in parts[:2]):
            return 1.0
        return 0.6

    def _format_address_confidence(self, address: str) -> float:
        if not address:
            return 0.0
        address = address.strip()
        score = 0.0
        street_keywords = ["street", "st", "road", "rd", "lane", "ln", "avenue", "ave", "block", "sector", "sector-"]
        if any(k in address.lower() for k in street_keywords):
            score += 0.5
        if re.search(r"\d{1,5}", address):
            score += 0.3
        if "," in address:
            score += 0.2
        return min(score, 1.0)

    def _format_registration_confidence(self, reg: str) -> float:
        if not reg:
            return 0.0
        r = reg.strip()
        if re.search(r"[A-Z]{1,5}\s*[-]?\s*\d{2,8}", r, re.IGNORECASE):
            return 1.0
        if any(ch.isdigit() for ch in r) and any(ch.isalpha() for ch in r):
            return 0.6
        return 0.0

    # Source confidence (CSV vs PDF)
    def _source_confidence_for_field(self, field_name: str, csv_row: Optional[Dict[str, Any]], pdf_json: Optional[Dict[str, Any]], normalized_value) -> float:
        """
        Determine confidence coming from source agreement:
        - both present & equal -> strong (0.95)
        - both present & different -> low (0.40)
        - csv only -> medium-high (0.80)
        - pdf only -> medium (0.60)
        - none -> 0.0
        """
        csv_val = None
        pdf_val = None
        if csv_row:
            csv_val = csv_row.get(field_name)
        if pdf_json:
            # accept various keys for pdf
            # pdf extractor may use 'mobile' or 'phone'; normalize search
            possible_pdf_keys = [field_name, field_name.replace("clinic_", ""), "mobile", "contact", "phone_number"]
            for k in possible_pdf_keys:
                if k in pdf_json and pdf_json.get(k) not in (None, "", []):
                    pdf_val = pdf_json.get(k)
                    break

        # Normalized string compare if possible
        def norm(x):
            if x is None:
                return None
            try:
                return str(x).strip().lower()
            except:
                return None

        n_csv = norm(csv_val)
        n_pdf = norm(pdf_val)
        n_norm = norm(normalized_value)

        # Both present
        if n_csv and n_pdf:
            if n_csv == n_pdf:
                return 0.95
            # also if one of them equals the normalized value, treat as partial match
            if n_csv == n_norm or n_pdf == n_norm:
                return 0.85
            return 0.40
        # CSV only
        if n_csv and not n_pdf:
            # CSV is present; if it equals normalized value -> trust more
            if n_csv == n_norm:
                return 0.88
            return 0.80
        # PDF only
        if n_pdf and not n_csv:
            if n_pdf == n_norm:
                return 0.75
            return 0.60
        return 0.0

    # Main run
    def run(self, provider_id: str, csv_row: Optional[Dict[str, Any]] = None, extracted_json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Validate a single provider. Writes/updates validated JSON atomically.
        Returns the validated record.
        """
        logger.info("Running validation for provider_id=%s", provider_id)
        # Merge preference: extracted_json then csv (we will inspect both separately for source_confidence)
        source = {}
        if extracted_json:
            source.update(deepcopy(extracted_json))
        if csv_row:
            source.update(deepcopy(csv_row))

        # get raw fields (robust keys)
        def get_field(keys, default=""):
            for k in keys:
                if k in source and source[k] not in (None, ""):
                    return source[k]
            return default

        raw_name = get_field(["name", "provider_name", "full_name"])
        raw_phone = get_field(["phone", "phone_number", "contact", "mobile"])
        raw_email = get_field(["email", "email_address", "contact_email"])
        raw_address = get_field(["address", "location", "addr", "clinic_address"])
        raw_qual = get_field(["qualifications", "qualification", "degree"])
        raw_reg = get_field(["registration_number", "registration", "reg_no", "regnum"])
        raw_specs = get_field(["specializations", "speciality", "specialties"])
        raw_exp = get_field(["experience_years", "experience", "exp"])

        # normalize
        name = normalize_name(raw_name)
        phone = normalize_phone(raw_phone)
        email = normalize_email(raw_email)
        address = normalize_address(raw_address)
        qualifications = normalize_qualifications(raw_qual) if raw_qual else []
        registration_number = normalize_registration(raw_reg)
        specializations = raw_specs if isinstance(raw_specs, list) else ([s.strip() for s in re.split(r"[,/;]+", str(raw_specs)) if s.strip()] if raw_specs else [])
        experience_years = extract_experience_years(raw_exp) if raw_exp else None

        # format confidences
        format_conf = {}
        format_conf["name"] = self._format_name_confidence(name)
        format_conf["phone"] = self._format_phone_confidence(phone)
        format_conf["email"] = self._format_email_confidence(email)
        format_conf["address"] = self._format_address_confidence(address)
        format_conf["registration"] = self._format_registration_confidence(registration_number)
        format_conf["qualifications"] = 1.0 if qualifications else 0.0
        format_conf["experience_years"] = 1.0 if experience_years is not None else 0.0
        format_conf["specializations"] = 1.0 if specializations else 0.0

        # source confidences (csv vs pdf)
        source_conf = {}
        source_conf["name"] = self._source_confidence_for_field("name", csv_row, extracted_json, name)
        source_conf["phone"] = self._source_confidence_for_field("phone", csv_row, extracted_json, phone)
        source_conf["email"] = self._source_confidence_for_field("email", csv_row, extracted_json, email)
        source_conf["address"] = self._source_confidence_for_field("clinic_address", csv_row, extracted_json, address)
        source_conf["registration"] = self._source_confidence_for_field("registration_number", csv_row, extracted_json, registration_number)
        source_conf["qualifications"] = self._source_confidence_for_field("qualifications", csv_row, extracted_json, ",".join(qualifications))
        source_conf["experience_years"] = self._source_confidence_for_field("experience_years", csv_row, extracted_json, str(experience_years) if experience_years is not None else "")
        source_conf["specializations"] = self._source_confidence_for_field("specializations", csv_row, extracted_json, ",".join(specializations))

        # combined per-field confidence and flags
        field_confidence = {}
        validation_flags = {}
        missing_fields = []

        for field in format_conf.keys():
            fmt = float(format_conf.get(field, 0.0))
            src = float(source_conf.get(field, 0.0))
            # final per-field score: weighted combination (default equal)
            w_source = FINAL_CONF_WEIGHTS.get("source", 0.5)
            w_format = FINAL_CONF_WEIGHTS.get("format", 0.5)
            final_field_score = float(w_source * src + w_format * fmt)
            # keep in 0..1
            final_field_score = max(0.0, min(1.0, final_field_score))
            field_confidence[field] = round(final_field_score, 3)

            # flags
            if final_field_score < 0.5:
                validation_flags[field] = "low_confidence"
                if fmt == 0.0:
                    validation_flags[field] = validation_flags[field] + "|format_invalid"
                if src == 0.0:
                    validation_flags[field] = validation_flags[field] + "|source_missing"
                # record missing if completely absent
                if src == 0.0 and fmt == 0.0:
                    missing_fields.append(field)

        # overall confidence: weighted average using a sensible weight map (name, phone, email, address, registration, qualifications)
        weight_map = {"name": 0.25, "phone": 0.2, "email": 0.15, "address": 0.2, "registration": 0.1, "qualifications": 0.05}
        total_w = sum(weight_map.values())
        overall = 0.0
        for k, w in weight_map.items():
            overall += field_confidence.get(k, 0.0) * w
        overall_confidence = float(overall / total_w) if total_w > 0 else 0.0

        # validation record (structure kept compatible)
        validated_record = {
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
                "experience_years": experience_years if experience_years is not None else "",
            },
            "format_confidence": format_conf,
            "source_confidence": source_conf,
            "field_confidence": field_confidence,
            "validation_flags": validation_flags,
            "missing_fields": missing_fields,
            "overall_confidence": round(overall_confidence, 3),
            "status": "PENDING",
        }

        # persist (merge with existing)
        existing = self._load_existing()
        existing[provider_id] = validated_record
        _atomic_write(self.validated_json_path, existing)

        logger.info("Validation complete for %s (overall_conf=%.3f)", provider_id, overall_confidence)
        return validated_record

    # Quick self-test helper
    def self_test(self):
        # Small sanity check: demonstrate behavior on a few synthetic examples
        samples = [
            ("CSV+PDF MATCH", {"phone": "9876543210"}, {"mobile": "+91-9876543210"}),
            ("PDF only local", None, {"mobile": "6202775969"}),
            ("CSV only", {"phone": "+91 9988776655"}, None),
            ("Mismatch", {"phone": "9876543210"}, {"mobile": "9876543200"}),
        ]
        results = {}
        for name, csv, pdf in samples:
            pid = f"SELF_{name.replace(' ', '_')}"
            r = self.run(pid, csv_row=csv, extracted_json=pdf)
            results[pid] = r
        return results


# CLI runner
def _load_json_file(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(prog="data_validation_agent", description="Run DataValidationAgent for a provider.")
    parser.add_argument("provider_id", help="Provider unique id (string).")
    parser.add_argument("--csv", "-c", help="Path to CSV row JSON file (optional).", default=None)
    parser.add_argument("--extracted", "-e", help="Path to extracted JSON file (optional).", default=None)
    parser.add_argument("--show", action="store_true", help="Print validated output to stdout after run.")
    parser.add_argument("--selftest", action="store_true", help="Run built-in self-tests.")
    args = parser.parse_args()

    agent = DataValidationAgent()

    if args.selftest:
        results = agent.self_test()
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return

    csv_data = _load_json_file(args.csv) if args.csv else None
    extracted_data = _load_json_file(args.extracted) if args.extracted else None

    result = agent.run(args.provider_id, csv_row=csv_data, extracted_json=extracted_data)
    if args.show:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
