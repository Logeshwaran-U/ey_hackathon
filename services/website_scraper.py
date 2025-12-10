# services/website_scraper.py
"""
Option A — Modern structured website scraper (one-file).
Output JSON shape (recommended):
{
  "provider_id": "P001",
  "url": "https://example.com",
  "status": "ok" | "fail" | "invalid_or_parked",
  "signals": {
      "is_js_heavy": False,
      "is_parked_likely": False,
      "parked_indicators": [],
      "not_medical_site": False
  },
  "hospital": { "emails": [], "phones": [], "addresses": [], "score": 0.0 },
  "doctor": {
      "name": "Dr X",
      "doctor_page_found": True,
      "doctor_pages": [],
      "doctor_emails": [],
      "doctor_phones": [],
      "structured_profile": {},
      "score": 0.0
  },
  "website_trust_score": 0.0
}
"""

from __future__ import annotations

import re
import json
import time
import random
import logging
import argparse
import traceback
import requests
import urllib.parse
import urllib.robotparser
from bs4 import BeautifulSoup
from pathlib import Path
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from xml.etree import ElementTree as ET

logger = logging.getLogger("WebsiteScraper")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class WebsiteScraper:
    USER_AGENT = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # strict-ish phone regex for captured E.164-ish segments (we keep flexible)
    STRICT_PHONE_RE = re.compile(r"(?:\+?\d[\d\-\s().]{6,}\d)")
    EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
    PARKED_KEYWORDS = [
        "parked", "buy this domain", "this domain is for sale", "domain parking",
        "under construction", "coming soon", "sitebuilder", "website builder", "template"
    ]

    ADDRESS_KEYWORDS = [
        "street", "st", "road", "rd", "avenue", "ave", "colony", "sector",
        "lane", "block", "suite", "hospital", "clinic", "city", "zip", "zipcode", "state"
    ]

    DOCTOR_PAGE_KEYWORDS = [
        "doctor", "provider", "physician", "team", "profile", "specialist", "staff", "doctors", "our-doctors"
    ]

    SAFE_DISCOVERY_PATHS = ["/doctors", "/our-doctors", "/team", "/our-team", "/physicians", "/providers"]
    MAX_CRAWL_PAGES = 6
    REQUEST_TIMEOUT = 8
    MAX_HTML_SIZE = 2_000_000
    SITEMAP_MAX_URLS = 200

    def __init__(self, session: Optional[requests.Session] = None):
        self.domain_keepalive: Dict[str, bool] = {}
        self.session = session or self._build_session()

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        retries = Retry(
            total=3,
            connect=3,
            read=2,
            status=2,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update(self.USER_AGENT)
        s.trust_env = True
        return s

    def _allowed_by_robots(self, url: str) -> bool:
        try:
            parsed = urllib.parse.urlparse(url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(urllib.parse.urljoin(base, "/robots.txt"))
            try:
                rp.read()
            except Exception:
                # permissive fallback: treat as allowed if robots can't be read
                return True
            return rp.can_fetch(self.USER_AGENT["User-Agent"], url)
        except Exception:
            return True

    def _domain_key(self, url: str) -> str:
        try:
            return urllib.parse.urlparse(url).netloc.lower()
        except Exception:
            return url

    def _fetch_html(self, url: str, max_attempts: int = 3) -> Dict[str, Any]:
        if not self._allowed_by_robots(url):
            return {"ok": False, "html": "", "status_code": None, "error": "Blocked by robots.txt"}

        domain = self._domain_key(url)
        allow_keepalive = self.domain_keepalive.get(domain, True)
        attempt = 0
        last_exc = None

        while attempt < max_attempts:
            attempt += 1
            try:
                headers = self.USER_AGENT.copy()
                if not allow_keepalive:
                    headers["Connection"] = "close"
                resp = self.session.get(url, timeout=self.REQUEST_TIMEOUT, headers=headers, stream=False)
                if resp.status_code != 200:
                    return {"ok": False, "html": "", "status_code": resp.status_code, "error": f"HTTP {resp.status_code}"}
                html = resp.text or ""
                if len(html) > self.MAX_HTML_SIZE:
                    html = html[: self.MAX_HTML_SIZE]
                return {"ok": True, "html": html, "status_code": resp.status_code, "error": None}
            except requests.exceptions.ConnectionError as e:
                last_exc = e
                logger.warning("ConnectionError %s (attempt %d): %s", url, attempt, e)
                if attempt == 1:
                    self.domain_keepalive[domain] = False
            except requests.exceptions.ReadTimeout as e:
                last_exc = e
                logger.warning("ReadTimeout %s (attempt %d): %s", url, attempt, e)
            except Exception as e:
                last_exc = e
                logger.exception("Exception fetching %s (attempt %d): %s", url, attempt, e)

            time.sleep((2 ** (attempt - 1)) * 0.4 + random.uniform(0, 0.25))

        return {"ok": False, "html": "", "status_code": None, "error": str(last_exc), "trace": traceback.format_exception_only(type(last_exc), last_exc) if last_exc else []}

    def _bs4_soup(self, html: str) -> BeautifulSoup:
        try:
            return BeautifulSoup(html, "lxml")
        except Exception:
            return BeautifulSoup(html, "html.parser")

    def _visible_text(self, soup: BeautifulSoup) -> str:
        # remove script/style/svg/iframe to avoid junk
        for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
            tag.decompose()
        return soup.get_text("\n", strip=True)

    def _extract_emails(self, text: str) -> List[str]:
        return list(dict.fromkeys(re.findall(self.EMAIL_RE, text)))

    def _extract_phones(self, text: str) -> List[str]:
        raw = re.findall(self.STRICT_PHONE_RE, text)
        cleaned = []
        seen = set()
        for m in raw:
            s = re.sub(r"[^\d+]", "", m)
            # keep plausible lengths (7..15 digits)
            digits = re.sub(r"\D", "", s)
            if 7 <= len(digits) <= 15:
                # normalize: prefer leading + when available
                if s.startswith("+"):
                    val = "+" + digits
                elif len(digits) == 10:
                    # heuristic: local 10-digit -> do not force country (leave digits)
                    val = digits
                else:
                    val = "+" + digits
                if val not in seen:
                    seen.add(val)
                    cleaned.append(val)
        return cleaned

    def _extract_addresses(self, text: str) -> List[str]:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        candidates = []
        for ln in lines:
            low = ln.lower()
            if (any(tok in low for tok in self.ADDRESS_KEYWORDS) or (re.search(r"\d{3,}", ln) and "," in ln)) and 10 <= len(ln) <= 400:
                candidates.append(ln)
        # dedupe preserving order
        out = []
        seen = set()
        for a in candidates:
            if a not in seen:
                seen.add(a)
                out.append(a)
        return out

    def _looks_js_heavy(self, html: str) -> bool:
        low = html.lower()
        if len(html) < 200:
            return True
        if "var __NEXT_DATA__" in low or "window.__INITIAL_STATE__" in low or low.count("<script") > 20:
            return True
        return False

    def _detect_doctor_links(self, soup: BeautifulSoup, base_url: str, doctor_name: Optional[str]) -> List[str]:
        anchors = soup.find_all("a", href=True)
        pages = []
        tokens = []
        if doctor_name:
            key = doctor_name.lower().replace("dr.", "").replace("dr ", "").strip()
            key = re.sub(r"[^a-z0-9\s\-]", " ", key)
            parts = [p for p in key.split() if p]
            if parts:
                tokens = {parts[0], parts[-1], "".join(parts), "-".join(parts)}
        for a in anchors:
            href = a["href"]
            resolved = urllib.parse.urljoin(base_url, href)
            href_l = resolved.lower()
            txt = (a.get_text(" ", strip=True) or "").lower()
            matches_keyword = any(k in href_l or k in txt for k in self.DOCTOR_PAGE_KEYWORDS)
            matches_token = any(t in href_l or t in txt for t in tokens) if tokens else False
            if matches_keyword or matches_token:
                pages.append(resolved)
        # dedupe and limit
        seen = set(); out = []
        for p in pages:
            if p not in seen:
                seen.add(p); out.append(p)
        return out[: self.MAX_CRAWL_PAGES]

    def _safe_discover_doctor(self, base_url: str, doctor_name: str) -> Optional[str]:
        parsed = urllib.parse.urlparse(base_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for p in self.SAFE_DISCOVERY_PATHS:
            candidate = urllib.parse.urljoin(base, p)
            fetched = self._fetch_html(candidate)
            if not fetched.get("ok"):
                continue
            soup = self._bs4_soup(fetched["html"])
            pages = self._detect_doctor_links(soup, base, doctor_name)
            if pages:
                # validate first candidate by checking name presence
                for page in pages:
                    pf = self._fetch_html(page)
                    if pf.get("ok"):
                        text = self._visible_text(self._bs4_soup(pf["html"])).lower()
                        if doctor_name.lower().replace("dr.", "").strip() in text:
                            return page
                # if none validated, return first candidate as best-effort
                return pages[0]
        return None

    def _sitemap_discover(self, base_url: str, doctor_name: str) -> Optional[str]:
        try:
            parsed = urllib.parse.urlparse(base_url)
            sitemap_url = urllib.parse.urljoin(f"{parsed.scheme}://{parsed.netloc}", "/sitemap.xml")
            fetched = self._fetch_html(sitemap_url)
            if not fetched.get("ok"):
                return None
            xml = fetched["html"]
            root = ET.fromstring(xml)
            urls = []
            # look for loc elements
            for url_el in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc") or root.findall(".//loc"):
                if url_el is not None and url_el.text:
                    u = url_el.text.strip()
                    if len(urls) >= self.SITEMAP_MAX_URLS:
                        break
                    if any(k in u.lower() for k in ("doctor", "doctors", "profile", "physician")):
                        urls.append(u)
            for u in urls:
                pf = self._fetch_html(u)
                if pf.get("ok"):
                    text = self._visible_text(self._bs4_soup(pf["html"])).lower()
                    if doctor_name.lower().replace("dr.", "").strip() in text:
                        return u
        except Exception:
            return None
        return None

    def _parked_signals(self, text: str, html: str) -> (bool, List[str]):
        indicators = []
        low = (text + " " + (html[:1000] if html else "")).lower()
        for k in self.PARKED_KEYWORDS:
            if k in low:
                indicators.append(k)
        return (len(indicators) > 0, indicators)

    def extract_doctor_profile_structured(self, html: str) -> Optional[Dict[str, Any]]:
        soup = self._bs4_soup(html)
        # remove scripts/styles
        for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
            tag.decompose()

        headings = soup.find_all(re.compile("^h[1-6]$"))
        candidate_nodes = []
        keywords = ("qualification", "education", "experience", "about", "profile", "bio", "career", "degree")
        for h in headings:
            htext = (h.get_text(" ", strip=True) or "").lower()
            if any(k in htext for k in keywords):
                candidate_nodes.append(h.find_parent(["section", "article", "div"]) or h.parent)
        if not candidate_nodes:
            for tag in soup.find_all(["p", "div", "section"]):
                txt = (tag.get_text(" ", strip=True) or "").lower()
                if any(k in txt for k in ("qualification", "education", "experience", "degree")):
                    candidate_nodes.append(tag)
                    if len(candidate_nodes) >= 3:
                        break
        degrees = []; colleges = []; experiences = []; location = ""
        degree_re = re.compile(r"\b(MBBS|MD|DO|DNB|M\.D\.|Fellowship|Residency|PhD|MRCP|FRCS)\b", re.IGNORECASE)
        college_re = re.compile(r"(college|university|institute|hospital|medical college|academy)", re.IGNORECASE)
        exp_re = re.compile(r"(year|yrs|experience|since|\d{4})", re.IGNORECASE)

        seen = set()
        for node in candidate_nodes:
            text = node.get_text("\n", strip=True)
            for ln in [l.strip() for l in text.splitlines() if l.strip()]:
                low = ln.lower()
                if ln in seen:
                    continue
                seen.add(ln)
                if degree_re.search(ln):
                    if ln not in degrees:
                        degrees.append(ln)
                if college_re.search(ln):
                    cleaned = re.sub(r"(?i)\b(qualification|education|degree|studied at|studies at)\b", "", ln).strip(" -,:;•")
                    if cleaned and cleaned not in colleges:
                        colleges.append(cleaned)
                if exp_re.search(ln) or any(w in low for w in ("consultant", "practice", "joined", "worked")):
                    if ln not in experiences:
                        experiences.append(ln)
        def compact(lst: List[str]) -> List[str]:
            out = []
            for v in lst:
                v2 = re.sub(r"\s{2,}", " ", v).strip()
                v2 = re.sub(r"<[^>]+>", "", v2)
                v2 = v2.strip()
                if v2 and v2 not in out:
                    out.append(v2)
            return out

        degrees = compact(degrees)
        colleges = compact(colleges)
        experiences = compact(experiences)
        if degrees or colleges or experiences or location:
            return {
                "qualifications": degrees,
                "colleges": colleges,
                "work_experience": experiences,
                "working_location": location
            }
        return None

    def score_website_trust(self, hospital_score: float, doctor_score: float, parked_flag: bool, js_heavy: bool) -> float:
        # simple weighted aggregator; adjustable
        score = 0.0
        score += hospital_score * 0.6
        score += doctor_score * 0.3
        if parked_flag:
            score -= 0.5
        if js_heavy:
            score -= 0.05
        return max(0.0, min(1.0, score))

    def scrape(self, url: str, provider_id: str,
               doctor_name: Optional[str] = None,
               specialization: Optional[str] = None,
               save: bool = False) -> Dict[str, Any]:

        out = {
            "provider_id": provider_id,
            "url": url,
            "status": "fail",
            "signals": {
                "is_js_heavy": False,
                "is_parked_likely": False,
                "parked_indicators": [],
                "not_medical_site": False
            },
            "hospital": {"emails": [], "phones": [], "addresses": [], "score": 0.0},
            "doctor": {
                "name": doctor_name or "",
                "doctor_page_found": False,
                "doctor_pages": [],
                "doctor_emails": [],
                "doctor_phones": [],
                "structured_profile": None,
                "score": 0.0
            },
            "website_trust_score": 0.0
        }

        fetched = self._fetch_html(url)
        if not fetched.get("ok"):
            out["status"] = "fail"
            out["error"] = fetched.get("error")
            out["trace"] = fetched.get("trace", [])
            return out

        html = fetched["html"]
        soup = self._bs4_soup(html)
        text = self._visible_text(soup)

        # signals
        js_heavy = self._looks_js_heavy(html)
        parked_flag, parked_indicators = self._parked_signals(text, html)
        out["signals"]["is_js_heavy"] = js_heavy
        out["signals"]["is_parked_likely"] = parked_flag
        out["signals"]["parked_indicators"] = parked_indicators

        # extract hospital-level info
        emails = self._extract_emails(text)
        phones = self._extract_phones(text)
        addresses = self._extract_addresses(text)

        out["hospital"]["emails"] = emails
        out["hospital"]["phones"] = phones
        out["hospital"]["addresses"] = addresses
        hospital_score = 0.0
        if emails or phones or addresses:
            hospital_score = min(1.0, (0.33 if emails else 0) + (0.34 if phones else 0) + (0.33 if addresses else 0))
        out["hospital"]["score"] = round(hospital_score, 3)

        # Doctor discovery
        doctor_pages = self._detect_doctor_links(soup, url, doctor_name) if doctor_name else []
        # safe discovery if none found on homepage
        if doctor_name and not doctor_pages:
            discovered = self._safe_discover_doctor(url, doctor_name)
            if discovered:
                doctor_pages = [discovered]
        # sitemap fallback
        if doctor_name and not doctor_pages:
            sm = self._sitemap_discover(url, doctor_name)
            if sm:
                doctor_pages = [sm]

        doctor_found = False
        doc_emails = []
        doc_phones = []
        structured_profile = None
        spec_match = False
        name_match = False

        for p in doctor_pages:
            pf = self._fetch_html(p)
            if not pf.get("ok"):
                continue
            doc_html = pf["html"]
            doc_soup = self._bs4_soup(doc_html)
            doc_text = self._visible_text(doc_soup).lower()
            if doctor_name and doctor_name.lower().replace("dr.", "").strip() in doc_text:
                name_match = True
                doctor_found = True
            if specialization and specialization.lower() in doc_text:
                spec_match = True
            doc_emails += self._extract_emails(doc_text)
            doc_phones += self._extract_phones(doc_text)
            if not structured_profile:
                structured_profile = self.extract_doctor_profile_structured(doc_html)

        doc_emails = list(dict.fromkeys(doc_emails))
        doc_phones = list(dict.fromkeys(doc_phones))

        doctor_score = 0.0
        if doctor_name:
            doctor_score = min(1.0, (0.5 if doctor_found else 0) + (0.3 if name_match else 0) + (0.2 if spec_match else 0))

        out["doctor"].update({
            "doctor_page_found": doctor_found,
            "doctor_pages": doctor_pages,
            "doctor_emails": doc_emails,
            "doctor_phones": doc_phones,
            "structured_profile": structured_profile,
            "score": round(doctor_score, 3)
        })

        out["signals"]["not_medical_site"] = False
        # Simple heuristic: if neither hospital signals nor doctor signals, mark maybe not medical
        if hospital_score < 0.1 and doctor_score < 0.1:
            out["signals"]["not_medical_site"] = True

        # compute overall trust
        trust = self.score_website_trust(hospital_score, doctor_score, parked_flag, js_heavy)
        out["website_trust_score"] = round(trust, 3)

        # status decisions (do not drop/throw away working sites)
        # if parked_flag strong and trust very low, tag as parked but still return data
        if parked_flag and trust < 0.15:
            out["status"] = "invalid_or_parked"
        else:
            out["status"] = "ok"

        # optional save
        if save:
            out_dir = Path("data/processed/website_scrape")
            out_dir.mkdir(parents=True, exist_ok=True)
            with open(out_dir / f"{provider_id}.json", "w", encoding="utf-8") as fh:
                json.dump(out, fh, indent=2, ensure_ascii=False)

        return out


# CLI helper
def main():
    parser = argparse.ArgumentParser(description="WebsiteScraper (Option A structured output)")
    parser.add_argument("--url", required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--name", default=None)
    parser.add_argument("--spec", default=None)
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()

    scraper = WebsiteScraper()
    out = scraper.scrape(args.url, args.provider, args.name, args.spec, args.save)
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
