# services/website_scraper.py
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
from bs4 import BeautifulSoup, Tag
from pathlib import Path
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("WebsiteScraper")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class WebsiteScraper:
    USER_AGENT = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    PHONE_RE = re.compile(r"(?:\+?\d{1,3}[-\s.]?)?(?:\(?\d{2,5}\)?[-\s.]?)?\d{3,4}[-\s.]?\d{3,4}")
    EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
    ADDRESS_KEYWORDS = [
        "road", "street", "st", "avenue", "colony", "sector",
        "lane", "block", "district", "city", "india", "usa", "patna"
    ]

    MAX_CRAWL_PAGES = 5
    REQUEST_TIMEOUT = 10
    MAX_HTML_SIZE = 2_000_000

    # common degree keywords (expand as needed)
    DEGREE_KEYWORDS = ["MBBS", "MD", "DNB", "MS", "DO", "M.D.", "M.B.B.S", "Fellowship", "Diploma"]

    def __init__(self, session: Optional[requests.Session] = None):
        # keep per-domain keepalive control: domain -> bool (True means allow keepalive)
        self.domain_keepalive: Dict[str, bool] = {}
        self.session = session or self._build_session()

    def _build_session(self) -> requests.Session:
        """
        Build a requests.Session with robust Retry settings (including connect retries).
        """
        s = requests.Session()
        retries = Retry(
            total=3,
            connect=3,     # retry on connection errors
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update(self.USER_AGENT)
        # trust_env True uses system proxies; set False if proxies interfere
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
                # be permissive if robots can't be fetched (optional: change behavior)
                return True
            return rp.can_fetch(self.USER_AGENT["User-Agent"], url)
        except Exception:
            return True

    def _domain_key(self, url: str) -> str:
        """
        Simple domain key for domain-specific heuristics.
        """
        try:
            p = urllib.parse.urlparse(url)
            return p.netloc.lower()
        except Exception:
            return url

    def _fetch_html(self, url: str, max_attempts: int = 4) -> Dict[str, Any]:
        """
        Robust fetch with per-domain connection-control and jittered backoff.
        Returns dict: {'ok', 'html', 'status_code', 'error', 'trace'}
        """
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
                # If server returns non-200 code, return that as error (no further retry by us)
                if resp.status_code != 200:
                    return {"ok": False, "html": "", "status_code": resp.status_code, "error": f"HTTP {resp.status_code}"}
                html = resp.text or ""
                if len(html) > self.MAX_HTML_SIZE:
                    html = html[:self.MAX_HTML_SIZE]
                return {"ok": True, "html": html, "status_code": resp.status_code, "error": None}
            except requests.exceptions.ConnectionError as e:
                # often wraps ConnectionResetError (10054)
                last_exc = e
                logger.warning("ConnectionError for %s (attempt %d/%d): %s", url, attempt, max_attempts, e)
                # heuristic: disable keepalive for this domain after first connection reset
                if attempt == 1:
                    logger.debug("Disabling keepalive for domain %s due to ConnectionError", domain)
                    self.domain_keepalive[domain] = False
            except requests.exceptions.ReadTimeout as e:
                last_exc = e
                logger.warning("ReadTimeout for %s (attempt %d/%d): %s", url, attempt, max_attempts, e)
            except requests.exceptions.RequestException as e:
                last_exc = e
                logger.warning("RequestException for %s (attempt %d/%d): %s", url, attempt, max_attempts, e)
            except Exception as e:
                last_exc = e
                logger.exception("Unexpected exception fetching %s (attempt %d/%d)", url, attempt, max_attempts)

            # Exponential backoff + jitter
            backoff = (2 ** (attempt - 1)) * 0.5
            jitter = random.uniform(0, 0.5)
            sleep_time = backoff + jitter
            logger.debug("Sleeping %.2fs before retrying %s", sleep_time, url)
            time.sleep(sleep_time)

        # attempts exhausted
        tb = traceback.format_exception_only(type(last_exc), last_exc) if last_exc else []
        return {"ok": False, "html": "", "status_code": None, "error": str(last_exc), "trace": tb}

    def _bs4_soup(self, html: str) -> BeautifulSoup:
        try:
            return BeautifulSoup(html, "lxml")
        except Exception:
            return BeautifulSoup(html, "html.parser")

    def _extract_emails(self, text: str) -> List[str]:
        return list(dict.fromkeys(re.findall(self.EMAIL_RE, text)))

    def _extract_phones(self, text: str) -> List[str]:
        raw = re.findall(self.PHONE_RE, text)
        cleaned = []
        seen = set()
        for num in raw:
            digits = re.sub(r"\D", "", num)
            if 7 <= len(digits) <= 15:
                if len(digits) == 10:
                    fmt = "+91" + digits
                else:
                    fmt = "+" + digits
                if fmt not in seen:
                    seen.add(fmt)
                    cleaned.append(fmt)
        return cleaned

    def _extract_addresses(self, text: str) -> List[str]:
        lines = text.splitlines()
        addrs = []
        for ln in lines:
            ln_s = ln.strip()
            low = ln_s.lower()
            if any(k in low for k in self.ADDRESS_KEYWORDS) and 15 <= len(ln_s) <= 300:
                addrs.append(ln_s)
        return list(dict.fromkeys(addrs))

    def _detect_doctor_pages(self, soup: BeautifulSoup, doctor_name: str) -> List[str]:
        # Return raw hrefs (the caller resolves relative URLs using the page URL)
        if not doctor_name:
            return []
        key = doctor_name.lower().replace("dr.", "").replace("dr ", "").strip()
        first = key.split()[0] if key else ""
        pages = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_l = href.lower()
            txt = (a.get_text() or "").lower()
            if any(w in href_l for w in ["doctor", "team", "profile", "physician", "our-doctors"]):
                if first and (first in href_l or first in txt):
                    pages.append(href)
            elif key in txt:
                pages.append(href)
        # dedupe while preserving order
        seen = set(); out = []
        for p in pages:
            if p not in seen:
                seen.add(p); out.append(p)
        return out[:self.MAX_CRAWL_PAGES]

    def _looks_js_heavy(self, html: str) -> bool:
        if len(html) < 200 or "<script" in html.lower():
            return True
        return False

    # -----------------------
    # NEW: structured doctor profile extractor
    # -----------------------
    def extract_doctor_profile_structured(self, doc_html: str) -> Optional[Dict[str, Any]]:
        soup = self._bs4_soup(doc_html)

        headings = soup.find_all(re.compile("^h[1-6]$"))
        keywords = ("qualification", "education", "experience", "about", "profile", "bio", "career")
        candidate_nodes = []

        for h in headings:
            h_text = (h.get_text(" ", strip=True) or "").lower()
            if any(k in h_text for k in keywords):
                parent = h.find_parent(["section", "article", "div"]) or h.parent
                candidate_nodes.append(parent)

        if not candidate_nodes:
            for tag in soup.find_all(["p", "div", "section"]):
                txt = (tag.get_text(" ", strip=True) or "").lower()
                if "qualification" in txt or "qualification:" in txt or "experience" in txt:
                    candidate_nodes.append(tag)
                    if len(candidate_nodes) >= 3:
                        break

        if not candidate_nodes:
            main_content = soup.find("main") or soup.body
            if main_content:
                candidate_nodes.append(main_content)

        degrees = []
        colleges = []
        experiences = []
        working_location = None

        def clean_text_from_node(node: Tag) -> str:
            for s in node(["script", "style", "noscript"]):
                s.decompose()
            text = node.get_text("\n", strip=True)
            text = re.sub(r"\n{2,}", "\n", text)
            return text

        degree_keyword_re = re.compile(r"\b(" + "|".join([re.escape(k) for k in self.DEGREE_KEYWORDS]) + r")\b", re.IGNORECASE)
        college_hint_re = re.compile(r"(college|university|medical college|institute|school|hospital|medical)", re.IGNORECASE)
        experience_hint_re = re.compile(r"(year|yrs|experience|since|\d{4})", re.IGNORECASE)

        seen_chunks = set()
        for node in candidate_nodes:
            text = clean_text_from_node(node)
            for ln in [l.strip() for l in text.splitlines() if l.strip()]:
                ln_low = ln.lower()
                if ln in seen_chunks:
                    continue
                seen_chunks.add(ln)

                if len(ln) < 120 and any(city in ln_low for city in ["delhi", "patna", "jaipur", "jodhpur", "bihar", "mumbai", "chennai", "bangalore", "kolkata"]):
                    if not working_location:
                        for city in ["Delhi", "Patna", "Jaipur", "Jodhpur", "Bihar", "Mumbai", "Chennai", "Bangalore", "Kolkata"]:
                            if city.lower() in ln_low:
                                working_location = city
                                break

                if degree_keyword_re.search(ln):
                    parts = re.split(r"[:,\-–\|\/]", ln)
                    for p in parts:
                        if degree_keyword_re.search(p):
                            p_clean = re.sub(r"\s{2,}", " ", p).strip(" \t-•")
                            if p_clean and p_clean not in degrees:
                                degrees.append(p_clean)

                if college_hint_re.search(ln):
                    possible = re.sub(r"(?i)\b(qualification|qualification:|degree:|education:|studied at|studies at|qualification)\b", "", ln)
                    possible = re.sub(r"\b(consultant|doctor|dr\.|mbbs|md|dnb)\b", "", possible, flags=re.IGNORECASE)
                    possible = re.sub(r"[\n\t\r]+", " ", possible).strip(" -,:;•")
                    if len(possible) > 6 and possible not in colleges:
                        colleges.append(possible)

                if experience_hint_re.search(ln) or any(w in ln_low for w in ["consultant", "sr", "senior resident", "worked", "joined", "practice"]):
                    maybe = ln.strip(" -:•")
                    if len(maybe) > 10 and maybe not in experiences:
                        experiences.append(maybe)

        normalized_degrees = []
        for d in degrees:
            if " from " in d.lower():
                normalized_degrees.append(d.strip())
            else:
                normalized_degrees.append(d.strip())

        if not colleges and degrees:
            for d in normalized_degrees:
                m = re.search(r"from\s+(.*)", d, re.IGNORECASE)
                if m:
                    col = m.group(1).strip(" .;")
                    if col and col not in colleges:
                        colleges.append(col)

        if not working_location:
            page_text = soup.get_text("\n", strip=True)
            for city in ["Delhi", "Patna", "Jaipur", "Jodhpur", "Mumbai", "Chennai", "Bangalore", "Kolkata"]:
                if city.lower() in page_text.lower():
                    working_location = city
                    break

        def compact_list(lst: List[str]) -> List[str]:
            out = []
            for v in lst:
                v2 = re.sub(r"\s{2,}", " ", v).strip()
                v2 = re.sub(r"<[^>]+>", "", v2)
                v2 = v2.strip()
                if v2 and v2 not in out:
                    out.append(v2)
            return out

        normalized_degrees = compact_list(normalized_degrees)
        colleges = compact_list(colleges)
        experiences = compact_list(experiences)

        if normalized_degrees or colleges or experiences or working_location:
            return {
                "qualifications": normalized_degrees,
                "colleges": colleges or ["least one source"],
                "work_experience": experiences,
                "working_location": working_location or ""
            }
        return None

    def scrape(self, url: str, provider_id: str,
               doctor_name: Optional[str] = None,
               specialization: Optional[str] = None,
               save: bool = False) -> Dict[str, Any]:

        fetched = self._fetch_html(url)
        if not fetched.get("ok"):
            return {"provider_id": provider_id, "status": "fail", "error": fetched.get("error"), "trace": fetched.get("trace")}

        html = fetched["html"]
        js_heavy = self._looks_js_heavy(html)

        soup = self._bs4_soup(html)
        text = soup.get_text("\n", strip=True)

        hospital_emails = self._extract_emails(html + "\n" + text)
        hospital_phones = self._extract_phones(html + "\n" + text)
        hospital_addresses = self._extract_addresses(text)

        hospital_score = (
            (0.3 if hospital_emails else 0) +
            (0.4 if hospital_phones else 0) +
            (0.3 if hospital_addresses else 0)
        )

        # DOCTOR-level
        doctor_pages = []
        doctor_found = False
        doctor_emails = []
        doctor_phones = []
        spec_match = False
        name_match = False

        structured_profile = None

        if doctor_name:
            key = doctor_name.lower().replace("dr.", "").replace("dr ", "").strip()
            if key and key in text.lower():
                name_match = True

            pages = self._detect_doctor_pages(soup, doctor_name)
            # resolve relative pages against the page URL
            doctor_pages = [urllib.parse.urljoin(url, p) for p in pages]

            for p in doctor_pages:
                doc_fetched = self._fetch_html(p)
                if not doc_fetched.get("ok"):
                    logger.debug("Skipping doctor page %s: %s", p, doc_fetched.get("error"))
                    continue
                doc_html = doc_fetched["html"]
                doc_soup = self._bs4_soup(doc_html)
                doc_text = doc_soup.get_text(" ", strip=True).lower()
                doctor_found = True
                doctor_emails += self._extract_emails(doc_html + "\n" + doc_text)
                doctor_phones += self._extract_phones(doc_html + "\n" + doc_text)
                if specialization and specialization.lower() in doc_text:
                    spec_match = True

                if not structured_profile:
                    structured_profile = self.extract_doctor_profile_structured(doc_html)

        doctor_emails = list(dict.fromkeys(doctor_emails))
        doctor_phones = list(dict.fromkeys(doctor_phones))

        doctor_score = (
            (0.5 if doctor_found else 0) +
            (0.3 if name_match else 0) +
            (0.2 if spec_match else 0)
        )

        if structured_profile:
            hospital_addresses_structured = [structured_profile]
        else:
            hospital_addresses_structured = hospital_addresses

        output = {
            "provider_id": provider_id,
            "url": url,
            "status": "ok",
            "js_render_required": js_heavy,
            "hospital": {
                "emails": hospital_emails,
                "phones": hospital_phones,
                "addresses": hospital_addresses_structured,
                "score": round(min(hospital_score, 1.0), 3),
            },
            "doctor": {
                "name": doctor_name,
                "doctor_page_found": doctor_found,
                "doctor_pages": doctor_pages,
                "doctor_emails": doctor_emails,
                "doctor_phones": doctor_phones,
                "name_match": name_match,
                "specialization_match": spec_match,
                "score": round(min(doctor_score, 1.0), 3),
            }
        }

        if save:
            out = Path("data/processed/website_scrape")
            out.mkdir(exist_ok=True, parents=True)
            with open(out / f"{provider_id}.json", "w", encoding="utf-8") as fh:
                json.dump(output, fh, indent=2, ensure_ascii=False)

        return output


# CLI test helper
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--name", default=None)
    parser.add_argument("--spec", default=None)
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()

    scraper = WebsiteScraper()
    print(json.dumps(scraper.scrape(
        args.url, args.provider, args.name, args.spec, args.save
    ), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
