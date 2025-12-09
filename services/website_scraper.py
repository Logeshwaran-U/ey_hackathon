
import re
import json
import argparse
import requests
import urllib.parse
from bs4 import BeautifulSoup
from pathlib import Path
from typing import List, Dict, Any, Optional


class WebsiteScraper:
    USER_AGENT = {
        "User-Agent": "Mozilla/5.0 (ProviderValidatorBot/3.0)"
    }

    PHONE_RE = re.compile(
        r"(?:\+?\d{1,3}[- ]?)?(?:\(?\d{3,5}\)?[- ]?)?\d{3,4}[- ]?\d{3,4}"
    )

    ADDRESS_KEYWORDS = [
        "road", "street", "st", "avenue", "colony", "sector",
        "lane", "block", "district", "city", "india", "usa"
    ]

    MAX_CRAWL_PAGES = 3
    REQUEST_TIMEOUT = 3     

    def _fetch_html(self, url: str) -> str:
        try:
            r = requests.get(
                url,
                headers=self.USER_AGENT,
                timeout=self.REQUEST_TIMEOUT
            )
            if r.status_code == 200 and len(r.text) < 2_000_000:  
                return r.text
        except Exception:
            pass
        return ""

    def _extract_emails(self, text: str) -> List[str]:
        return list(set(re.findall(
            r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
            text
        )))

    def _extract_phones(self, text: str) -> List[str]:
        raw = re.findall(self.PHONE_RE, text)
        cleaned = set()

        for num in raw:
            digits = re.sub(r"\D", "", num)
            if 7 <= len(digits) <= 15:
                if len(digits) == 10:
                    cleaned.add("+91" + digits)
                else:
                    cleaned.add("+" + digits)

        return list(cleaned)

    def _extract_addresses(self, text: str) -> List[str]:
        lines = text.split("\n")
        addrs = []
        for ln in lines:
            ln_low = ln.lower()
            if any(k in ln_low for k in self.ADDRESS_KEYWORDS):
                if 15 <= len(ln.strip()) <= 200:
                    addrs.append(ln.strip())
        return list(set(addrs))

    def _detect_doctor_pages(self, soup: BeautifulSoup, doctor_name: str) -> List[str]:
        if not doctor_name:
            return []

        key = doctor_name.lower().replace("dr.", "").replace("dr ", "").strip()
        first = key.split()[0]

        pages = []
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            txt = (a.get_text() or "").lower()

            if any(w in href for w in ["doctor", "team", "profile", "physician"]):
                if first in href or first in txt:
                    pages.append(a["href"])

            if key in txt:
                pages.append(a["href"])

        return list(set(pages))[:self.MAX_CRAWL_PAGES]  # LIMIT FIX

    # MAIN SCRAPE FUNCTION
    def scrape(self, url: str, provider_id: str,
               doctor_name: Optional[str] = None,
               specialization: Optional[str] = None,
               save: bool = False) -> Dict[str, Any]:

        html = self._fetch_html(url)
        if not html:
            return {
                "provider_id": provider_id,
                "status": "fail",
                "error": "Website unreachable"
            }

        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text("\n", strip=True)

        # HOSPITAL LEVEL
        hospital_emails = self._extract_emails(html)
        hospital_phones = self._extract_phones(html)
        hospital_addresses = self._extract_addresses(text)

        hospital_score = (
            (0.3 if hospital_emails else 0) +
            (0.4 if hospital_phones else 0) +
            (0.3 if hospital_addresses else 0)
        )

        #  DOCTOR LEVEL
        doctor_pages = []
        doctor_found = False
        doctor_emails = []
        doctor_phones = []
        spec_match = False
        name_match = False

        if doctor_name:
            key = doctor_name.lower().replace("dr.", "").replace("dr ", "").strip()

            if key in text.lower():
                name_match = True

            pages = self._detect_doctor_pages(soup, doctor_name)
            doctor_pages = [urllib.parse.urljoin(url, p) for p in pages]

            for p in doctor_pages:
                doc_html = self._fetch_html(p)
                if not doc_html:
                    continue

                doctor_found = True
                doc_soup = BeautifulSoup(doc_html, "lxml")
                doc_text = doc_soup.get_text(" ", strip=True).lower()

                doctor_emails += self._extract_emails(doc_html)
                doctor_phones += self._extract_phones(doc_html)

                if specialization and specialization.lower() in doc_text:
                    spec_match = True

        doctor_score = (
            (0.5 if doctor_found else 0) +
            (0.3 if name_match else 0) +
            (0.2 if spec_match else 0)
        )

        output = {
            "provider_id": provider_id,
            "url": url,
            "status": "ok",
            "hospital": {
                "emails": list(set(hospital_emails)),
                "phones": list(set(hospital_phones)),
                "addresses": hospital_addresses,
                "score": min(hospital_score, 1.0),
            },
            "doctor": {
                "name": doctor_name,
                "doctor_page_found": doctor_found,
                "doctor_pages": doctor_pages,
                "doctor_emails": list(set(doctor_emails)),
                "doctor_phones": list(set(doctor_phones)),
                "name_match": name_match,
                "specialization_match": spec_match,
                "score": min(doctor_score, 1.0),
            }
        }

        if save:
            out = Path("data/processed/website_scrape")
            out.mkdir(exist_ok=True, parents=True)
            json.dump(output, open(out / f"{provider_id}.json", "w"), indent=2)

        return output



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
    ), indent=2))


if __name__ == "__main__":
    main()

