import re
import json
import time
import base64
import requests
from bs4 import BeautifulSoup as BS
from math import ceil
from urllib.parse import urlencode, urlunparse, urljoin
from scrapers.base import JobScraper


class GeneralDynamicsScraper(JobScraper):
    START_URL = "https://www.gd.com/careers"
    JOB_SEARCH_URL = "https://www.gd.com/careers/job-search"
    API_PATH = "/API/Careers/CareerSearch"

    def __init__(self):
        super().__init__(base_url=self.START_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.suppress_console = False
        self.testing = getattr(self, "testing", False)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.headers.get("User-Agent", "Mozilla/5.0"),
                "Accept": "application/json, text/plain, */*",
                "Referer": self.JOB_SEARCH_URL,
            }
        )

        self.session.get(self.JOB_SEARCH_URL, timeout=30)

    def raw_id(self, raw_job):
        return raw_job.get("id")

    def b64url_decode(self, s):
        s += "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s.encode("utf-8")).decode("utf-8")

    def b64url_encode(self, obj):
        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    def absolute_url(self, href):
        return urljoin("https://www.gd.com", href or "")

    def text(self, node):
        return BS(str(node), "html.parser").get_text(" ", strip=True)

    def flatten(self, prefix, obj, out):
        if isinstance(obj, dict):
            for k, v in obj.items():
                self.flatten(f"{prefix}.{k}" if prefix else k, v, out)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                self.flatten(f"{prefix}[{i}]", v, out)
        else:
            out[prefix] = obj

    def collect_until_next_b(self, start_b):
        parts = []
        list_items = None
        for sib in start_b.next_siblings:
            # Stop when we hit the next <b> label
            if getattr(sib, "name", None) == "b":
                break
            # Collect lists as arrays
            if getattr(sib, "name", None) == "ul":
                items = []
                for li in sib.find_all("li"):
                    items.append(self.text(li))
                list_items = (list_items or []) + items
                continue
            # Everything else as text (handles <br>, <p>, strings, etc.)
            if isinstance(sib, str):
                parts.append(sib)
            else:
                parts.append(self.text(sib))
        # Normalize text
        value = " ".join(p.strip() for p in parts if p and p.strip())
        return (
            (list_items if list_items is not None else value).strip()
            if isinstance(value, str)
            else list_items
        )

    def extract_insets(self, soup: BS) -> dict:
        out = {}
        for dl in soup.select(".career-search-result__insets dl"):
            dt = dl.find("dt")
            dts = dt.get_text(" ", strip=True) if dt else ""
            # Some have icon-only <dt> (e.g., location). Use dd when dt is empty.
            dds = "; ".join(dd.get_text(" ", strip=True) for dd in dl.find_all("dd"))
            key = dts if dts else "Location"
            if key and dds:
                out[f"inset.{key}"] = dds
        return out

    def extract_bold_block(self, soup: BS) -> dict:
        """
        Parse blocks like:
        <b>Location:</b> USA AL Huntsville<br>...
        <b>Job Duties and Responsibilities</b><br><ul><li>...</li></ul>
        """
        data = {}
        container = soup.select_one(".career-detail-description") or soup
        for b in container.find_all("b"):
            label = self.text(b).rstrip(":").strip()
            if not label:
                continue
            val = self.collect_until_next_b(b)
            # If multiple same labels appear, keep the richest (list beats text, longer text beats shorter)
            if label in data:
                cur = data[label]

                def score(v):
                    return (1, len(v)) if isinstance(v, list) else (0, len(v or ""))

                if score(val) > score(cur):
                    data[label] = val
            else:
                data[label] = val
        # Flatten lists into strings (or keep lists if you prefer arrays)
        for k, v in list(data.items()):
            if isinstance(v, list):
                data[k] = "; ".join(v)
        # Add the H1 title as a convenience if present
        h1 = soup.select_one(".career-detail-title, h1")
        if h1 and "Page Title" not in data:
            data["Page Title"] = self.text(h1)
        return data

    def extract_jsonld(self, soup):
        out = {}
        blocks = soup.find_all("script", attrs={"type": "application/ld+json"})
        for b in blocks:
            try:
                data = json.loads(b.string or b.get_text() or "")
            except Exception:
                continue
            if isinstance(data, list):
                for i, item in enumerate(data):
                    self.flatten(f"ld[{i}]", item, out)
            else:
                self.flatten("ld", data, out)
        return out

    def extract_meta(self, soup):
        out = {}
        for m in soup.find_all("meta"):
            name = m.get("name") or m.get("property")
            if not name:
                continue
            content = m.get("content")
            if content is None:
                continue
            key = f"meta.{name}"
            if key not in out:
                out[key] = content
        h1 = soup.find("h1")
        if h1 and "text" not in out:
            out["h1"] = h1.get_text(strip=True)
        return out

    def extract_datalayer(self, html):
        out = {}
        for m in re.finditer(
            r"window\.dataLayer\.push\(\{([^)]*?)\}\)", html, re.I | re.M | re.S
        ):
            body = m.group(1)
            for k, v in re.findall(
                r"['\"]([^'\"]+)['\"]\s*:\s*['\"]([^'\"]*)['\"]", body
            ):
                out[f"datalayer.{k}"] = v
        return out

    def fetch_job_detail_html(self, url):
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.text

    def parse_job_detail_doc(self, html):
        soup = BS(html, "html.parser")
        out = {}
        out.update(self.extract_insets(soup))
        out.update(self.extract_bold_block(soup))
        return out

    def call_api(self, request_token):
        url = urlunparse(
            (
                "https",
                "www.gd.com",
                self.API_PATH,
                "",
                urlencode({"request": request_token}),
                "",
            )
        )
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.json(), url

    def build_payload(self, page, page_size=200):
        return {
            "address": [],
            "facets": [
                {"name": "career_page_size", "values": [{"value": "200 Jobs Per Page"}]}
            ],
            "page": page,
            "pageSize": page_size,
            "what": "",
            "usedPlacesApi": False,
        }

    def fetch_data(self):
        page_size = 200
        use_facet = True
        jobs = []
        total = None
        max_pages = None
        page = 0
        fetched = 0
        target = 40 if self.testing else 10**12
        start_time = time.time()
        while True:
            if use_facet:
                payload = {
                    "address": [],
                    "facets": [
                        {
                            "name": "career_page_size",
                            "values": [{"value": "200 Jobs Per Page"}],
                        }
                    ],
                    "page": page,
                    "pageSize": page_size,
                    "what": "",
                    "usedPlacesApi": False,
                }
            else:
                payload = {
                    "address": [],
                    "facets": [],
                    "page": page,
                    "pageSize": page_size,
                    "what": "",
                    "usedPlacesApi": False,
                }
            token = self.b64url_encode(payload)
            try:
                data, _ = self.call_api(token)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 400:
                    if use_facet:
                        use_facet = False
                        continue
                    if page_size > 100:
                        offset = fetched
                        page_size = 100
                        page = offset // page_size
                        max_pages = None
                        continue
                    break
                raise
            if total is None:
                total = int(data.get("ResultTotal") or 0)
            if max_pages is None:
                pc = data.get("PageCount")
                pc = (
                    int(pc)
                    if isinstance(pc, int) or (isinstance(pc, str) and pc.isdigit())
                    else 0
                )
                calc = ceil(total / page_size) if total else 0
                if pc and calc:
                    max_pages = min(pc, calc)
                else:
                    max_pages = pc or calc or None
            results = data.get("Results") or []
            for item in results:
                link = (item.get("Link") or {}).get("Url") or ""
                if link:
                    jobs.append(
                        {
                            "detail_url": link,
                            "title": item.get("Title") or "",
                            "id": str(item.get("Id") or ""),
                            "company": item.get("Company") or "",
                            "location": "; ".join(item.get("LocationNames") or []),
                        }
                    )
                    fetched += 1
                    if fetched >= target:
                        break
            if fetched >= target:
                break
            page += 1
            if max_pages is not None and page >= max_pages:
                break
            if not results:
                break
        if not self.suppress_console:
            dur = time.time() - start_time
            print(
                f"GeneralDynamics: collected {len(jobs)} search-result rows in {dur:.2f}s"
            )
        return jobs

    def parse_job(self, raw_job):
        detail_rel = raw_job.get("detail_url", "")
        detail_url = self.absolute_url(detail_rel)
        record = {
            "Detail URL": detail_url,
            "Position Title": raw_job.get("title", ""),
            "Company": raw_job.get("company", ""),
            "Location": raw_job.get("location", ""),
            "Posting ID": raw_job.get("id", ""),
        }
        try:
            html = self.fetch_job_detail_html(detail_url)
            doc = self.parse_job_detail_doc(html)
            record.update(doc)
        except Exception:
            # keep the listing data even if the detail parse fails
            pass
        return record
