import re
import json
import time
import base64
import requests
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from scrapers.base import JobScraper


class GeneralDynamicsScraper(JobScraper):
    START_URL = "https://www.gd.com/careers"
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
                "Referer": self.START_URL,
            }
        )

    def _b64url_decode(self, s):
        s += "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s.encode("utf-8")).decode("utf-8")

    def _b64url_encode(self, obj):
        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    def _find_seed_request(self):
        r = self.session.get(self.START_URL, timeout=30)
        r.raise_for_status()
        m = re.search(r"/API/Careers/CareerSearch\?request=([A-Za-z0-9_\-]+)", r.text)
        if m:
            return m.group(1)
        m2 = re.search(r'"request"\s*:\s*"([A-Za-z0-9_\-]+)"', r.text)
        if m2:
            return m2.group(1)
        for u in re.findall(r"https://www\.gd\.com/[^\"']+", r.text):
            if "/API/Careers/CareerSearch?request=" in u:
                q = parse_qs(urlparse(u).query)
                if "request" in q and q["request"]:
                    return q["request"][0]
        raise RuntimeError("Could not locate initial GD CareerSearch request token")

    def _call_api(self, request_token):
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

    def fetch_data(self):
        seed = self._find_seed_request()
        payload = json.loads(self._b64url_decode(seed))
        payload.setdefault("Page", 0)
        payload.setdefault("PageSize", 10)
        if self.testing:
            payload["PageSize"] = min(20, payload.get("PageSize", 10))
        else:
            payload["PageSize"] = 100
        jobs = []
        total = None
        page = 0
        start_time = time.time()
        while True:
            payload["Page"] = page
            token = self._b64url_encode(payload)
            data, url_used = self._call_api(token)
            if total is None:
                total = int(data.get("ResultTotal") or 0)
                if not self.suppress_console:
                    print(f"GeneralDynamics: site reports total={total}")
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
                if self.testing and len(jobs) >= 40:
                    break
            if self.testing and len(jobs) >= 40:
                break
            page_count = int(data.get("PageCount") or 0)
            page += 1
            if page_count and page >= page_count:
                break
            if not results:
                break
        if not self.suppress_console:
            dur = time.time() - start_time
            print(
                f"GeneralDynamics: collected {len(jobs)} search-result rows in {dur:.2f}s"
            )
        return jobs

    def parse_job(self, job):
        return {
            "Detail URL": job.get("detail_url", ""),
            "Position Title": job.get("title", ""),
            "Company": job.get("company", ""),
            "Location": job.get("location", ""),
            "Posting ID": job.get("id", ""),
        }

    def scrape(self):
        data = self.fetch_data()
        if self.testing:
            data = data[:20]
        self.jobs = [self.parse_job(j) for j in data]
        if not self.suppress_console:
            print(f"{len(self.jobs)} General Dynamics job URLs collected.")
