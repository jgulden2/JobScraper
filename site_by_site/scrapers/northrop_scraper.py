import time
import re
import html
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl
from bs4 import BeautifulSoup as BS
from scrapers.base import JobScraper


class NorthropGrummanScraper(JobScraper):
    START_URL = "https://jobs.northropgrumman.com/careers"
    API_URL = "https://jobs.northropgrumman.com/api/apply/v2/jobs"
    CAREER_DETAIL_URL_TEMPLATE = "https://jobs.northropgrumman.com/careers?pid={pid}&domain=ngc.com&sort_by=recent"

    def __init__(self):
        super().__init__(base_url=self.START_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.suppress_console = False
        self.testing = getattr(self, "testing", False)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.headers.get("User-Agent", "Mozilla/5.0"),
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
                "Referer": self.START_URL,
            }
        )
        self.session.get(self.START_URL, timeout=30)

    def fetch_data(self):
        start = 0
        num = 100

        if getattr(self, "testing", False):
            num = 25
            max_jobs = 40

        jobs, total_count = [], None
        page_idx = 0

        while True:
            params = {
                "domains": "ngc.com",
                "domain": "ngc.com",
                "start": start,
                "num": num,
                "sort_by": "recent",
            }

            r = self.session.get(self.API_URL, params=params, timeout=30)

            r.raise_for_status()
            data = r.json()

            if total_count is None:
                total_count = data.get("count", 0)

            batch = data.get("positions", [])
            got = len(batch)
            print(f"page {page_idx} start={start} got={got} url={r.url}")

            if not batch:
                break

            for j in batch:
                jobs.append(
                    {
                        "pid": str(j.get("id")),
                        "ats_job_id": j.get("ats_job_id", ""),
                        "title": (j.get("name") or "").strip(),
                        "location": (j.get("location") or "").strip(),
                        "locations": j.get("locations", []),
                        "category": (j.get("department") or "").strip(),
                        "detail_url": j.get("canonicalPositionUrl"),
                    }
                )

                if getattr(self, "testing", False) and len(jobs) >= max_jobs:
                    break

            if getattr(self, "testing", False) and len(jobs) >= max_jobs:
                break

            start += got
            page_idx += 1

            if start >= total_count:
                break

        if not self.suppress_console:
            print(
                f"NorthropGrumman: Fetched {len(jobs)} jobs via API; reported total={data.get('count')}"
            )
        return jobs

    def _flatten(self, obj, prefix="", out=None):
        if out is None:
            out = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                self._flatten(v, f"{prefix}{k}." if prefix else f"{k}.", out)
        elif isinstance(obj, list):
            if all(isinstance(x, (str, int, float, bool)) or x is None for x in obj):
                out[prefix[:-1]] = "; ".join("" if x is None else str(x) for x in obj)
            else:
                for i, v in enumerate(obj):
                    self._flatten(v, f"{prefix}{i}.", out)
        else:
            out[prefix[:-1]] = "" if obj is None else obj
        return out

    def _parse_page_embed(self, html_text):
        soup = BS(html_text, "html.parser")
        code = soup.select_one("#smartApplyData")
        if not code:
            return {}
        raw = html.unescape(code.text)
        data = json.loads(raw)
        flat = self._flatten(data)
        if isinstance(data.get("positions"), list) and data["positions"]:
            pos = self._flatten(data["positions"][0])
            for k, v in pos.items():
                flat[f"positions.0.{k}"] = v
        return flat

    def parse_job(self, job):
        pid = job.get("pid") or job.get("ats_job_id")
        if not pid:
            return None

        base = {
            "Position Title": job.get("title", ""),
            "Location": job.get("location", ""),
            "Job Category": job.get("category", ""),
            "Posting ID": job.get("ats_job_id") or job.get("pid"),
            "Detail URL": job.get("detail_url", ""),
        }

        jr = self.session.get(
            f"{self.API_URL}/{pid}", params={"domain": "ngc.com"}, timeout=30
        )
        if jr.status_code == 200:
            j = jr.json()
            flat = self._flatten(j)
            desc = j.get("job_description") or j.get("description") or ""
            quals = j.get("qualifications") or ""
            pref = j.get("preferred_qualifications") or ""
            rec = {
                **base,
                "Job Description": self.clean_html(desc),
                "Required Skills": self.clean_html(quals),
                "Preferred Skills": self.clean_html(pref),
            }
            txt = f"{desc} {quals} {pref}".lower()
            rec["US Person Required"] = (
                "Yes" if ("us citizen" in txt or "u.s. citizen" in txt) else "No"
            )
            rec["Clearance Needed"] = self.extract_clearance(
                BeautifulSoup(desc, "html.parser")
            )
            rec["Clearance Obtainable"] = rec["Clearance Needed"]
            for k, v in flat.items():
                if k not in rec:
                    rec[f"json.{k}"] = v
            return rec

        if jr.status_code in (404, 405, 410):
            return base

        url = (
            job.get("detail_url")
            or f"https://jobs.northropgrumman.com/careers/job/{pid}"
        )
        u = urlparse(url)
        q = dict(parse_qsl(u.query))
        q.setdefault("domain", "ngc.com")
        url = urlunparse(
            (u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment)
        )
        r = self.session.get(url, timeout=30)
        if r.status_code != 200:
            return base
        flat_page = self._parse_page_embed(r.text)
        desc = (
            flat_page.get("positions.0.job_description")
            or flat_page.get("job_description")
            or flat_page.get("description")
            or ""
        )
        rec = {
            **base,
            "Job Description": self.clean_html(desc),
        }
        txt = desc.lower()
        rec["US Person Required"] = (
            "Yes" if ("us citizen" in txt or "u.s. citizen" in txt) else "No"
        )
        from bs4 import BeautifulSoup as _BS2

        rec["Clearance Needed"] = self.extract_clearance(_BS2(desc, "html.parser"))
        rec["Clearance Obtainable"] = rec["Clearance Needed"]
        for k, v in flat_page.items():
            if k not in rec:
                rec[f"page.{k}"] = v
        return rec

    def extract_clearance(self, soup):
        text = soup.get_text().lower()
        match = re.search(r"(secret|top secret|ts/sci|public trust)", text)
        return match.group(0).title() if match else ""

    def scrape(self):
        start_time = time.time()
        jobs_data = self.fetch_data()
        if getattr(self, "testing", False):
            jobs_data = jobs_data[:20]

        records = []
        all_keys = set()
        for job in jobs_data:
            try:
                rec = self.parse_job(job)
                if rec:
                    records.append(rec)
                    all_keys.update(rec.keys())
                    if not self.suppress_console:
                        print(
                            f"Parsed job: {rec.get('Position Title', '')} at {rec.get('Location', '')}"
                        )
            except Exception as e:
                if not self.suppress_console:
                    print(f"Detail parse failed: {e}")

        header = sorted(all_keys)
        normalized = [{k: r.get(k, "") for k in header} for r in records]
        self.jobs = normalized

        total_duration = time.time() - start_time
        print(
            f"{len(self.jobs)} Northrop Grumman job postings collected in {total_duration:.2f} seconds."
        )
