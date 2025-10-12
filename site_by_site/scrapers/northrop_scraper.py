"""
Northrop Grumman scraper.

Uses the public jobs API to enumerate listings, then enriches records either
by hitting the per-job API endpoint or, if needed, by falling back to the
HTML detail page and parsing the embedded data blob.

Logging follows the standardized `self.log("event", key=value, ...)` style.
"""

from __future__ import annotations

import requests

from typing import Any, Dict, List, Optional
from scrapers.base import JobScraper
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from utils.detail_fetchers import fetch_detail_artifacts


class NorthropGrummanScraper(JobScraper):
    """
    Scraper for Northrop Grumman job postings.

    Workflow:
      1) Page through the public API (`/api/apply/v2/jobs`) to gather listings.
      2) For each listing, attempt a detail fetch via `/api/apply/v2/jobs/{pid}`.
      3) If the detail API returns a non-200 but not fatal status, fall back to
         the HTML detail page and parse the embedded JSON payload.
    """

    START_URL = "https://jobs.northropgrumman.com/careers"
    API_URL = "https://jobs.northropgrumman.com/api/apply/v2/jobs"
    CAREER_DETAIL_URL_TEMPLATE = "https://jobs.northropgrumman.com/careers?pid={pid}&domain=ngc.com&sort_by=recent"

    def __init__(self) -> None:
        """
        Initialize HTTP session and headers.

        Args:
            None

        Returns:
            None

        Raises:
            requests.RequestException: If the initial warm-up request to
                `START_URL` fails (network errors, timeouts).
        """
        super().__init__(base_url=self.START_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.suppress_console = False
        self.testing = getattr(self, "testing", False)

        self.session = requests.Session()
        self.enable_retries(self.session)
        self.session.headers.update(
            {
                "User-Agent": self.headers.get("User-Agent", "Mozilla/5.0"),
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
                "Referer": self.START_URL,
            }
        )
        # Prime cookies/anti-bot
        self.session.get(self.START_URL, timeout=30)

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------
    def raw_id(self, raw_job: Dict[str, Any]) -> Optional[str]:
        """
        Return a stable raw identifier for de-duplication.

        Args:
            raw_job: Raw listing dictionary from the API.

        Returns:
            The ATS job ID if present, otherwise `pid`; or None if neither exists.

        Raises:
            None
        """
        return raw_job.get("ats_job_id") or raw_job.get("pid")

    # -------------------------------------------------------------------------
    # Listing
    # -------------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Retrieve job listings from the Northrop Grumman jobs API.

        Returns:
            List of raw listing dicts containing keys like:
            'pid', 'ats_job_id', 'title', 'location', 'locations', 'category',
            and 'detail_url'.

        Raises:
            requests.RequestException: If an API request fails (connection
                errors, timeouts). Note that `.raise_for_status()` is called on
                the listing request.
            ValueError: If an API response cannot be parsed as JSON or required
                fields are missing/invalid.
        """
        start = 0
        num = 100

        if getattr(self, "testing", False):
            num = 25
            max_jobs = 40

        jobs: List[Dict[str, Any]] = []
        total_count: Optional[int] = None
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
                total_count = int(data.get("count", 0))

            batch = data.get("positions", []) or []
            got = len(batch)
            self.log("list:page", page=page_idx, start=start, got=got, url=r.url)

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

            if total_count is not None and start >= total_count:
                break

        # Final listing telemetry
        self.log("source:total", total=data.get("count"))
        self.log("list:fetched", count=len(jobs))
        self.log("list:done", reason="end")
        return jobs

    # -------------------------------------------------------------------------
    # Parsing a single job
    # -------------------------------------------------------------------------
    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert a raw listing into a normalized job record, enriching with detail data.

        Args:
            raw_job: One raw listing dict as produced by `fetch_data()`.

        Returns:
            A normalized job record with core fields plus extracted detail data,
            or None if the listing lacks a usable `pid`.

        Raises:
            requests.RequestException: If a network error occurs while fetching
                API or HTML detail pages.
            json.JSONDecodeError: If embedded JSON on the HTML detail page cannot
                be decoded during fallback parsing.
        """
        pid = raw_job.get("pid") or raw_job.get("ats_job_id")
        if not pid:
            self.log("parse:skip", reason="no_pid")
            return None

        # Minimal identity fields: canonicalizer will populate the rest
        base = {
            "title": raw_job.get("title", ""),
            "location": raw_job.get("location", ""),
            "posting_id": raw_job.get("ats_job_id") or raw_job.get("pid"),
            "detail_url": raw_job.get("detail_url", ""),
        }

        # --- Preferred path: detail API
        self.log("detail:fetch", kind="api", pid=pid)
        jr = self.session.get(
            f"{self.API_URL}/{pid}", params={"domain": "ngc.com"}, timeout=30
        )

        if jr.status_code == 200:
            j = jr.json()
            return {
                **base,
                "artifacts": {"_vendor_blob": j},
            }

        if jr.status_code in (404, 405, 410):
            self.log("detail:http_status", kind="api", status=jr.status_code, pid=pid)
            return base

        # --- Fallback path: HTML detail page
        url = (
            raw_job.get("detail_url")
            or f"https://jobs.northropgrumman.com/careers/job/{pid}"
        )
        u = urlparse(url)
        q = dict(parse_qsl(u.query))
        q.setdefault("domain", "ngc.com")
        url = urlunparse(
            (u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment)
        )

        artifacts = fetch_detail_artifacts(self.session.get, self.log, url)
        return {
            **base,
            "detail_url": artifacts.get("_canonical_url") or url,
            "artifacts": artifacts,
        }
