# scrapers/usajobs_scraper.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

import os

from scrapers.engine import JobScraper


class USAJOBSScraper(JobScraper):
    """
    Scraper for USAJOBS postings via their public API.

    Workflow:
      1) Page through the USAJOBS search API to collect listings.
      2) For each listing, either:
         - use API fields directly, or
         - hit a detail endpoint / HTML page for enrichment.
      3) Map fields into our canonical schema; JobScraper handles the rest.
    """

    VENDOR = "USAJOBS"

    API_BASE = "https://data.usajobs.gov/api"

    def __init__(self) -> None:
        user_agent = os.getenv("USAJOBS_USER_AGENT", "")
        api_key = os.getenv("USAJOBS_API_KEY", "")

        super().__init__(
            base_url=f"{self.API_BASE}/Search",
            headers={
                "Host": "data.usajobs.gov",
                "User-Agent": user_agent,
                "Authorization-Key": api_key,
                "Accept": "application/json",
            },
        )

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Call the USAJOBS search API and return a list of raw listing dicts.
        """
        jobs: List[Dict[str, Any]] = []

        page = 1
        per_page = 250  # safe default, max is 500

        if getattr(self, "testing", False):
            # Smaller pages in test mode
            per_page = min(int(getattr(self, "test_limit", 40)), per_page)

        self.log("list:start", vendor=self.VENDOR, per_page=per_page)

        while True:
            params = {
                "ResultsPerPage": per_page,
                "Page": page,
                "Fields": "full",
            }

            self.log("list:page", page=page, per_page=per_page, params=params)

            r = self.session.get(self.base_url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            batch = self._extract_positions_from_response(data)
            if not batch:
                break

            jobs.extend(batch)

            # If testing, stop once we have enough items
            if getattr(self, "testing", False) and len(jobs) >= getattr(
                self, "test_limit", 40
            ):
                jobs = jobs[: self.test_limit]  # type: ignore[attr-defined]
                break

            page += 1

        self.log("list:fetched", count=len(jobs), vendor=self.VENDOR)
        return jobs

    def _extract_positions_from_response(
        self, data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Helper: pluck the list of SearchResultItems from the USAJOBS API response.
        """
        sr = data.get("SearchResult") or {}
        items = sr.get("SearchResultItems") or []
        # Each item has {MatchedObjectId, MatchedObjectDescriptor}
        return items

    # ------------------------------------------------------------------
    # Parse a single listing
    # ------------------------------------------------------------------
    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Map a single USAJOBS SearchResultItem into our normalized record.
        """
        descriptor = raw_job.get("MatchedObjectDescriptor") or {}
        if not descriptor:
            return None

        detail_url = descriptor.get("PositionURI") or descriptor.get("ApplyURI")
        if isinstance(detail_url, list):
            detail_url = detail_url[0] if detail_url else None

        title = descriptor.get("PositionTitle")
        posting_id = descriptor.get("PositionID") or raw_job.get("MatchedObjectId")

        # Location info is a list
        locs = descriptor.get("PositionLocation") or []
        loc0 = locs[0] if isinstance(locs, list) and locs else {}

        # Salary text lives in UserArea.Details.Salary
        details = (descriptor.get("UserArea") or {}).get("Details") or {}
        salary_raw = details.get("Salary")
        salary_min = None
        salary_max = None

        description = (
            descriptor.get("PositionSummary")
            or descriptor.get("UserArea", {}).get("Details", {}).get("JobSummary")
            or ""
        )

        # Use PositionStartDate as "Post Date"
        post_date = descriptor.get("PositionStartDate")

        rec: Dict[str, Any] = {
            "Position Title": title,
            "Detail URL": detail_url,
            "Posting ID": posting_id,
            "Description": description,
            "Post Date": post_date,
            "Salary Min (USD/yr)": salary_min,
            "Salary Max (USD/yr)": salary_max,
            "Salary Raw": salary_raw,
            "Raw Location": loc0.get("LocationName") or loc0.get("CityName"),
            "City": loc0.get("CityName"),
            "State": loc0.get("StateCode"),
            "Country": loc0.get("CountryCode"),
            "Postal Code": loc0.get("PostalCode"),
            "Full Time Status": descriptor.get("PositionSchedule"),
            "Job Category": descriptor.get("JobCategory"),
            "Required Skills": details.get("RequiredDocuments")
            or descriptor.get("QualificationSummary"),
        }

        return rec
