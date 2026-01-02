"""
BAE Systems scraper.

Implements listing pagination via the BAE careers site and extracts per-job
details by parsing the client-side phApp.ddo payload from each job page.
"""

from __future__ import annotations

from typing import Any, Dict, List

from scrapers.engine import JobScraper
from utils.extractors import extract_phapp_ddo, extract_total_results
from utils.detail_fetchers import fetch_detail_artifacts


class BAESystemsScraper(JobScraper):
    """
    Scraper for BAE Systems job postings.

    Uses listing pages to enumerate jobs and then opens each job detail page
    to extract normalized fields for export.
    """

    # Used by the base pipeline for CSV/DB exports and incremental skip checks
    VENDOR = "BAE Systems"

    def __init__(self) -> None:
        """
        Initialize the scraper with base URL and headers.

        Args:
            None

        Returns:
            None
        """
        super().__init__(
            base_url="https://jobs.baesystems.com/global/en/search-results",
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Retrieve job listings from the BAE Systems search results.

        Returns:
            A list of raw listing entries as dictionaries.

        Raises:
            requests.RequestException: If the initial or subsequent listing
                requests fail at the HTTP layer.
            json.JSONDecodeError: If the listing pages contain malformed
                phApp.ddo JSON payloads.
            ValueError: If required structures in the phApp.ddo payload are
                missing or invalid.
        """
        all_jobs: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        offset = 0
        page_size = 10
        if getattr(self, "testing", False):
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 0
            except Exception:
                job_limit = 15
        else:
            job_limit = float("inf")

        first_page_url = f"{self.base_url}?from={offset}&s=1"
        response = self.get(first_page_url)
        response.raise_for_status()
        html = response.text
        phapp_data = extract_phapp_ddo(html)
        total_results = extract_total_results(phapp_data)

        self.log("source:total", total=total_results)

        while offset < total_results and len(all_jobs) < job_limit:
            page_url = f"{self.base_url}?from={offset}&s=1"
            response = self.get(page_url)
            response.raise_for_status()
            html = response.text
            phapp_data = extract_phapp_ddo(html)
            self.log("list:page", offset=offset, requested=page_size)

            jobs = (
                phapp_data.get("eagerLoadRefineSearch", {})
                .get("data", {})
                .get("jobs", [])
            )

            if not jobs:
                self.log("list:done", reason="empty")
                break

            seen_ids = {x.get("jobId") for x in all_jobs}
            repeats = [j.get("jobId") for j in jobs if j.get("jobId") in seen_ids]
            if repeats:
                self.log("list:dup_ids", offset=offset, ids=",".join(map(str, repeats)))
            # De-dupe at listing time to avoid overlapping-page repeats
            added = 0
            for j in jobs:
                jid = str(j.get("jobId") or "")
                if not jid:
                    all_jobs.append(j)  # keep nonstandard items just in case
                    added += 1
                    continue
                if jid in seen_ids:
                    # optional: log for visibility
                    self.log("list:dup", offset=offset, jobId=jid)
                    continue
                seen_ids.add(jid)
                all_jobs.append(j)
                added += 1
            self.log(
                "list:added_unique", offset=offset, added=added, total=len(all_jobs)
            )
            self.log("list:fetched", count=len(jobs), offset=offset)
            offset += page_size

        self.log("list:done", reason="end")
        return all_jobs

    def parse_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a raw listing entry into a normalized job record.

        Args:
            job: Raw listing item as returned by `fetch_data`.

        Returns:
            A normalized job record dictionary suitable for export.

        Raises:
            Exception: Any exceptions thrown here will be caught and logged
                by `run()` as `parse:error`, and the pipeline will continue
                with the next record.
        """
        job_id = job.get("jobId")
        detail_url = f"https://jobs.baesystems.com/global/en/job/{job_id}/"
        artifacts = fetch_detail_artifacts(
            self.thread_get,
            self.log,
            detail_url,
            get_jsonld=False,
            get_meta=False,
            get_datalayer=False,
        )
        ph = artifacts.get("_vendor_blob")

        return {
            "Position Title": ph.get("title"),
            "Detail URL": detail_url,
            "Description": ph.get("description"),
            "Post Date": ph.get("postedDate"),
            "Posting ID": job_id,
            "US Person Required": ph.get("isUsCitizenshipRequired"),
            "Clearance Level Must Possess": ph.get("isSecurityClearanceRequired"),
            "Clearance Level Must Obtain": ph.get("clearenceLevel"),
            "Relocation Available": ph.get("isRelocationAvailable"),
            "Salary Raw": ph.get("payRange"),
            "Salary Min (USD/yr)": ph.get("salaryMin"),
            "Salary Max (USD/yr)": ph.get("salaryMax"),
            "Bonus": ph.get("bonus"),
            "Remote Status": ph.get("physicalLocation"),
            "Full Time Status": ph.get("structureData", {}).get("employmentType"),
            "Hours Per Week": ph.get("structureData", {}).get("workHours"),
            "Travel Percentage": ph.get("travelPercentage"),
            "Job Category": ph.get("category"),
            "Business Sector": ph.get("sector"),
            "Business Area": ph.get("businessArea"),
            "Industry": ph.get("industry"),
            "Shift": ph.get("shift"),
            "Career Level": ph.get("careerLevel"),
            "Raw Location": ph.get("location"),
            "Country": ph.get("country"),
            "State": ph.get("state"),
            "City": ph.get("city"),
            "Postal Code": ph.get("postalCode"),
            "Latitude": ph.get("latitude"),
            "Longitude": ph.get("longitude"),
        }
