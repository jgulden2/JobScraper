"""
BAE Systems scraper.

Implements listing pagination via the BAE careers site and extracts per-job
details by parsing the client-side phApp.ddo payload from each job page.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from scrapers.base import JobScraper
from utils.extractors import extract_phapp_ddo, extract_total_results
from utils.detail_fetchers import fetch_detail_artifacts


class BAESystemsScraper(JobScraper):
    """
    Scraper for BAE Systems job postings.

    Uses listing pages to enumerate jobs and then opens each job detail page
    to extract normalized fields for export.
    """

    def __init__(self) -> None:
        """
        Initialize the scraper with base URL and headers.

        Args:
            None

        Returns:
            None
        """
        self.suppress_console = False
        super().__init__(
            base_url="https://jobs.baesystems.com/global/en/search-results",
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def raw_id(self, raw_job: Dict[str, Any]) -> Optional[str]:
        """
        Return the stable raw identifier used for de-duplication.

        Args:
            raw_job: Raw listing item from the listing payload.

        Returns:
            The job's unique ID string or None if unavailable.
        """
        return raw_job.get("jobId")

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
        offset = 0
        page_size = 10
        job_limit = 15 if getattr(self, "testing", False) else float("inf")  # type: ignore[assignment]

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

            all_jobs.extend(jobs)
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
        artifacts = fetch_detail_artifacts(self.get, self.log, detail_url)
        if not artifacts:
            self.log("parse:errors_detail", n=1, reason="detail_empty", job_id=job_id)
        return {
            "title": job.get("title"),
            "posting_id": job_id,
            "detail_url": artifacts.get("_canonical_url") or detail_url,
            "artifacts": artifacts,
        }
