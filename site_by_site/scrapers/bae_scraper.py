"""
BAE Systems scraper.

Implements listing pagination via the BAE careers site and extracts per-job
details by parsing the client-side phApp.ddo payload from each job page.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import requests
from scrapers.base import JobScraper
from traceback import format_exc


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

    def extract_total_results(self, phapp_data: Dict[str, Any]) -> int:
        """
        Extract the total number of hits from a phApp.ddo payload.

        Args:
            phapp_data: Parsed phApp.ddo JSON object from a listing page.

        Returns:
            Total number of hits as an integer.

        Raises:
            KeyError: If expected keys are missing and cannot be resolved.
            ValueError: If the extracted value cannot be converted to int.
            TypeError: If the extracted value is not a valid type for int().
        """
        return int(phapp_data.get("eagerLoadRefineSearch", {}).get("totalHits", 0))

    def extract_phapp_ddo(self, html: str) -> Dict[str, Any]:
        """
        Parse the page HTML and return the embedded phApp.ddo JSON object.

        Args:
            html: Full HTML of a listing or detail page.

        Returns:
            The decoded phApp.ddo JSON object as a dictionary.

        Raises:
            ValueError: If the phApp.ddo object is not found in the HTML.
            json.JSONDecodeError: If the embedded JSON cannot be decoded.
        """
        pattern = re.compile(r"phApp\.ddo\s*=\s*(\{.*?\});", re.DOTALL)
        match = pattern.search(html)
        if not match:
            raise ValueError("phApp.ddo object not found in HTML")
        phapp_ddo_str = match.group(1)
        data: Dict[str, Any] = json.loads(phapp_ddo_str)
        return data

    def fetch_job_detail(self, job_id: Optional[str]) -> Dict[str, Any]:
        """
        Fetch and parse the job detail page payload for a given job.

        Args:
            job_id: BAE Systems job ID string.

        Returns:
            Parsed job detail dictionary (may be empty on failure).

        Raises:
            None. Network and parse errors are captured and logged; on failure,
            an empty dict is returned to allow the pipeline to continue.
        """
        url = f"https://jobs.baesystems.com/global/en/job/{job_id}/"
        self.log("detail:fetch", url=url)
        try:
            response = self.get(url)
            response.raise_for_status()
        except requests.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            self.log(
                "detail:http_error",
                level="warning",
                url=url,
                status=status,
                error=format_exc(),
            )
            return {}
        html = response.text
        try:
            phapp_data = self.extract_phapp_ddo(html)
        except Exception:
            self.log("detail:parse_error", level="warning", url=url, error=format_exc())
            return {}
        return phapp_data.get("jobDetail", {}).get("data", {}).get("job", {})

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
        phapp_data = self.extract_phapp_ddo(html)
        total_results = self.extract_total_results(phapp_data)

        self.log("source:total", total=total_results)

        while offset < total_results and len(all_jobs) < job_limit:
            page_url = f"{self.base_url}?from={offset}&s=1"
            response = self.get(page_url)
            response.raise_for_status()
            html = response.text
            phapp_data = self.extract_phapp_ddo(html)
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
        detail = self.fetch_job_detail(job_id)

        if not detail:
            self.log("parse:errors_detail", n=1, reason="detail_empty", job_id=job_id)

        return {
            "Position Title": job.get("title"),
            "Location": job.get("cityStateCountry"),
            "Job Category": ", ".join(job.get("multi_category", [])),
            "Posting ID": job_id,
            "Post Date": job.get("postedDate"),
            "Clearance Obtainable": detail.get("clearenceLevel", ""),
            "Clearance Needed": detail.get("isSecurityClearanceRequired", ""),
            "Relocation Available": job.get("isRelocationAvailable"),
            "US Person Required": "Yes" if detail.get("itar") == "Yes" else "No",
            "Salary Min": detail.get("salaryMin", ""),
            "Salary Max": detail.get("salaryMax", ""),
            "Reward Bonus": detail.get("reward", ""),
            "Hybrid/Online Status": detail.get("physicalLocation", ""),
            "Required Skills": self.clean_html(
                detail.get("requiredSkillsEducation", "")
            ),
            "Preferred Skills": self.clean_html(
                detail.get("preferredSkillsEducation", "")
            ),
            "Job Description": self.clean_html(detail.get("description", "")),
        }
