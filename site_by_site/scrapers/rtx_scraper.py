"""
Raytheon Technologies (RTX) scraper.

Uses Selenium (undetected-chromedriver) to load dynamic content from
careers.rtx.com, extract the `phApp.ddo` data object for listings,
and then fetch detailed job information via requests.
"""

from __future__ import annotations

import json
import os
import re
import time
import requests
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from traceback import format_exc
from typing import Any, Dict, List, Optional, Tuple

from scrapers.base import JobScraper


# -------------------------------------------------------------------------
# Suppress noisy undetected_chromedriver destructor logs on Windows
# -------------------------------------------------------------------------
if os.name == "nt" and os.environ.get("RTX_SILENCE_UC_DEL", "1") == "1":
    try:

        def noop(self) -> None:
            return None

        uc.Chrome.__del__ = noop
    except Exception:
        pass


class RTXScraper(JobScraper):
    """
    Scraper for Raytheon Technologies (RTX) job listings.

    Workflow:
      1) Load search-results pages with undetected_chromedriver (UC) to capture
         dynamically populated job data from the global `phApp.ddo` variable.
      2) Extract job listings from the `eagerLoadRefineSearch` JSON.
      3) For each job, optionally fetch job detail pages for enrichment.
    """

    def __init__(self) -> None:
        """
        Initialize the RTX scraper with base URL, headers, and Selenium templates.

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        super().__init__(
            base_url="https://careers.rtx.com/global/en/search-results",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Connection": "keep-alive",
            },
        )
        self.search_url_template = (
            "https://careers.rtx.com/global/en/search-results?from={offset}&s=1"
        )
        self.job_detail_url_template = "https://careers.rtx.com/global/en/job/{job_id}/"
        self.page_size = 10
        self.suppress_console = False

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------
    def raw_id(self, raw_job: Dict[str, Any]) -> Optional[str]:
        """
        Return the job ID for de-duplication.

        Args:
            raw_job: Raw job dictionary from `phApp.ddo`.

        Returns:
            The job's 'jobId' string, or None if unavailable.
        """
        return raw_job.get("jobId")

    # -------------------------------------------------------------------------
    # Data extraction utilities
    # -------------------------------------------------------------------------
    def extract_phapp_ddo(self, html: str) -> Dict[str, Any]:
        """
        Extract and decode the global `phApp.ddo` object embedded in HTML.

        Args:
            html: The HTML string containing a script assignment to `phApp.ddo`.

        Returns:
            Parsed Python dictionary representing the `phApp.ddo` JSON.

        Raises:
            ValueError: If the `phApp.ddo` object cannot be found in the HTML.
            json.JSONDecodeError: If the JSON payload cannot be parsed.
        """
        pattern = re.compile(r"phApp\.ddo\s*=\s*(\{.*?\});", re.DOTALL)
        match = pattern.search(html)
        if not match:
            raise ValueError("phApp.ddo object not found in HTML")
        return json.loads(match.group(1))

    def extract_total_results(self, phapp_data: Dict[str, Any]) -> int:
        """
        Extract the total job count from the phApp.ddo structure.

        Args:
            phapp_data: The parsed JSON data from phApp.ddo.

        Returns:
            Integer total number of job results.
        """
        return int(phapp_data.get("eagerLoadRefineSearch", {}).get("totalHits", 0))

    # -------------------------------------------------------------------------
    # Fetch listings
    # -------------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Retrieve all job listings from RTX Careers search-results.

        Returns:
            A list of job listing dictionaries from `phApp.ddo['jobs']`.

        Raises:
            requests.RequestException: If network communication fails.
            ValueError: If the `phApp.ddo` data cannot be found or parsed.
        """
        job_limit = 15 if getattr(self, "testing", False) else float("inf")
        all_jobs: List[Dict[str, Any]] = []
        offset = 0

        options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1200")
        options.add_argument("--no-sandbox")

        self.log("driver:init")
        driver = uc.Chrome(options=options)

        try:
            first_page_url = "https://careers.rtx.com/global/en/search-results"
            driver.get(first_page_url)

            self.log("driver:wait", target="phApp.ddo")
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script("return window.phApp && window.phApp.ddo;")
            )
            phapp_data: Dict[str, Any] = driver.execute_script(
                "return window.phApp.ddo;"
            )

            total_results = self.extract_total_results(phapp_data)
            self.log("source:total", total=total_results)

            jobs: List[dict[str, Any]] = (
                phapp_data.get("eagerLoadRefineSearch", {})
                .get("data", {})
                .get("jobs", [])
            )
            for job in jobs:
                if len(all_jobs) >= job_limit:
                    break
                all_jobs.append(job)

            if len(all_jobs) >= job_limit:
                self.log("list:done", reason="test_limit_initial")
                return all_jobs

            self.log("list:fetched", count=len(jobs), offset=0)
            offset += self.page_size

            while offset < total_results:
                if len(all_jobs) >= job_limit:
                    break
                page_url = f"https://careers.rtx.com/global/en/search-results?from={offset}&s=1"
                driver.get(page_url)
                html = driver.page_source
                phapp_data = self.extract_phapp_ddo(html)

                jobs = (
                    phapp_data.get("eagerLoadRefineSearch", {})
                    .get("data", {})
                    .get("jobs", [])
                )
                if not jobs:
                    self.log("list:done", reason="empty")
                    break

                for job in jobs:
                    if len(all_jobs) >= job_limit:
                        break
                    all_jobs.append(job)

                self.log("list:fetched", count=len(jobs), offset=offset)
                offset += self.page_size
                time.sleep(1)

        finally:
            self.log("driver:quit")
            driver.quit()

        self.log("list:done", reason="end")
        return all_jobs

    # -------------------------------------------------------------------------
    # Fetch details
    # -------------------------------------------------------------------------
    def fetch_job_detail(self, job_id: str) -> Dict[str, Any]:
        """
        Fetch the detailed JSON for a given job posting.

        Args:
            job_id: The job identifier string.

        Returns:
            Parsed job detail data dictionary (may be empty if unavailable).

        Raises:
            requests.RequestException: If the detail page cannot be retrieved.
            ValueError: If the phApp.ddo object cannot be found or parsed.
        """
        url = self.job_detail_url_template.format(job_id=job_id)
        self.log("detail:fetch", url=url)

        try:
            response = requests.get(url, headers=self.headers)
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

    # -------------------------------------------------------------------------
    # Parsing helpers
    # -------------------------------------------------------------------------
    def extract_salary_range(self, text: str) -> Tuple[str, str]:
        """
        Extract a USD salary range from plain text.

        Args:
            text: Text to search for a pattern like '#### USD - #### USD'.

        Returns:
            A (min, max) tuple of numeric strings, or ("", "") if not found.
        """
        match = re.search(r"([\d,]+)\s*USD\s*-\s*([\d,]+)\s*USD", text)
        if match:
            min_salary = match.group(1).replace(",", "")
            max_salary = match.group(2).replace(",", "")
            return min_salary, max_salary
        return "", ""

    def extract_section(
        self, text: str, start_markers: List[str], end_markers: List[str]
    ) -> str:
        """
        Extract a section of text bounded by specific start and end markers.

        Args:
            text: The full text from which to extract.
            start_markers: List of possible starting phrases.
            end_markers: List of possible ending phrases.

        Returns:
            The text content between markers, stripped of colons and whitespace,
            or an empty string if no section is found.
        """
        for start_marker in start_markers:
            start_idx = text.find(start_marker)
            if start_idx != -1:
                for end_marker in end_markers:
                    end_idx = text.find(end_marker, start_idx)
                    if end_idx != -1:
                        section = text[start_idx + len(start_marker) : end_idx].strip()
                        return section.lstrip(": ").strip()
                # If no end_marker found, grab till end
                section = text[start_idx + len(start_marker) :].strip()
                return section.lstrip(": ").strip()
        return ""

    # -------------------------------------------------------------------------
    # Parse job
    # -------------------------------------------------------------------------
    def parse_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a raw job listing into a normalized structured record.

        Args:
            raw_job: One raw job dict as produced by `fetch_data()`.

        Returns:
            A normalized job dictionary containing:
              - Core metadata (title, location, posting ID)
              - Job description and qualification sections
              - Salary, clearance, relocation, and citizenship fields
        """
        job_id = raw_job.get("jobId")
        detail = self.fetch_job_detail(job_id)
        if not detail:
            self.log("parse:errors_detail", n=1, reason="detail_empty", job_id=job_id)

        description_html = detail.get("description", "")
        clean_desc = self.clean_html(description_html)

        us_person_required = (
            "Yes" if "U.S. citizenship is required" in clean_desc else "No"
        )
        salary_min, salary_max = self.extract_salary_range(clean_desc)

        required_skills = self.extract_section(
            clean_desc,
            start_markers=[
                "Qualifications You Must Have",
                "Qualifications, Experience and Skills",
                "Basic Qualifications",
                "Skills and Experience",
                "Experience and Qualifications",
            ],
            end_markers=[
                "Qualifications We Prefer",
                "Qualifications We Value",
                "Preferred Qualifications",
                "Highly Desirable",
                "Desirable",
                "What We Offer",
                "Benefits",
                "Privacy Policy and Terms",
            ],
        )

        preferred_skills = self.extract_section(
            clean_desc,
            start_markers=[
                "Qualifications We Prefer",
                "Qualifications We Value",
                "Highly Desirable",
                "Preferred Qualifications",
                "Desirable",
            ],
            end_markers=["What We Offer", "Benefits", "Privacy Policy and Terms"],
        )

        return {
            "Position Title": raw_job.get("title"),
            "Location": raw_job.get("cityStateCountry"),
            "Job Category": detail.get("category", ""),
            "Posting ID": job_id,
            "Post Date": detail.get("dateCreated"),
            "Clearance Obtainable": detail.get("clearenceLevel", ""),
            "Clearance Needed": detail.get("clearanceType", ""),
            "Relocation Available": detail.get("relocationEligible"),
            "US Person Required": us_person_required,
            "Salary Min": salary_min,
            "Salary Max": salary_max,
            "Reward Bonus": detail.get("reward", ""),
            "Hybrid/Online Status": detail.get("locationType", ""),
            "Required Skills": required_skills,
            "Preferred Skills": preferred_skills,
            "Job Description": clean_desc,
        }
