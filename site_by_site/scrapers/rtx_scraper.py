"""
Raytheon Technologies (RTX) scraper.

Uses Selenium (undetected-chromedriver) to load dynamic content from
careers.rtx.com, extract the `phApp.ddo` data object for listings,
and then fetch detailed job information via requests.
"""

from __future__ import annotations

import os
import time
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from typing import Any, Dict, List, Optional

from scrapers.base import JobScraper
from utils.extractors import extract_phapp_ddo, extract_total_results
from utils.detail_fetchers import fetch_detail_artifacts


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

            total_results = extract_total_results(phapp_data)
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
                phapp_data = extract_phapp_ddo(html)

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
    # Parse job
    # -------------------------------------------------------------------------
    def parse_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert listing to a minimal record + artifacts for canonicalization.
        """
        job_id = raw_job.get("jobId")
        detail_url = self.job_detail_url_template.format(job_id=job_id)
        artifacts = fetch_detail_artifacts(self.get, self.log, detail_url)
        return {
            "title": raw_job.get("title"),
            "location": raw_job.get("cityStateCountry"),
            "posting_id": job_id,
            "detail_url": artifacts.get("_canonical_url") or detail_url,
            "artifacts": artifacts,
        }
