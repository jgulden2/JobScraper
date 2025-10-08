"""
Lockheed Martin scraper.

Lists job result pages from the public search UI, gathers per-job links/IDs,
and then parses each job detail page (preferring JSON-LD when possible) into
a normalized record for export.
"""

from __future__ import annotations

import json
import re
from time import sleep
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup as BS

from scrapers.base import JobScraper
from traceback import format_exc


class LockheedMartinScraper(JobScraper):
    """
    Scraper for Lockheed Martin job postings.

    Workflow:
      1) Determine total listing pages from the search results page.
      2) Iterate result pages to collect job links and IDs (de-duped per run).
      3) Visit each job detail page and parse JSON-LD and tagged fields.
    """

    BASE_URL = "https://www.lockheedmartinjobs.com"
    SEARCH_URL = f"{BASE_URL}/search-jobs"

    def __init__(self, max_pages: Optional[int] = None, delay: float = 0.5) -> None:
        """
        Initialize the scraper with UI endpoints and runtime tuning knobs.

        Args:
            max_pages: Optional maximum number of listing pages to visit.
            delay: Sleep (seconds) between page fetches to be polite.

        Returns:
            None

        Raises:
            None
        """
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.visited_job_ids: Set[str] = set()
        self.max_pages: Optional[int] = max_pages
        self.delay = delay
        self.suppress_console = False

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------
    def raw_id(self, raw_job: Dict[str, Any]) -> Optional[str]:
        """
        Extract a stable identifier for de-duplication of parsed records.

        Args:
            raw_job: The raw job object produced during fetch.

        Returns:
            The vendor job ID string, or None if not present.

        Raises:
            None
        """
        return raw_job.get("job_id")

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, str]]:
        """
        Collect job links/IDs from the search results pages.

        Returns:
            A list of objects like: {"job_id": "...", "url": "https://..."}.

        Raises:
            requests.RequestException: If listing pages cannot be fetched.
            ValueError: If the pagination metadata is malformed.
        """
        total_pages = self.get_total_pages()
        self.log("list:pages", total_pages=total_pages)

        job_limit = 15 if getattr(self, "testing", False) else float("inf")  # type: ignore[assignment]
        if getattr(self, "testing", False):
            total_pages = 1
        elif self.max_pages:
            total_pages = min(total_pages, self.max_pages)

        all_job_links: List[Dict[str, str]] = []
        for page_num in range(1, total_pages + 1):
            page_links = self.get_job_links(page_num)
            self.log("list:fetched", page=page_num, count=len(page_links))

            for link in page_links:
                if len(all_job_links) >= job_limit:
                    break
                all_job_links.append(link)

            sleep(self.delay)

        self.log("list:done", reason="end")
        return all_job_links

    def parse_job(self, job_entry: Dict[str, str]) -> Dict[str, str]:
        """
        Convert a fetched job entry into the normalized record shape.

        Args:
            job_entry: An item from `fetch_data()` containing 'url' and 'job_id'.

        Returns:
            Dictionary with normalized fields suitable for export.

        Raises:
            None. Network/parse errors inside detail scraping are handled and
            logged; an empty or partial record will still be returned.
        """
        return self.scrape_job_detail(job_entry["url"], job_entry["job_id"])

    # -------------------------------------------------------------------------
    # Listing helpers
    # -------------------------------------------------------------------------
    def get_total_pages(self) -> int:
        """
        Inspect the search page and return the total number of result pages.

        Returns:
            Positive integer count of available pages (defaults to 1).

        Raises:
            requests.RequestException: If the search page cannot be fetched.
            ValueError: If the 'data-total-pages' attribute is non-numeric.
        """
        response = requests.get(self.SEARCH_URL, headers=self.headers)
        response.raise_for_status()
        soup = BS(response.text, "html.parser")
        pagination = soup.select_one("section#search-results")
        if not pagination:
            return 1
        total_raw = pagination.get("data-total-pages", "1")
        try:
            return int(total_raw)
        except (TypeError, ValueError):
            raise ValueError(f"Unexpected data-total-pages value: {total_raw!r}")

    def get_job_links(self, page_num: int) -> List[Dict[str, str]]:
        """
        Collect unique (job_id, url) pairs from a given search results page.

        Args:
            page_num: 1-based index of the results page to fetch.

        Returns:
            List of dicts with 'job_id' and 'url' keys; duplicates are skipped.

        Raises:
            requests.RequestException: If the page cannot be fetched.
        """
        page_url = f"{self.SEARCH_URL}?p={page_num}"
        response = requests.get(page_url, headers=self.headers)
        response.raise_for_status()

        soup = BS(response.text, "html.parser")
        job_links: List[Dict[str, str]] = []

        for link in soup.select("section#search-results-list a[data-job-id]"):
            job_id = link["data-job-id"]
            href = link["href"]
            if not job_id or not href:
                continue
            if job_id in self.visited_job_ids:
                continue
            self.visited_job_ids.add(job_id)
            full_url = f"{self.BASE_URL}{href}"
            job_links.append({"job_id": job_id, "url": full_url})

        return job_links

    # -------------------------------------------------------------------------
    # Detail parsing
    # -------------------------------------------------------------------------
    def scrape_job_detail(self, url: str, job_id: str) -> Dict[str, str]:
        """
        Fetch and parse a job detail page.

        Args:
            url: Absolute URL to the job detail page.
            job_id: Vendor job identifier.

        Returns:
            A normalized job record containing core fields (title, location,
            IDs, skills, description, etc.). Missing values are returned as
            empty strings.

        Raises:
            requests.RequestException: Propagated if the HTTP request fails.
                (Note: `run()` catches and logs parse exceptions per item.)
            json.JSONDecodeError: Not raised; JSON-LD parse errors are logged
                as 'detail:jsonld_error' and parsing continues.
        """
        self.log("detail:fetch", url=url)
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        soup = BS(response.text, "html.parser")

        # Prefer JSON-LD when available
        job_data: Dict[str, Any] = {}
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                # Guard against stray control chars that break json.loads
                clean_json = re.sub(r"[\x00-\x1F\x7F]", "", json_ld.string)
                job_data = json.loads(clean_json)
            except json.JSONDecodeError:
                self.log(
                    "detail:jsonld_error", level="warning", url=url, error=format_exc()
                )

        # Convenience accessor for JSON-LD fields
        def get_ld(field: str, default: str = "") -> str:
            val = job_data.get(field, default)
            return val if isinstance(val, str) else default

        # Location(s)
        locations: List[str] = []
        for loc in job_data.get("jobLocation") or []:
            address = loc.get("address", {}) if isinstance(loc, dict) else {}
            city = (
                address.get("addressLocality", "") if isinstance(address, dict) else ""
            )
            region = (
                address.get("addressRegion", "") if isinstance(address, dict) else ""
            )
            full_location = ", ".join(filter(None, [city, region]))
            if full_location:
                locations.append(full_location)
        location = "; ".join(locations)

        # Salary range
        salary_min, salary_max = self.extract_salary_range(soup)

        return {
            "Position Title": get_ld("title"),
            "Location": location,
            "Job Category": self.extract_job_category(soup, get_ld("industry")),
            "Posting ID": job_id,
            "Post Date": get_ld("datePosted"),
            "Clearance Obtainable": get_ld("employmentType"),
            "Clearance Needed": self.extract_tagged_value(soup, "Clearance Level"),
            "Relocation Available": self.extract_tagged_value(
                soup, "Relocation Available"
            ),
            "US Person Required": "Yes" if "US Citizen" in soup.get_text() else "No",
            "Salary Min": salary_min,
            "Salary Max": salary_max,
            "Reward Bonus": "",
            "Hybrid/Online Status": self.extract_tagged_value(
                soup, "Ability to Work Remotely"
            ),
            "Required Skills": self.clean_html(get_ld("qualifications")),
            "Preferred Skills": self.clean_html(get_ld("educationRequirements")),
            "Job Description": self.clean_html(get_ld("description")),
        }

    def extract_salary_range(self, soup: BS) -> Tuple[str, str]:
        """
        Parse a dollar range from the 'Pay Rate' section if present.

        Args:
            soup: Parsed BeautifulSoup document of the job detail page.

        Returns:
            A `(min, max)` tuple of numeric strings (no commas), or empty strings
            when no range is present.

        Raises:
            None
        """
        pay_rate_tag = soup.find("strong", string=lambda s: s and "Pay Rate:" in s)
        if not pay_rate_tag or not pay_rate_tag.next_sibling:
            return "", ""

        pay_rate_text = str(pay_rate_tag.next_sibling)
        match = re.search(r"\$([\d,]+)\s*-\s*\$([\d,]+)", pay_rate_text)
        if not match:
            return "", ""
        min_salary = match.group(1).replace(",", "")
        max_salary = match.group(2).replace(",", "")
        return min_salary, max_salary

    def extract_job_category(self, soup: BS, json_ld_industry: str) -> str:
        """
        Determine the job category, favoring on-page tags over JSON-LD.

        Args:
            soup: Parsed BeautifulSoup document.
            json_ld_industry: The 'industry' field from JSON-LD (if present).

        Returns:
            A job category string or an empty string.

        Raises:
            None
        """
        value = self.extract_tagged_value(soup, "Career Area")
        if value:
            return value.strip()
        if json_ld_industry:
            return re.sub(r"^\d+:\s*", "", json_ld_industry).strip()
        return ""

    def extract_tagged_value(self, soup: BS, label: str) -> str:
        """
        Read a value that appears immediately after a bolded label.

        Example HTML:
            <b>Clearance Level:</b> Secret

        Args:
            soup: Parsed BeautifulSoup document.
            label: The exact label text to locate (case-sensitive substring).

        Returns:
            The stripped value following the label, or an empty string if not found.

        Raises:
            None
        """
        tag = soup.find("b", string=lambda text: text and label in text)
        if tag and tag.next_sibling:
            return str(tag.next_sibling).strip(": ").strip()
        return ""
