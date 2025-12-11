"""
Lockheed Martin scraper.

Lists job result pages from the public search UI, gathers per-job links/IDs,
and then parses each job detail page (preferring JSON-LD when possible) into
a normalized record for export.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set

from bs4 import BeautifulSoup as BS

from scrapers.base import JobScraper
from utils.extractors import extract_bold_block
from utils.detail_fetchers import fetch_detail_artifacts


class LockheedMartinScraper(JobScraper):
    """
    Scraper for Lockheed Martin job postings.

    Workflow:
      1) Determine total listing pages from the search results page.
      2) Iterate result pages to collect job links and IDs (de-duped per run).
      3) Visit each job detail page and parse JSON-LD and tagged fields.
    """

    # Used by the base pipeline for CSV/DB exports and incremental skip checks
    VENDOR = "Lockheed Martin"
    BASE_URL = "https://www.lockheedmartinjobs.com"
    SEARCH_URL = f"{BASE_URL}/search-jobs"

    def __init__(self, max_pages: Optional[int] = None) -> None:
        """
        Initialize the scraper with UI endpoints and runtime tuning knobs.

        Args:
            max_pages: Optional maximum number of listing pages to visit.

        Returns:
            None

        Raises:
            None
        """
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.visited_job_ids: Set[str] = set()
        self.max_pages: Optional[int] = max_pages

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

        if getattr(self, "testing", False):
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 0
            except Exception:
                job_limit = 15
            # Do NOT force total_pages=1; let the loop paginate until job_limit is reached.
            # This keeps default 15 fast, and allows values >15 (e.g., --limit 20) to span pages.
        else:
            job_limit = float("inf")  # type: ignore[assignment]
            if self.max_pages:
                total_pages = min(total_pages, self.max_pages)

        all_job_links: List[Dict[str, str]] = []
        for page_num in range(1, total_pages + 1):
            page_links = self.get_job_links(page_num)
            self.log("list:fetched", page=page_num, count=len(page_links))

            for link in page_links:
                if len(all_job_links) >= job_limit:
                    break
                all_job_links.append(link)

            # If we hit the cap inside this page, stop paging.
            if len(all_job_links) >= job_limit:
                break

        self.log("list:done", reason="end")
        return all_job_links

    def parse_job(self, job_entry: Dict[str, str]) -> Dict[str, str]:
        """
        Convert the listing entry to a minimal record + artifacts for canonicalization.
        """
        url = job_entry["Detail URL"]
        job_id = job_entry["Posting ID"]
        artifacts = fetch_detail_artifacts(
            self.thread_get, self.log, url, get_vendor_blob=False, get_datalayer=False
        )
        jsonld = artifacts.get("_jsonld")
        meta = artifacts.get("_meta")
        soup = BS(artifacts.get("_html"), "lxml")
        blocks = extract_bold_block(soup)
        return {
            "Posting ID": job_id,
            "Position Title": jsonld.get("title"),
            "Detail URL": artifacts.get("_canonical_url") or url,
            "Description": "; ".join(blocks.values()),
            "Post Date": jsonld.get("datePosted"),
            "Required Education": jsonld.get("educationRequirements"),
            "Clearance Level Must Possess": jsonld.get("employmentType"),
            "Required Skills": jsonld.get("qualifications"),
            "City": jsonld.get("jobLocation.0.address.addressLocality"),
            "State": jsonld.get("jobLocation.0.address.addressRegion"),
            "Country": jsonld.get("jobLocation.0.address.addressCountry"),
            "Postal Code": jsonld.get("jobLocation.0.address.postalCode"),
            "Business Sector": meta.get("gtm_tbcn_division"),
            "Raw Location": meta.get("gtm_tbcn_location"),
            "Business Area": jsonld.get("industry"),
        }

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
        response = self.get(self.SEARCH_URL)
        response.raise_for_status()
        soup = BS(response.text, "lxml")
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
        response = self.get(page_url)
        response.raise_for_status()

        soup = BS(response.text, "lxml")
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
            job_links.append({"Posting ID": job_id, "Detail URL": full_url})

        return job_links
