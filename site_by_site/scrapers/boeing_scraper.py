"""
Boeing scraper.

Lists job result pages from the public Boeing careers search UI, gathers
per-job links/IDs, and then parses each job detail page (preferring JSON-LD)
into a normalized record for export.
"""

from __future__ import annotations

from time import sleep
from typing import Any, Dict, List, Optional, Set

from bs4 import BeautifulSoup as BS

from scrapers.base import JobScraper
from utils.extractors import extract_bold_block
from utils.detail_fetchers import fetch_detail_artifacts


class BoeingScraper(JobScraper):
    """
    Scraper for Boeing job postings.

    Workflow:
      1) Determine total listing pages from the search results page.
      2) Iterate result pages to collect job links and IDs (de-duped per run).
      3) Visit each job detail page and parse JSON-LD and tagged fields.
    """

    VENDOR = "Boeing"
    BASE_URL = "https://jobs.boeing.com"
    SEARCH_URL = f"{BASE_URL}/search-jobs"

    def __init__(self, max_pages: Optional[int] = None, delay: float = 0.5) -> None:
        """
        Initialize the scraper with UI endpoints and runtime tuning knobs.

        Args:
            max_pages: Optional maximum number of listing pages to visit.
            delay: Sleep (seconds) between page fetches to be polite.
        """
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.visited_job_ids: Set[str] = set()
        self.max_pages: Optional[int] = max_pages
        self.delay = delay

    # -------------------------------------------------------------------------
    # Listing helpers
    # -------------------------------------------------------------------------
    def get_total_pages(self) -> int:
        """
        Read pagination metadata from the first search-results page.

        Returns:
            Total number of result pages (>=1).

        Raises:
            requests.RequestException: If the GET fails.
            ValueError: If required pagination attributes are missing/invalid.
        """
        resp = self.get(self.SEARCH_URL)
        resp.raise_for_status()
        soup = BS(resp.text, "lxml")

        section = soup.select_one("section.search-results#search-results")
        if section is None:
            raise ValueError(
                "Unable to locate <section id='search-results'> for Boeing"
            )

        total_pages_str = section.get("data-total-pages") or "1"
        total_results_str = section.get("data-total-results") or "0"

        try:
            total_pages = int(total_pages_str)
        except ValueError as e:
            raise ValueError(
                f"Invalid data-total-pages value: {total_pages_str!r}"
            ) from e

        try:
            total_results = int(total_results_str)
        except ValueError:
            total_results = 0

        self.log("source:total", total_results=total_results, total_pages=total_pages)

        if total_pages < 1:
            total_pages = 1

        if self.max_pages:
            total_pages = min(total_pages, self.max_pages)

        return total_pages

    def get_job_links(self, page_num: int) -> List[Dict[str, str]]:
        """
        Collect job links/IDs from a single search-results page.

        Args:
            page_num: 1-based page index.

        Returns:
            List of listing dicts with keys:
              - "Posting ID"
              - "Detail URL"
              - "Position Title"
              - "Raw Location"
              - "Post Date"
        """
        if page_num <= 1:
            page_url = self.SEARCH_URL
        else:
            # TalentBrew paging convention: ?p=N
            page_url = f"{self.SEARCH_URL}?p={page_num}"

        resp = self.get(page_url)
        resp.raise_for_status()
        soup = BS(resp.text, "lxml")

        container = soup.select_one("#search-results-list")
        if container is None:
            self.log("list:empty_page", page=page_num)
            return []

        results: List[Dict[str, str]] = []

        # Each job is a <li> containing an <a.search-results__job-link data-job-id="...">
        for li in container.select("ul > li"):
            a = li.select_one("a.search-results__job-link[data-job-id]")
            if a is None:
                continue

            job_id = (a.get("data-job-id") or "").strip()
            if not job_id:
                continue
            if job_id in self.visited_job_ids:
                # De-dupe across pages
                continue
            self.visited_job_ids.add(job_id)

            href = a.get("href") or ""
            if href.startswith("http"):
                url = href
            else:
                url = f"{self.BASE_URL}{href}"

            title_el = a.select_one(".search-results__job-title")
            title = (
                title_el.get_text(strip=True)
                if title_el is not None
                else a.get_text(strip=True)
            )

            loc_el = li.select_one(".search-results__job-info.location")
            date_el = li.select_one(".search-results__job-info.date")
            raw_location = loc_el.get_text(strip=True) if loc_el else ""
            post_date = date_el.get_text(strip=True) if date_el else ""

            results.append(
                {
                    "Posting ID": job_id,
                    "Detail URL": url,
                    "Position Title": title,
                    "Raw Location": raw_location,
                    "Post Date": post_date,
                }
            )

        return results

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, str]]:
        """
        Collect job links/IDs from all relevant search-results pages.

        Returns:
            List of shallow listing dicts, each suitable for `parse_job`.
        """
        total_pages = self.get_total_pages()
        self.log("list:pages", total_pages=total_pages)

        if getattr(self, "testing", False):
            # --limit for test runs
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 0
            except Exception:
                job_limit = 15
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

            sleep(self.delay)

            if len(all_job_links) >= job_limit:
                self.log("list:done", reason="test_limit")
                break

        return all_job_links

    # -------------------------------------------------------------------------
    # Detail parsing
    # -------------------------------------------------------------------------
    def parse_job(self, job_entry: Dict[str, str]) -> Dict[str, Any]:
        """
        Fetch and normalize a single Boeing job posting.

        Args:
            job_entry: Listing dict from `fetch_data` / `get_job_links`.

        Returns:
            Flattened record with canonical fields (Position Title, Posting ID,
            location, business/sector, description, etc.).
        """
        url = job_entry.get("Detail URL") or ""
        if not url:
            raise ValueError("Missing Detail URL in job_entry")

        artifacts = fetch_detail_artifacts(
            self.thread_get,
            self.log,
            url,
            get_vendor_blob=False,
            get_datalayer=False,
        )
        jsonld: Dict[str, Any] = artifacts.get("_jsonld") or {}
        meta: Dict[str, Any] = artifacts.get("_meta") or {}
        html = artifacts.get("_html") or ""
        soup = BS(html, "lxml") if html else None
        canonical_url = artifacts.get("_canonical_url") or url

        # ID & title
        job_id = (
            job_entry.get("Posting ID")
            or jsonld.get("identifier.value")
            or meta.get("gtm_jobid")
            or ""
        )
        title = (
            job_entry.get("Position Title")
            or jsonld.get("title")
            or meta.get("gtm_tbcn_jobtitle")
            or ""
        )

        # Location
        raw_location = (
            job_entry.get("Raw Location") or meta.get("gtm_tbcn_location") or ""
        )
        city = jsonld.get("jobLocation.0.address.addressLocality")
        state = jsonld.get("jobLocation.0.address.addressRegion")
        country = jsonld.get("jobLocation.0.address.addressCountry")

        # Business / function
        business_area = jsonld.get("industry")
        business_sector = meta.get("gtm_tbcn_division")
        job_function = meta.get("gtm_tbcn_jobcategory")

        # Description: prefer labeled bold blocks, fall back to JSON-LD description
        description = ""
        if soup is not None:
            blocks = extract_bold_block(soup)
            if blocks:
                description = "; ".join(blocks.values())
        if not description:
            description = jsonld.get("description") or ""

        # Employment type as a stand-in clearance-ish field if you want parity
        employment_type = jsonld.get("employmentType")

        record: Dict[str, Any] = {
            "Vendor": self.VENDOR,
            "Posting ID": job_id,
            "Position Title": title,
            "Detail URL": canonical_url,
            "Raw Location": raw_location,
            "City": city,
            "State": state,
            "Country": country,
            "Business Area": business_area,
            "Business Sector": business_sector,
            "Job Function/Discipline": job_function,
            "Clearance Level Must Possess": employment_type,
            "Clearance Level Must be Able to Obtain": None,
            "Description": description,
        }

        # Surface a few useful meta fields for debugging/analysis
        record["Source Meta.gtm_tbcn_division"] = meta.get("gtm_tbcn_division")
        record["Source Meta.gtm_tbcn_jobcategory"] = meta.get("gtm_tbcn_jobcategory")
        record["Source Meta.gtm_tbcn_location"] = meta.get("gtm_tbcn_location")

        return record
