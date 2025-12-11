"""
L3Harris scraper.

This scraper:
  1) Uses the public search UI listing pages to collect job links + IDs.
  2) Uses the job detail pages' JSON-LD and meta tags to build normalized
     records suitable for your canonical pipeline.

Deliberately *avoids* arbitrary heuristics:
  - Pagination count is taken directly from the `data-total-pages` attribute
    on `<section id="search-results">`. If this attribute is missing or
    non-numeric, we raise instead of guessing.
  - We do not try to infer locations, dates, or other fields from free-text.
    If structured values are not present in JSON-LD / meta, they are left
    as None.

The only structural assumption (documented below) is the page parameter
for additional listing pages (`?p=`), which is how other TalentBrew
deployments expose pagination (e.g. Lockheed).
"""

from __future__ import annotations

from time import sleep
from typing import Any, Dict, List, Optional, Set

from bs4 import BeautifulSoup as BS

from scrapers.base import JobScraper
from utils.detail_fetchers import fetch_detail_artifacts


class L3HarrisScraper(JobScraper):
    """
    Scraper for L3Harris job postings.

    Workflow:
      1) Determine total listing pages from the search results `data-total-pages`.
      2) Iterate result pages to collect unique (Posting ID, Detail URL) pairs.
      3) Visit each job detail page and parse JSON-LD + tagged fields.
    """

    VENDOR = "L3Harris"

    BASE_URL = "https://careers.l3harris.com"
    SEARCH_URL = f"{BASE_URL}/en/search-jobs"

    def __init__(self, max_pages: Optional[int] = None, delay: float = 0.5) -> None:
        """
        Args:
            max_pages: Optional hard cap on number of listing pages to visit.
            delay: Sleep (seconds) between listing page fetches.
        """
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.visited_job_ids: Set[str] = set()
        self.max_pages: Optional[int] = max_pages
        self.delay = delay

    # ------------------------------------------------------------------ #
    # Listing-side: fetch listing pages and collect job links/IDs
    # ------------------------------------------------------------------ #

    def fetch_data(self) -> List[Dict[str, str]]:
        """
        Collect job links/IDs from the search results pages.

        Returns:
            A list of dicts like:
              {"Posting ID": "...", "Detail URL": "https://..."}

        Raises:
            requests.RequestException: If listing pages cannot be fetched.
            ValueError: If the pagination metadata is malformed.
        """
        total_pages = self.get_total_pages()
        self.log("list:pages", total_pages=total_pages)

        job_limit: float
        if getattr(self, "testing", False):
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 15
            except Exception:
                job_limit = 15
        else:
            job_limit = float("inf")

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

            if len(all_job_links) >= job_limit:
                break

            sleep(self.delay)

        self.log("list:done", total=len(all_job_links))
        return all_job_links

    def get_total_pages(self) -> int:
        """
        Inspect the search page and return the total number of result pages.

        Returns:
            Positive integer count of available pages.

        Raises:
            requests.RequestException: If the search page cannot be fetched.
            ValueError: If the 'data-total-pages' attribute is missing or malformed.
        """
        response = self.get(self.SEARCH_URL)
        response.raise_for_status()

        soup = BS(response.text, "lxml")
        pagination = soup.select_one("section#search-results")
        if not pagination:
            raise ValueError("Missing <section id='search-results'> on listing page")

        total_raw = pagination.get("data-total-pages")
        if total_raw is None:
            raise ValueError(
                "Missing 'data-total-pages' on <section id='search-results'>"
            )

        try:
            total = int(total_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Unexpected data-total-pages value: {total_raw!r}"
            ) from exc

        if total < 1:
            raise ValueError(f"Non-positive data-total-pages value: {total!r}")

        return total

    def get_job_links(self, page_num: int) -> List[Dict[str, str]]:
        """
        Collect unique (Posting ID, Detail URL) pairs from a given search
        results page.

        Args:
            page_num: 1-based index of the results page to fetch.

        Returns:
            List of dicts with 'Posting ID' and 'Detail URL' keys;
            duplicates (per run) are skipped.

        Raises:
            requests.RequestException: If the page cannot be fetched.
        """
        page_url = f"{self.SEARCH_URL}?p={page_num}"
        response = self.get(page_url)
        response.raise_for_status()

        soup = BS(response.text, "lxml")
        job_links: List[Dict[str, str]] = []

        for link in soup.select("section#search-results-list a[data-job-id]"):
            job_id = link.get("data-job-id")
            href = link.get("href")

            if not job_id or not href:
                continue
            if job_id in self.visited_job_ids:
                continue

            self.visited_job_ids.add(job_id)

            if href.startswith("http://") or href.startswith("https://"):
                full_url = href
            else:
                if not href.startswith("/"):
                    href = "/" + href
                full_url = f"{self.BASE_URL}{href}"

            job_links.append(
                {
                    "Posting ID": job_id,
                    "Detail URL": full_url,
                }
            )

        return job_links

    # ------------------------------------------------------------------ #
    # Detail-side: enrich each listing via its detail page
    # ------------------------------------------------------------------ #

    def parse_job(self, job_entry: Dict[str, str]) -> Dict[str, Any]:
        """
        Convert a listing entry into a normalized job record.

        Args:
            job_entry: Raw listing item as returned by `fetch_data`.

        Returns:
            A normalized job record dictionary suitable for canonicalization.

        Raises:
            Any exception here is caught/logged by JobScraper.run() as
            'parse:error', and the pipeline continues with the next record.
        """
        url = job_entry["Detail URL"]
        job_id = job_entry["Posting ID"]

        artifacts = fetch_detail_artifacts(
            self.thread_get,
            self.log,
            url,
            get_vendor_blob=False,
            get_datalayer=False,
        )

        jsonld: Dict[str, Any] = artifacts.get("_jsonld") or {}
        meta: Dict[str, str] = artifacts.get("_meta") or {}

        position_title = jsonld.get("title")
        description = jsonld.get("description")
        post_date = jsonld.get("datePosted")

        city = jsonld.get("jobLocation.0.address.addressLocality")
        state = jsonld.get("jobLocation.0.address.addressRegion")
        country = jsonld.get("jobLocation.0.address.addressCountry")
        postal_code = jsonld.get("jobLocation.0.address.postalCode")

        employment_type = jsonld.get("employmentType")
        ats_req_id = meta.get("meta.job-ats-req-id")
        raw_location = meta.get("meta.gtm_tbcn_location")
        business_sector = meta.get("meta.gtm_tbcn_division")

        detail_url = artifacts.get("_canonical_url") or url

        record: Dict[str, Any] = {
            "Posting ID": ats_req_id or job_id,
            "Position Title": position_title,
            "Detail URL": detail_url,
            "Description": description,
            "Post Date": post_date,
            "Full Time Status": employment_type,
            "Raw Location": raw_location,
            "Country": country,
            "State": state,
            "City": city,
            "Postal Code": postal_code,
            "Business Sector": business_sector,
        }

        return record
