"""
General Dynamics scraper.

Implements listing retrieval via the public Careers API and enriches each record
by fetching and parsing the associated detail page (HTML). Listing pagination
uses a base64url-encoded request payload; when the API returns HTTP 400 for
certain facet/page-size combinations, the scraper downgrades capability
(`api:retry_mode`) and continues.

Logging is standardized via `JobScraper.log(event, **kv)` with a `scraper`
field injected by the base class' LoggerAdapter.
"""

from __future__ import annotations

from time import time
from math import ceil
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlunparse, urljoin

import requests
from bs4 import BeautifulSoup as BS

from scrapers.base import JobScraper
from utils.http import b64url_encode
from utils.extractors import extract_insets, extract_bold_block
from utils.detail_fetchers import fetch_detail_artifacts
from traceback import format_exc


class GeneralDynamicsScraper(JobScraper):
    """
    Scraper for General Dynamics job postings.

    The workflow:
      1) Call the careers API with a base64url-encoded payload to obtain listing pages.
      2) Iterate results, building shallow records with title/location/ID/URL.
      3) For each record, fetch the HTML detail page and extract labeled fields
         (insets, bold block content) into a flattened dictionary.

    Attributes:
        START_URL: Landing page used for session warm-up.
        JOB_SEARCH_URL: Search page URL used as the HTTP referer.
        API_PATH: Path component for the career search API.
        session: `requests.Session` with default headers and referer set.
    """

    START_URL = "https://www.gd.com/careers"
    JOB_SEARCH_URL = "https://www.gd.com/careers/job-search"
    API_PATH = "/API/Careers/CareerSearch"

    def __init__(self) -> None:
        """
        Initialize the scraper and warm the HTTP session.

        Args:
            None

        Returns:
            None

        Raises:
            requests.RequestException: If the warm-up GET to `JOB_SEARCH_URL` fails.
        """
        super().__init__(base_url=self.START_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.suppress_console = False
        self.testing = getattr(self, "testing", False)

        self.session = requests.Session()
        self.enable_retries(self.session)
        self.session.headers.update(
            {
                "User-Agent": self.headers.get("User-Agent", "Mozilla/5.0"),
                "Accept": "application/json, text/plain, */*",
                "Referer": self.JOB_SEARCH_URL,
            }
        )

        # Prime cookies/anti-bot state
        self.session.get(self.JOB_SEARCH_URL, timeout=30)

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------
    def raw_id(self, raw_job: Dict[str, Any]) -> Optional[str]:
        """
        Return the stable raw identifier for de-duplication.

        Args:
            raw_job: Raw listing item as returned by the careers API.

        Returns:
            The job's unique identifier string, or None if unavailable.

        Raises:
            None
        """
        return raw_job.get("id")

    # -------------------------------------------------------------------------
    # Detail fetch/parse helpers
    # -------------------------------------------------------------------------
    def parse_job_detail_doc(self, html: str) -> Dict[str, str]:
        """
        Parse a job detail HTML document and extract labeled fields.

        Args:
            html: Raw HTML of the job detail page.

        Returns:
            Dictionary of extracted fields (insets + bold blocks).

        Raises:
            None
        """
        soup = BS(html, "html.parser")
        out: Dict[str, str] = {}
        out.update(extract_insets(soup))
        out.update(extract_bold_block(soup))
        return out

    # -------------------------------------------------------------------------
    # API interaction
    # -------------------------------------------------------------------------
    def call_api(self, request_token: str) -> Tuple[Dict[str, Any], str]:
        """
        Perform a GET request to the careers API with an encoded payload.

        Args:
            request_token: Base64url-encoded JSON request payload.

        Returns:
            A tuple of (parsed JSON dict, request URL used).

        Raises:
            requests.RequestException: If the API request fails.
            ValueError: If the response cannot be parsed as JSON.
        """
        url = urlunparse(
            (
                "https",
                "www.gd.com",
                self.API_PATH,
                "",
                urlencode({"request": request_token}),
                "",
            )
        )
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.json(), url

    def build_payload(self, page: int, page_size: int = 200) -> Dict[str, Any]:
        """
        Build the careers API payload for a given page and size.

        Args:
            page: Zero-based page index.
            page_size: Number of results per page (server may cap this).

        Returns:
            JSON-serializable dictionary representing the request body.

        Raises:
            None
        """
        return {
            "address": [],
            "facets": [
                {"name": "career_page_size", "values": [{"value": "200 Jobs Per Page"}]}
            ],
            "page": page,
            "pageSize": page_size,
            "what": "",
            "usedPlacesApi": False,
        }

    # -------------------------------------------------------------------------
    # Lifecycle overrides
    # -------------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Retrieve job listings from the General Dynamics careers API.

        Returns:
            List of raw listing objects (each includes id, title, location, url).

        Raises:
            requests.RequestException: If API requests fail irrecoverably.
            ValueError: If the API returns malformed JSON or missing fields.
        """
        page_size = 200
        use_facet = True
        jobs: List[Dict[str, Any]] = []
        total: Optional[int] = None
        max_pages: Optional[int] = None
        page = 0
        fetched = 0
        target = 40 if self.testing else 10**12
        start_time = time()

        while True:
            if use_facet:
                payload = {
                    "address": [],
                    "facets": [
                        {
                            "name": "career_page_size",
                            "values": [{"value": "200 Jobs Per Page"}],
                        }
                    ],
                    "page": page,
                    "pageSize": page_size,
                    "what": "",
                    "usedPlacesApi": False,
                }
            else:
                payload = {
                    "address": [],
                    "facets": [],
                    "page": page,
                    "pageSize": page_size,
                    "what": "",
                    "usedPlacesApi": False,
                }
            token = b64url_encode(payload)
            try:
                data, _ = self.call_api(token)
            except requests.HTTPError as e:
                # The API sometimes returns 400 for facet/page size combos; downgrade
                if e.response is not None and e.response.status_code == 400:
                    if use_facet:
                        use_facet = False
                        self.log(
                            "api:retry_mode",
                            reason="400",
                            use_facet=use_facet,
                            page_size=page_size,
                        )
                        continue
                    if page_size > 100:
                        offset = fetched
                        page_size = 100
                        page = offset // page_size
                        max_pages = None
                        self.log(
                            "api:retry_mode",
                            reason="400",
                            use_facet=use_facet,
                            page_size=page_size,
                        )
                        continue
                    break
                raise

            if total is None:
                total = int(data.get("ResultTotal") or 0)
                self.log("source:total", total=total)

            if max_pages is None:
                pc = data.get("PageCount")
                pc = (
                    int(pc)
                    if isinstance(pc, int) or (isinstance(pc, str) and pc.isdigit())
                    else 0
                )
                calc = ceil(total / page_size) if total else 0
                if pc and calc:
                    max_pages = min(pc, calc)
                else:
                    max_pages = pc or calc or None

            results = data.get("Results") or []
            self.log("list:page", page=page, page_size=page_size, got=len(results))

            for item in results:
                link = (item.get("Link") or {}).get("Url") or ""
                if link:
                    jobs.append(
                        {
                            "detail_url": link,
                            "title": item.get("Title") or "",
                            "id": str(item.get("Id") or ""),
                            "company": item.get("Company") or "",
                            "location": "; ".join(item.get("LocationNames") or []),
                        }
                    )
                    fetched += 1
                    if fetched >= target:
                        break

            if fetched >= target:
                break
            page += 1
            if max_pages is not None and page >= max_pages:
                break
            if not results:
                break

        dur = time() - start_time
        self.log("list:fetched", count=len(jobs))
        self.log("run:segment", segment="gd.fetch_data", seconds=round(dur, 3))
        return jobs

    def parse_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a raw listing into a normalized job record with detail fields.

        Args:
            raw_job: One raw listing object as returned by `fetch_data()`.

        Returns:
            Dictionary with normalized fields including 'Detail URL',
            'Position Title', 'Company', 'Location', 'Posting ID', and any
            fields extracted from the detail HTML.

        Raises:
            None. Network and parse errors during detail enrichment are caught
            and logged as 'detail:http_error' or 'detail:parse_error'. A soft
            error counter is emitted as 'parse:errors_detail' when non-fatal
            parse issues occur.
        """
        warns = 0
        detail_rel = raw_job.get("detail_url", "")
        detail_url = urljoin("https://www.gd.com", detail_rel or "")
        record = {
            "detail_url": detail_url,
            "title": raw_job.get("title", ""),
            "location": raw_job.get("location", ""),
            "posting_id": raw_job.get("id", ""),
        }
        try:
            self.log("detail:fetch", url=detail_url)
            # Use the unified artifact fetcher (rides this scraper's session)
            artifacts = fetch_detail_artifacts(self.session.get, self.log, detail_url)
            html = artifacts.get("_html", "")
            if html:
                doc = self.parse_job_detail_doc(html)
                record.update(doc)
            # Prefer canonical URL if present
            canon = artifacts.get("_canonical_url")
            if canon:
                record["Detail URL"] = canon
        except requests.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            self.log(
                "detail:http_error",
                level="warning",
                url=detail_url,
                status=status,
                error=format_exc(),
            )
        except Exception:
            self.log(
                "detail:parse_error",
                level="warning",
                url=detail_url,
                error=format_exc(),
            )
            warns += 1

        if warns:
            self.log("parse:errors_detail", n=warns)

        return record
