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
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup as BS

from scrapers.engine import JobScraper
from utils.http import b64url_encode
from utils.extractors import extract_bold_block
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

    # Used by the base pipeline for CSV/DB exports and incremental skip checks
    VENDOR = "General Dynamics"
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
        fetched = 0
        if getattr(self, "testing", False):
            try:
                target = int(getattr(self, "test_limit", 40)) or 0
            except Exception:
                target = 40
        else:
            target = 10**12
        start_time = time()

        def make_payload(p: int, ps: int, facet: bool) -> Dict[str, Any]:
            return {
                "address": [],
                "facets": (
                    [
                        {
                            "name": "career_page_size",
                            "values": [{"value": "200 Jobs Per Page"}],
                        }
                    ]
                    if facet
                    else []
                ),
                "page": p,
                "pageSize": ps,
                "what": "",
                "usedPlacesApi": False,
            }

        # ---- Step 1: learn a stable mode on page 0 (facet / page_size) ----
        page0_data: Optional[Dict[str, Any]] = None
        while True:
            try:
                tok0 = b64url_encode(make_payload(0, page_size, use_facet))
                page0_data, _ = self.call_api(tok0)
                break
            except requests.HTTPError as e:
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
                        page_size = 100
                        self.log(
                            "api:retry_mode",
                            reason="400",
                            use_facet=use_facet,
                            page_size=page_size,
                        )
                        continue
                raise

        total = int(page0_data.get("ResultTotal") or 0)
        self.log("source:total", total=total)

        pc = page0_data.get("PageCount")
        pc_int = (
            int(pc)
            if isinstance(pc, int) or (isinstance(pc, str) and pc.isdigit())
            else 0
        )
        calc = ceil(total / page_size) if total else 0
        max_pages = min(pc_int, calc) if (pc_int and calc) else (pc_int or calc or 1)

        # ---- Collect page 0 results ----
        results0 = page0_data.get("Results") or []
        self.log("list:page", page=0, page_size=page_size, got=len(results0))
        for item in results0:
            link = (item.get("Link") or {}).get("Url")
            if not link:
                continue
            loc0 = ((item.get("Locations") or [{}])[0]) or {}
            workplace_options = item.get("WorkplaceOptions") or []
            jobs.append(
                {
                    "Detail URL": link,
                    "Posting ID": item.get("ReferenceCode"),
                    "Full Time Status": ", ".join(item.get("EmploymentTypes")),
                    "Job Category": item.get("Category"),
                    "Clearance Level Must Possess": item.get("Clearance"),
                    "Position Title": item.get("Title"),
                    "Post Date": item.get("Date"),
                    "Country": loc0.get("Country"),
                    "State": loc0.get("State"),
                    "Latitude": loc0.get("Latitude"),
                    "Longitude": loc0.get("Longitude"),
                    "Business Area": item.get("Company"),
                    "Raw Location": loc0.get("Name"),
                    "Remote Status": ""
                    if len(workplace_options) == 0
                    else ", ".join(workplace_options),
                }
            )
            fetched += 1
            if fetched >= target:
                break

        if fetched >= target or max_pages <= 1:
            dur = time() - start_time
            self.log("list:fetched", count=len(jobs))
            self.log("run:segment", segment="gd.fetch_data", seconds=round(dur, 3))
            return jobs

        # ---- Step 2: fan out pages 1..N-1 concurrently under the learned mode ----
        list_workers = getattr(self, "list_workers", 6)

        def fetch_page(p: int) -> List[Dict[str, Any]]:
            token = b64url_encode(make_payload(p, page_size, use_facet))
            data, _ = self.call_api(token)
            self.log(
                "list:page",
                page=p,
                page_size=page_size,
                got=len(data.get("Results") or []),
            )
            return data.get("Results") or []

        futures = {}
        with ThreadPoolExecutor(max_workers=list_workers) as ex:
            for p in range(1, max_pages):
                futures[ex.submit(fetch_page, p)] = p

            # preserve deterministic order by staging in an array
            page_results: List[Optional[List[Dict[str, Any]]]] = [None] * (
                max_pages - 1
            )
            for fut in as_completed(futures):
                p = futures[fut]
                try:
                    page_results[p - 1] = fut.result()
                except Exception:
                    # log but keep going
                    self.log(
                        "api:page_error",
                        level="warning",
                        page=p,
                        error=format_exc(),
                        use_facet=use_facet,
                        page_size=page_size,
                    )

        for res in page_results:
            if not res:
                continue
            for item in res:
                if fetched >= target:
                    break
                link = (item.get("Link") or {}).get("Url")
                if not link:
                    continue
                loc0 = ((item.get("Locations") or [{}])[0]) or {}
                workplace_options = item.get("WorkplaceOptions") or []
                jobs.append(
                    {
                        "Detail URL": link,
                        "Posting ID": item.get("ReferenceCode"),
                        "Full Time Status": ", ".join(item.get("EmploymentTypes")),
                        "Job Category": item.get("Category"),
                        "Clearance Level Must Possess": item.get("Clearance"),
                        "Position Title": item.get("Title"),
                        "Post Date": item.get("Date"),
                        "Country": loc0.get("Country"),
                        "State": loc0.get("State"),
                        "Latitude": loc0.get("Latitude"),
                        "Longitude": loc0.get("Longitude"),
                        "Business Area": item.get("Company"),
                        "Raw Location": loc0.get("Name"),
                        "Remote Status": ""
                        if len(workplace_options) == 0
                        else ", ".join(workplace_options),
                    }
                )
                fetched += 1
            if fetched >= target:
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
        detail_rel = raw_job.get("Detail URL", "")
        detail_url = urljoin("https://www.gd.com", detail_rel or "")
        record = dict(raw_job)
        record["Detail URL"] = detail_url
        record["Position Title"] = record.get("Position Title", "")
        try:
            self.log("detail:fetch", url=detail_url)
            # Use the unified artifact fetcher (rides this scraper's session)
            artifacts = fetch_detail_artifacts(
                self.thread_get,
                self.log,
                detail_url,
                get_vendor_blob=False,
                get_jsonld=False,
                get_meta=False,
                get_datalayer=False,
            )
            html = artifacts.get("_html", "")
            soup = BS(html, "lxml")
            blocks = extract_bold_block(soup)

            desc = "; ".join(blocks.values())
            record.update(
                {
                    "Description": desc,
                    "Career Level": blocks.get("Career Level"),
                }
            )

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
