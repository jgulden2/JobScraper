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

import base64
import json
import re
from time import time
from math import ceil
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlunparse

import requests
from bs4 import BeautifulSoup as BS

from scrapers.base import JobScraper
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
    # Identity / URL helpers
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

    def b64url_decode(self, s: str) -> str:
        """
        Decode a base64url string to UTF-8 text (with missing padding tolerated).

        Args:
            s: Base64url text (padding optional).

        Returns:
            Decoded UTF-8 string.

        Raises:
            binascii.Error: If the input contains invalid base64url characters.
            UnicodeDecodeError: If the decoded bytes are not valid UTF-8.
        """
        s += "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s.encode("utf-8")).decode("utf-8")

    def b64url_encode(self, obj: str) -> str:
        """
        Encode a Python object to compact JSON, then base64url without padding.

        Args:
            obj: JSON-serializable object.

        Returns:
            Base64url-encoded string with trailing '=' padding removed.

        Raises:
            TypeError: If `obj` is not JSON-serializable.
        """
        raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    def absolute_url(self, href: Optional[str]) -> str:
        """
        Convert a relative link to an absolute URL on the gd.com domain.

        Args:
            href: Relative or absolute link (may be None or empty).

        Returns:
            Absolute URL as a string. Empty input becomes the site root.

        Raises:
            None
        """
        return urljoin("https://www.gd.com", href or "")

    # -------------------------------------------------------------------------
    # HTML utilities
    # -------------------------------------------------------------------------
    def text(self, node: Any) -> str:
        """
        Extract plain text from HTML, collapsing whitespace.

        Args:
            node: HTML node or markup snippet.

        Returns:
            Plain text content with spaces normalized.

        Raises:
            None
        """
        return BS(str(node), "html.parser").get_text(" ", strip=True)

    def flatten(self, prefix: str, obj: Any, out: Dict[str, Any]) -> None:
        """
        Flatten nested dicts/lists into dotted keys in-place.

        Args:
            prefix: Current key path ('' for root).
            obj: The value at the current path (dict, list, or scalar).
            out: Destination dictionary populated with flattened keys.

        Returns:
            None

        Raises:
            None
        """
        if isinstance(obj, dict):
            for k, v in obj.items():
                self.flatten(f"{prefix}.{k}" if prefix else k, v, out)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                self.flatten(f"{prefix}[{i}]", v, out)
        else:
            out[prefix] = obj

    def collect_until_next_b(self, start_b: Any) -> str:
        """
        Collect text and list items that follow a <b> label until the next <b>.

        Handles sequences like:
            <b>Location:</b> USA AL Huntsville<br>...
            <b>Job Duties and Responsibilities</b><ul><li>...</li></ul>

        Args:
            start_b: A BeautifulSoup node pointing to a <b> element.

        Returns:
            A single string with line breaks preserved for list items.

        Raises:
            None
        """
        parts: List[str] = []
        list_items: Optional[List[str]] = None
        for sib in start_b.next_siblings:
            # Stop when we hit the next <b> label
            if getattr(sib, "name", None) == "b":
                break
            # Collect lists as arrays
            if getattr(sib, "name", None) == "ul":
                items: List[str] = []
                for li in sib.find_all("li"):
                    items.append(self.text(li))
                list_items = (list_items or []) + items
                continue
            # Everything else as text (handles <br>, <p>, strings, etc.)
            if isinstance(sib, str):
                parts.append(sib)
            else:
                parts.append(self.text(sib))
        # Normalize text
        value = " ".join(p.strip() for p in parts if p and p.strip())
        val_raw = list_items if list_items is not None else value
        if isinstance(val_raw, list):
            val = "\n".join(x.strip() for x in val_raw if isinstance(x, str))
        else:
            val = (val_raw or "").strip()
        return val

    def extract_insets(self, soup: BS) -> Dict[str, str]:
        """
        Extract inset values from the detail page (location, etc.).

        Args:
            soup: Parsed BeautifulSoup document for a job detail page.

        Returns:
            Mapping from 'inset.<Label>' to string values.

        Raises:
            None
        """
        out: Dict[str, str] = {}
        for dl in soup.select(".career-search-result__insets dl"):
            dt = dl.find("dt")
            dts = dt.get_text(" ", strip=True) if dt else ""
            # Some have icon-only <dt> (e.g., location). Use dd when dt is empty.
            dds = "; ".join(dd.get_text(" ", strip=True) for dd in dl.find_all("dd"))
            key = dts if dts else "Location"
            if key and dds:
                out[f"inset.{key}"] = dds
        return out

    def extract_bold_block(self, soup: BS) -> Dict[str, str]:
        """
        Parse labeled blocks under '.career-detail-description'.

        Args:
            soup: Parsed BeautifulSoup document.

        Returns:
            Flat mapping of labels to strings. For sections that are lists,
            items are joined with '; '. Adds 'Page Title' if a title is found.

        Raises:
            None
        """
        data: Dict[str, Any] = {}
        container = soup.select_one(".career-detail-description") or soup
        for b in container.find_all("b"):
            label = self.text(b).rstrip(":").strip()
            if not label:
                continue
            val = self.collect_until_next_b(b)
            # If multiple same labels appear, keep the richest (list beats text, longer text beats shorter)
            if label in data:
                cur = data[label]

                def score(v: Any) -> Tuple[int, int]:
                    return (1, len(v)) if isinstance(v, list) else (0, len(v or ""))

                if score(val) > score(cur):
                    data[label] = val
            else:
                data[label] = val
        # Flatten lists into strings (or keep lists if you prefer arrays)
        for k, v in list(data.items()):
            if isinstance(v, list):
                data[k] = "; ".join(v)
        # Add the H1 title as a convenience if present
        h1 = soup.select_one(".career-detail-title, h1")
        if h1 and "Page Title" not in data:
            data["Page Title"] = self.text(h1)
        # Type-narrow to str values
        return {k: str(v) for k, v in data.items()}

    def extract_jsonld(self, soup: BS) -> Dict[str, Any]:
        """
        Extract and flatten JSON-LD script blocks into a dotted-key mapping.

        Args:
            soup: Parsed BeautifulSoup document.

        Returns:
            Dictionary containing flattened JSON-LD data with prefixes 'ld' or 'ld[i]'.

        Raises:
            None (malformed/empty blocks are ignored).
        """
        out: Dict[str, Any] = {}
        blocks = soup.find_all("script", attrs={"type": "application/ld+json"})
        for b in blocks:
            try:
                data = json.loads(b.string or b.get_text() or "")
            except Exception:
                continue
            if isinstance(data, list):
                for i, item in enumerate(data):
                    self.flatten(f"ld[{i}]", item, out)
            else:
                self.flatten("ld", data, out)
        return out

    def extract_meta(self, soup: BS) -> Dict[str, str]:
        """
        Extract basic <meta> values and the first <h1> when available.

        Args:
            soup: Parsed BeautifulSoup document.

        Returns:
            Dictionary mapping meta names/properties to content (prefixed with 'meta.'),
            plus 'h1' when present.

        Raises:
            None
        """
        out: Dict[str, str] = {}
        for m in soup.find_all("meta"):
            name = m.get("name") or m.get("property")
            if not name:
                continue
            content = m.get("content")
            if content is None:
                continue
            key = f"meta.{name}"
            if key not in out:
                out[key] = content
        h1 = soup.find("h1")
        if h1 and "text" not in out:
            out["h1"] = h1.get_text(strip=True)
        return out

    def extract_datalayer(self, html: str) -> Dict[str, str]:
        """
        Parse `window.dataLayer.push({...})` calls into a flat mapping.

        Args:
            html: Raw HTML of a job detail page.

        Returns:
            Dictionary mapping 'datalayer.<key>' to extracted values.

        Raises:
            None
        """
        out: Dict[str, str] = {}
        for m in re.finditer(
            r"window\.dataLayer\.push\(\{([^)]*?)\}\)", html, re.I | re.M | re.S
        ):
            body = m.group(1)
            for k, v in re.findall(
                r"['\"]([^'\"]+)['\"]\s*:\s*['\"]([^'\"]*)['\"]", body
            ):
                out[f"datalayer.{k}"] = v
        return out

    # -------------------------------------------------------------------------
    # Detail fetch/parse helpers
    # -------------------------------------------------------------------------
    def fetch_job_detail_html(self, url: str) -> str:
        """
        Fetch the HTML for a given job detail URL.

        Args:
            url: Absolute URL to the job detail page.

        Returns:
            Raw HTML string.

        Raises:
            requests.RequestException: If the HTTP request fails.
        """
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.text

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
        out.update(self.extract_insets(soup))
        out.update(self.extract_bold_block(soup))
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
            token = self.b64url_encode(payload)
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
        detail_url = self.absolute_url(detail_rel)
        record = {
            "Detail URL": detail_url,
            "Position Title": raw_job.get("title", ""),
            "Company": raw_job.get("company", ""),
            "Location": raw_job.get("location", ""),
            "Posting ID": raw_job.get("id", ""),
        }
        try:
            self.log("detail:fetch", url=detail_url)
            html = self.fetch_job_detail_html(detail_url)
            doc = self.parse_job_detail_doc(html)
            record.update(doc)
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
