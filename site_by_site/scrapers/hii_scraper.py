"""
HII (Huntington Ingalls Industries) scraper.

Enumerates job listings from the HII careers search page and then loads
each job detail page to extract normalized fields for export.
"""

from __future__ import annotations

from math import ceil
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
import re

import requests
from bs4 import BeautifulSoup as BS

from scrapers.engine import JobScraper


class HIIScraper(JobScraper):
    # Used by the base pipeline for CSV/DB exports and incremental skip checks
    VENDOR = "HII"

    def __init__(self) -> None:
        """
        Initialize the HII scraper with base URL and headers.

        Args:
            None

        Returns:
            None
        """
        super().__init__(
            base_url="https://careers.huntingtoningalls.com/search/",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                )
            },
        )

    # -------------------------------------------------------------------------
    # Listing fetch
    # -------------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, Any]]:
        all_jobs: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        # Respect the unified testing limit behavior (like other scrapers)
        if getattr(self, "testing", False):
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 0
            except Exception:
                job_limit = 15
        else:
            job_limit = float("inf")  # type: ignore[assignment]

        # ---- Fetch first page to learn pagination / totals ----
        params = {
            "q": "",
            "sortColumn": "referencedate",
            "sortDirection": "desc",
        }
        first_resp = self.get(self.base_url, params=params)
        first_resp.raise_for_status()
        first_html = first_resp.text
        first_soup = BS(first_html, "lxml")

        table = first_soup.find("table", id="searchresults")
        if not table:
            self.log("list:error", reason="no_table", url=first_resp.url)
            return []

        rows = table.select("tr.data-row")
        page_size, total_results = self._parse_pagination(first_soup, len(rows))
        total_pages = (
            ceil(total_results / page_size) if page_size and total_results else 1
        )

        self.log(
            "source:total",
            total=total_results,
            page_size=page_size,
            total_pages=total_pages,
        )

        # Helper to extract job entries from a BeautifulSoup page
        def extract_jobs_from_soup(
            soup: BS,
            page_index: int,
        ) -> int:
            nonlocal all_jobs, seen_ids

            table_local = soup.find("table", id="searchresults")
            if not table_local:
                self.log("list:page", page=page_index + 1, got=0, reason="no_table")
                return 0

            rows_local = table_local.select("tr.data-row")
            self.log("list:page", page=page_index + 1, got=len(rows_local))

            added_here = 0
            for row in rows_local:
                if len(all_jobs) >= job_limit:
                    break

                # Title & detail URL
                link = row.select_one("a.jobTitle-link")
                if not link:
                    continue

                href = (link.get("href") or "").strip()
                if not href:
                    continue

                detail_url = urljoin("https://careers.huntingtoningalls.com", href)

                # Posting ID from the numeric tail of the URL: /.../1341969500/
                posting_id = self._extract_job_id_from_href(href)
                if posting_id and posting_id in seen_ids:
                    continue

                title = link.get_text(strip=True)

                # Location and date (hidden-phone columns)
                loc_span = row.select_one("td.colLocation span.jobLocation")
                date_span = row.select_one("td.colDate span.jobDate")

                raw_loc = loc_span.get_text(strip=True) if loc_span else ""
                post_date = date_span.get_text(strip=True) if date_span else ""

                job: Dict[str, Any] = {
                    "Posting ID": posting_id,
                    "Position Title": title,
                    "Detail URL": detail_url,
                    "Raw Location": raw_loc,
                    "Post Date": post_date,
                }

                all_jobs.append(job)
                added_here += 1

                if posting_id:
                    seen_ids.add(posting_id)

            return added_here

        # ---- Page 0 ----
        added = extract_jobs_from_soup(first_soup, page_index=0)
        if len(all_jobs) >= job_limit or total_pages <= 1:
            self.log(
                "list:done",
                reason="end",
                pages=1,
                total=len(all_jobs),
            )
            return all_jobs

        # ---- Remaining pages ----
        for page_index in range(1, total_pages):
            if len(all_jobs) >= job_limit:
                break

            offset = page_index * page_size
            params = {
                "q": "",
                "sortColumn": "referencedate",
                "sortDirection": "desc",
                "startrow": str(offset),
            }
            resp = self.get(self.base_url, params=params)
            resp.raise_for_status()
            soup = BS(resp.text, "lxml")

            added = extract_jobs_from_soup(soup, page_index=page_index)
            if added == 0:
                # Defensive break if pagination metadata was off
                self.log(
                    "list:done",
                    reason="empty_page",
                    page=page_index + 1,
                    total=len(all_jobs),
                )
                break

        self.log(
            "list:done",
            reason="end",
            pages=min(total_pages, page_index + 1),
            total=len(all_jobs),
        )
        return all_jobs

    # -------------------------------------------------------------------------
    # Detail parsing
    # -------------------------------------------------------------------------
    def parse_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        record: Dict[str, Any] = dict(raw_job)

        # Normalize detail URL to absolute, just in case
        detail_rel = raw_job.get("Detail URL") or ""
        detail_url = urljoin("https://careers.huntingtoningalls.com", detail_rel)
        record["Detail URL"] = detail_url

        # Ensure we always have a Posting ID, falling back to the URL if needed
        posting_id = record.get("Posting ID") or self._extract_job_id_from_href(
            detail_url
        )
        record["Posting ID"] = posting_id

        try:
            self.log("detail:fetch", url=detail_url)
            resp = self.thread_get(detail_url)
            resp.raise_for_status()
            html = resp.text
            soup = BS(html, "lxml")

            # ---- Title ----
            # Prefer existing listing title; fall back to og:title or h1.
            if not record.get("Position Title"):
                meta_title = soup.find("meta", attrs={"property": "og:title"})
                if meta_title and meta_title.get("content"):
                    record["Position Title"] = meta_title["content"].strip()
                else:
                    h1 = soup.find("h1")
                    if h1:
                        record["Position Title"] = h1.get_text(strip=True)

            # ---- Location & Date from detail page ----
            loc_span = soup.select_one("span.jobGeoLocation")
            if loc_span:
                raw_loc = loc_span.get_text(strip=True)
                record["Raw Location"] = raw_loc

                city, state, country = self._split_location(raw_loc)
                if city:
                    record["City"] = city
                if state:
                    record["State"] = state
                if country:
                    record["Country"] = country

            date_span = soup.find("span", attrs={"data-careersite-propertyid": "date"})
            if date_span:
                # e.g. "Dec 11, 2025"
                record["Post Date"] = date_span.get_text(strip=True)

            # ---- Description + structured fields ----
            desc_block = soup.select_one("span.jobdescription")
            description_text = ""
            if desc_block:
                # Normalize <br> tags into newlines before extracting text
                for br in desc_block.find_all("br"):
                    br.replace_with("\n")

                description_text = desc_block.get_text("\n", strip=True)
                record["Description"] = description_text

                # Parse header lines: Req ID, Team, Entity, US Citizenship, etc.
                header_fields = self._parse_hii_header_lines(
                    description_text.splitlines()
                )
                record.update(header_fields)

        except requests.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            self.log(
                "detail:http_error",
                url=detail_url,
                error=str(e),
                status=status,
            )
        except Exception as e:
            # Keep the listing-level record but note the parse error.
            self.log("detail:parse_error", url=detail_url, error=str(e))

        return record

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _extract_job_id_from_href(href: str) -> Optional[str]:
        if not href:
            return None
        # Try to capture a trailing number-like segment
        m = re.search(r"/(\d+)/?$", href)
        if m:
            return m.group(1)
        # Fallback: last segment, stripped of non-digits
        tail = href.rstrip("/").split("/")[-1]
        digits = "".join(ch for ch in tail if ch.isdigit())
        return digits or tail or None

    @staticmethod
    def _split_location(raw: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        if not raw:
            return None, None, None
        parts = [p.strip() for p in raw.split(",") if p and p.strip()]
        if len(parts) >= 3:
            city = parts[0]
            state = parts[1]
            country = ", ".join(parts[2:])
            return city or None, state or None, country or None
        if len(parts) == 2:
            return parts[0] or None, parts[1] or None, None
        if len(parts) == 1:
            return parts[0] or None, None, None
        return None, None, None

    @staticmethod
    def _parse_pagination(soup: BS, fallback_page_size: int) -> Tuple[int, int]:
        label = soup.select_one(".paginationLabel")
        if not label:
            # Fallback: use row count as page size, unknown total
            return max(fallback_page_size, 1), fallback_page_size

        text = label.get_text(" ", strip=True)
        # Handle both en dash and hyphen between start/end
        m = re.search(r"Results\s+\d+\s*[–-]\s*(\d+)\s+of\s+(\d+)", text)
        if not m:
            return max(fallback_page_size, 1), fallback_page_size

        try:
            end_idx = int(m.group(1))
            total = int(m.group(2))
        except ValueError:
            return max(fallback_page_size, 1), fallback_page_size

        # On the first page "Results 1 – N" → N is the page size.
        page_size = end_idx or fallback_page_size or 25
        return page_size, total

    @staticmethod
    def _parse_hii_header_lines(lines: List[str]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        def after_colon(s: str) -> str:
            return s.split(":", 1)[1].strip() if ":" in s else s.strip()

        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue

            if line.startswith("Req ID"):
                val = after_colon(line)
                out["Req ID"] = val
                out["Requisition ID"] = val

            elif line.startswith("Team:"):
                out["Team"] = after_colon(line)

            elif line.startswith("Entity:"):
                out["Entity"] = after_colon(line)

            elif line.startswith("US Citizenship Required for this Position"):
                val = after_colon(line)
                out["US Citizenship Required"] = val
                # Map to a more generic column name used elsewhere
                out["US Person Required"] = val

            elif line in ("Full-Time", "Part-Time") or line.endswith("-Time"):
                # e.g. "Full-Time"
                out["Full Time Status"] = line

            elif line.startswith("Shift:"):
                out["Shift"] = after_colon(line)

            elif line.startswith("Relocation:"):
                out["Relocation Available"] = after_colon(line)

            elif line.startswith("Virtual/Telework Opportunity"):
                out["Remote Status"] = after_colon(line)

            elif line.startswith("Travel Requirement"):
                out["Travel Requirement"] = after_colon(line)

            elif line.startswith("Clearance Required"):
                val = after_colon(line)
                out["Clearance Required"] = val
                out["Clearance Level Must Possess"] = val

        return out
