"""scrapers/leidos_scraper.py

Leidos scraper.

Cloudflare blocks both the paginated search pages *and* the individual job
detail pages for non-browser clients.

This scraper therefore uses undetected-chromedriver for:
  1) Enumerating listings from the search pages
  2) Fetching/parsing detail pages

To remain compatible with the existing JobScraper framework (which parallelizes
parse_job() with a ThreadPool), we override run() and handle detail scraping
using a small pool of independent browser instances.
"""

from __future__ import annotations

import os
import re
import psycopg2
import sqlite3
from time import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin

import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait

from scrapers.engine import JobScraper
from utils.canonicalize import CANON_COLUMNS, canonicalize_record


if os.name == "nt" and os.environ.get("LEIDOS_SILENCE_UC_DEL", "1") == "1":
    try:

        def noop(self) -> None:
            return None

        uc.Chrome.__del__ = noop
    except Exception:
        pass


class LeidosScraper(JobScraper):
    VENDOR = "Leidos"

    def __init__(self) -> None:
        super().__init__(
            base_url="https://careers.leidos.com",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Connection": "keep-alive",
            },
        )

        # From the provided DOM: pagination links look like /search/jobs/in?page=2&q=#
        self.search_url_template = (
            "https://careers.leidos.com/search/jobs/in?page={page}&q=#"
        )

    # -------------------------------------------------------------------------
    # Listings (Selenium)
    # -------------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, Any]]:
        if getattr(self, "testing", False):
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 0
            except Exception:
                job_limit = 15
        else:
            job_limit = float("inf")  # type: ignore[assignment]

        options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1200")
        options.add_argument("--no-sandbox")

        self.log("driver:init")
        driver = uc.Chrome(options=options)

        all_jobs: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()

        try:
            first_url = self.search_url_template.format(page=1)
            driver.get(first_url)

            # Wait until job list is present (DOM uses jobs-section__list/items)
            self.log("driver:wait", target="jobs-section__list")
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script(
                    "return document.querySelectorAll('div.jobs-section__item').length > 0;"
                )
            )

            first_soup = BeautifulSoup(driver.page_source, "lxml")
            total_pages = self._extract_total_pages(first_soup)
            self.log("source:total_pages", total_pages=total_pages)

            # Iterate pages deterministically by URL (no clicking needed)
            for page in range(1, total_pages + 1):
                if len(all_jobs) >= job_limit:
                    break

                url = self.search_url_template.format(page=page)
                driver.get(url)

                # Ensure page content loads
                WebDriverWait(driver, 30).until(
                    lambda d: d.execute_script(
                        "return document.querySelectorAll('div.jobs-section__item').length > 0;"
                    )
                )

                soup = BeautifulSoup(driver.page_source, "lxml")
                page_jobs = self._extract_jobs_from_search_page(soup, page=page)

                # Deduplicate by detail URL
                added = 0
                for j in page_jobs:
                    if len(all_jobs) >= job_limit:
                        break
                    u = j.get("Detail URL") or ""
                    if not u or u in seen_urls:
                        continue
                    seen_urls.add(u)
                    all_jobs.append(j)
                    added += 1

                self.log("list:page", page=page, got=len(page_jobs), added=added)

            self.log("list:done", reason="end", total=len(all_jobs))
            return all_jobs

        finally:
            self.log("driver:quit")
            driver.quit()

    @staticmethod
    def _extract_total_pages(soup: BeautifulSoup) -> int:
        paginate = soup.find("div", class_="jobs-section__paginate")
        if not paginate:
            # If pagination isn't present, treat as 1 page.
            return 1

        nums: List[int] = []
        for a in paginate.find_all("a", href=True):
            txt = a.get_text(strip=True)
            if txt.isdigit():
                try:
                    nums.append(int(txt))
                except Exception:
                    pass
        return max(nums) if nums else 1

    @staticmethod
    def _extract_jobs_from_search_page(
        soup: BeautifulSoup, page: int
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        for item in soup.find_all("div", class_="jobs-section__item"):
            a = item.find("a", href=True)
            if not a:
                continue

            href = a["href"].strip()
            if not href:
                continue
            # Listing links are absolute in the DOM sample.
            if "careers.leidos.com/jobs/" not in href:
                continue

            title = a.get_text(strip=True)
            if not title:
                continue

            # Deterministic label/value extraction from within this item only
            strings = [s.strip() for s in item.stripped_strings if s and s.strip()]

            def after(label: str) -> str:
                try:
                    idx = strings.index(label)
                except ValueError:
                    return ""
                return strings[idx + 1] if idx + 1 < len(strings) else ""

            raw_location = after("Location:")
            clearance = after("Clearance:")
            req_number = after("Req Number:")

            out.append(
                {
                    "Position Title": title,
                    "Detail URL": href,
                    "Raw Location": raw_location,
                    "Clearance": clearance,
                    "Req Number": req_number,
                    "_page": page,
                }
            )

        return out

    # -------------------------------------------------------------------------
    # Details (Selenium)
    # -------------------------------------------------------------------------
    @staticmethod
    def _extract_label_from_page_text(page_text: str, label: str) -> str:
        """Extract the first occurrence of '<label> VALUE' anchored to a line start."""
        m = re.search(rf"^{re.escape(label)}\s*(.+)$", page_text, flags=re.MULTILINE)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _new_driver() -> uc.Chrome:
        options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1200")
        options.add_argument("--no-sandbox")
        return uc.Chrome(options=options)

    @staticmethod
    def _wait_detail_loaded(driver: uc.Chrome) -> None:
        """Wait for a stable element on the job detail page.

        We avoid heuristics; we only wait for elements that are part of the
        consistent job template.
        """

        def _ready(d: uc.Chrome) -> bool:
            # Either we see an H1 (job title) or the main description container.
            return bool(
                d.execute_script(
                    "return !!document.querySelector('h1') || !!document.querySelector('div.job-description-js');"
                )
            )

        WebDriverWait(driver, 30).until(_ready)

    def _parse_job_with_driver(
        self, driver: uc.Chrome, raw_job: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        detail_url = raw_job.get("Detail URL") or ""
        if detail_url:
            detail_url = urljoin(self.base_url, detail_url)

        self.log("detail:fetch", url=detail_url)
        driver.get(detail_url)
        self._wait_detail_loaded(driver)

        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")

        title_el = soup.find("h1")
        title = (
            title_el.get_text(strip=True) if title_el else raw_job.get("Position Title")
        )

        desc_el = soup.find("div", class_="job-description-js")
        description = desc_el.get_text("\n", strip=True) if desc_el else ""

        page_text = soup.get_text("\n", strip=True)

        posting_id = self._extract_label_from_page_text(page_text, "Job #:")
        location = self._extract_label_from_page_text(page_text, "Location:")
        category = self._extract_label_from_page_text(page_text, "Category:")
        schedule = self._extract_label_from_page_text(page_text, "Schedule (FT/PT):")
        shift = self._extract_label_from_page_text(page_text, "Shift:")
        remote_type = self._extract_label_from_page_text(page_text, "Remote Type:")
        clearance = self._extract_label_from_page_text(page_text, "Clearance:")
        sector = self._extract_label_from_page_text(page_text, "Sector:")
        original_posting = self._extract_label_from_page_text(
            page_text, "Original Posting:"
        )
        pay_range = self._extract_label_from_page_text(page_text, "Pay Range:")

        return {
            "Vendor": self.VENDOR,
            "Posting ID": posting_id,
            "Position Title": title,
            "Description": description,
            "Detail URL": detail_url,
            "Raw Location": location,
            "Category": category,
            "Schedule (FT/PT)": schedule,
            "Shift": shift,
            "Remote Type": remote_type,
            "Clearance": clearance,
            "Sector": sector,
            "Original Posting": original_posting,
            "Pay Range": pay_range,
            "Req Number": raw_job.get("Req Number"),
            "_page": raw_job.get("_page"),
        }

    def parse_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """Not used.

        Base JobScraper.run() parallelizes parse_job() in a ThreadPool.
        Selenium drivers are not safely shareable across threads, and Leidos
        blocks detail pages for non-browser clients (403s in your log). fileciteturn49file9

        This scraper overrides run() and performs detail parsing with a small
        pool of independent UC drivers.
        """

        raise RuntimeError(
            "LeidosScraper overrides run(); parse_job() should not be called"
        )

    # ---------------------------------------------------------------------
    # DB helper (skip already-scraped detail URLs)
    # ---------------------------------------------------------------------
    @staticmethod
    def _chunk(items: List[str], size: int) -> Iterable[List[str]]:
        for i in range(0, len(items), size):
            yield items[i : i + size]

    def _get_existing_detail_urls(
        self,
        *,
        db_url: str,
        table: str,
        vendor: str | None,
        urls: List[str],
        chunk_size: int = 500,
    ) -> Set[str]:
        """Return the subset of `urls` that already exist in the DB for this vendor.

        Base JobScraper has an incremental skip mechanism that checks the DB
        "Dedupe Key". For Leidos, the DB dedupe key is typically Posting ID
        (Job #) *after* we visit the detail page. On listing pages, we only
        know Detail URL, so skipping by dedupe key doesn't help.

        This helper performs an exact match lookup against the DB "Detail URL"
        column instead.
        """

        ulist = [u for u in dict.fromkeys(urls) if u]  # stable unique
        if not ulist:
            return set()

        # SQLite
        if db_url.startswith("sqlite://"):
            path = db_url.replace("sqlite:///", "", 1).replace("sqlite://", "", 1)
            have: Set[str] = set()
            con = sqlite3.connect(path)
            try:
                cur = con.cursor()
                for chunk in self._chunk(ulist, chunk_size):
                    placeholders = ",".join(["?"] * len(chunk))
                    if vendor:
                        q = (
                            f'SELECT "Detail URL" FROM {table} '
                            f'WHERE "Vendor"=? AND "Detail URL" IN ({placeholders})'
                        )
                        cur.execute(q, [vendor, *chunk])
                    else:
                        q = (
                            f'SELECT "Detail URL" FROM {table} '
                            f'WHERE "Detail URL" IN ({placeholders})'
                        )
                        cur.execute(q, chunk)
                    have.update(r[0] for r in cur.fetchall() if r and r[0])
            finally:
                con.close()
            return have

        # Postgres
        if db_url.startswith(("postgres://", "postgresql://")):
            have: Set[str] = set()
            con = psycopg2.connect(db_url)
            try:
                with con.cursor() as cur:
                    for chunk in self._chunk(ulist, chunk_size):
                        if vendor:
                            cur.execute(
                                f'SELECT "Detail URL" FROM {table} '
                                f'WHERE "Vendor"=%s AND "Detail URL" = ANY(%s)',
                                (vendor, chunk),
                            )
                        else:
                            cur.execute(
                                f'SELECT "Detail URL" FROM {table} '
                                f'WHERE "Detail URL" = ANY(%s)',
                                (chunk,),
                            )
                        have.update(r[0] for r in cur.fetchall() if r and r[0])
            finally:
                con.close()
            return have

        raise ValueError(
            f"Unsupported db_url scheme for {db_url!r} (use sqlite:/// or postgresql://)"
        )

    # -------------------------------------------------------------------------
    # Orchestrator override
    # -------------------------------------------------------------------------
    def run(self) -> None:
        """Execute fetch + parse with Selenium for both phases."""

        start = time()
        errors = 0

        self.log("fetch:start")
        raw = self.fetch_data()

        # Testing / per-scraper limit behavior mirrors base.py
        if getattr(self, "testing", False):
            limit = int(getattr(self, "test_limit", 15) or 15)
            self.log("testing:limit", limit=limit)
            raw = raw[:limit]
        elif getattr(self, "limit_per_scraper", None):
            try:
                cap = int(getattr(self, "limit_per_scraper"))
            except Exception:
                cap = None
            if cap is not None and cap > 0:
                self.log("limit:per_scraper", limit=cap)
                raw = raw[:cap]

        self._kept_count = len(raw)
        self.log("fetch:done", n=len(raw))

        # -----------------------------
        # Optional: incremental skip via DB (matches base.py behavior)
        # -----------------------------
        vendor_name = getattr(self, "vendor", None) or getattr(self, "VENDOR", None)
        if not vendor_name:
            vendor_name = self.__class__.__name__.replace("Scraper", "")

        if self.db_url and self.db_skip_existing and raw:
            # IMPORTANT (Leidos): Our DB "Dedupe Key" is usually Posting ID (Job #)
            # after a successful scrape. But on the listings pages we only know the
            # Detail URL + Req Number. Checking only the dedupe key therefore won't
            # skip already-scraped jobs.
            #
            # To avoid wasting time launching browser navigations for already-known
            # postings, we also skip by exact "Detail URL" when a DB is configured.
            try:
                existing_urls = self._get_existing_detail_urls(
                    db_url=self.db_url,
                    table=self.db_table,
                    vendor=vendor_name,
                    urls=[r.get("Detail URL") or "" for r in raw],
                )
                if existing_urls:
                    before = len(raw)
                    raw = [
                        r
                        for r in raw
                        if (r.get("Detail URL") or "") not in existing_urls
                    ]
                    self.log(
                        "incremental:skip_existing",
                        before=before,
                        skipped=(before - len(raw)),
                        kept=len(raw),
                        method="detail_url",
                    )
                    self.metrics.inc("incremental.skipped", before - len(raw))
                    self.metrics.set_gauge("incremental.kept", len(raw))
            except Exception:
                # Non-fatal: fall back to full parse if existence check fails
                self.logger.exception("incremental:skip:error")

        self.log("parse:start", total=len(raw))
        parsed_min: List[Dict[str, Any]] = []
        parsed_full: List[Dict[str, Any]] = []

        # Use a small number of browsers to avoid triggering Cloudflare.
        # Allow override via env var; default is 2 (or 1 in testing).
        if getattr(self, "testing", False):
            driver_workers = 1
        else:
            try:
                driver_workers = max(
                    1, int(os.environ.get("LEIDOS_DRIVER_WORKERS", "2"))
                )
            except Exception:
                driver_workers = 2

        self.log("parse:pool:start", workers=driver_workers, total=len(raw))

        # Partition work so each thread owns its browser instance.
        chunks: List[List[Dict[str, Any]]] = [[] for _ in range(driver_workers)]
        for i, item in enumerate(raw):
            chunks[i % driver_workers].append(item)

        def _worker(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            d = self._new_driver()
            out: List[Dict[str, Any]] = []
            try:
                for r in items:
                    rec = self._parse_job_with_driver(d, r)
                    if rec:
                        out.append(rec)
            finally:
                try:
                    d.quit()
                except Exception:
                    pass
            return out

        idx = 0
        with ThreadPoolExecutor(max_workers=driver_workers) as ex:
            futures = [ex.submit(_worker, c) for c in chunks if c]
            for fut in as_completed(futures):
                try:
                    rows = fut.result()
                except Exception:
                    errors += 1
                    self.logger.exception("parse:error")
                    continue

                for rec in rows:
                    idx += 1
                    artifacts = rec.pop("artifacts", None)
                    full_row = {"Vendor": self.VENDOR, **rec}
                    if artifacts:
                        full_row["_artifacts"] = artifacts

                    canon_row = canonicalize_record(vendor=self.VENDOR, raw=rec)
                    parsed_full.append(full_row)
                    parsed_min.append({k: canon_row.get(k, "") for k in CANON_COLUMNS})

                    if idx == 1 or idx % self.log_every == 0:
                        self.log("parse:progress", idx=idx, total=len(raw))

        self.log("parse:pool:done", parsed=len(parsed_min))

        parsed_min = self.dedupe_records(parsed_min)
        if self.write_full_also:
            parsed_full = self.dedupe_records(parsed_full)
            self.jobs_full = parsed_full

        self.jobs = parsed_min
        self.log("done", count=len(self.jobs))
        self.log("parse:errors", n=errors)
        self.log("run:duration", seconds=round(time() - start, 3))
