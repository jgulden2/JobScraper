"""
Phase 0.3 Contract (Listing vs Detail split)
- Platform adapters do LISTING only: return stable IDs + detail URLs (+ optional minimal metadata)
- Driver owns DETAIL fetching: fetch_detail_artifacts() happens here, not inside adapters
- Adapters map artifacts -> raw record via normalize()
Legacy company-specific scrapers may exist until Phase 1 migration.
"""

# scrapers/company_driver.py
from __future__ import annotations

import tempfile
import os

import undetected_chromedriver as uc

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
from typing import Any, Dict, List, Optional
from scrapers.engine import JobScraper
from utils.detail_fetchers import fetch_detail_artifacts
from utils.company_config import CompanyConfig
from scrapers.platform_adapters.base import Adapter
from scrapers.platform_adapters.sitemap_job_urls import SitemapJobUrlsAdapter
from scrapers.platform_adapters.phenom_sitemap import PhenomSitemapAdapter
from scrapers.platform_adapters.apply_v2 import ApplyV2Adapter
from scrapers.platform_adapters.paged_html_search import PagedHtmlSearchAdapter
from scrapers.platform_adapters.selenium_paged_html_search import (
    SeleniumPagedHtmlSearchAdapter,
)
from scrapers.platform_adapters.usajobs_api import USAJobsApiAdapter
from scrapers.platform_adapters.phenom_search import PhenomSearchAdapter
from scrapers.platform_adapters.encoded_request_api import EncodedRequestApiAdapter
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException


# -----------------------------
# Adapter registry
# -----------------------------
ADAPTERS = {
    "sitemap_job_urls": SitemapJobUrlsAdapter(),
    "phenom": PhenomSitemapAdapter(),
    "apply_v2": ApplyV2Adapter(),
    "paged_html_search": PagedHtmlSearchAdapter(),
    "selenium_paged_html_search": SeleniumPagedHtmlSearchAdapter(),
    "usajobs_api": USAJobsApiAdapter(),
    "phenom_search": PhenomSearchAdapter(),
    "encoded_request_api": EncodedRequestApiAdapter(),
}


class CompanyConfigScraper(JobScraper):
    """
    Config-driven scraper:
      adapter.list_jobs(cfg) -> minimal refs
      shared fetch_detail_artifacts(url)
      adapter.normalize(cfg, raw_job, artifacts) -> raw record
    """

    def __init__(self, cfg: CompanyConfig) -> None:
        base_url = cfg.search_url or cfg.careers_home or ""
        headers = cfg.headers or {}
        super().__init__(base_url=base_url, headers=headers)

        self.cfg = cfg
        self.VENDOR = cfg.name
        self.vendor = cfg.name

        platform_key = cfg.platform_name or "sitemap_job_urls"
        if platform_key not in ADAPTERS:
            raise ValueError(
                f"Unknown platform {platform_key!r} for company {cfg.company_id!r}"
            )
        self.adapter: Adapter = ADAPTERS[platform_key]

    def fetch_data(self) -> List[Dict[str, Any]]:
        rows = self.adapter.list_jobs(self, self.cfg)
        # Minimal enforcement of the listing/detail boundary:
        # list_jobs must return refs that include Detail URL
        for i, r in enumerate(rows[:20]):  # only sample a few
            if not (r.get("Detail URL") or "").strip():
                raise ValueError(
                    f"adapter.list_jobs returned row missing Detail URL (idx={i}) for {self.cfg.company_id}"
                )
        return rows

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = (raw_job.get("Detail URL") or "").strip()
        if not url:
            return None

        # Some platforms (e.g., USAJOBS) are API-first and do not need HTML detail fetching.
        if getattr(self.adapter, "skip_detail_fetch", False):
            artifacts: Dict[str, Any] = {}
            record = self.adapter.normalize(self.cfg, raw_job, artifacts)
            record["artifacts"] = artifacts
            return record

        artifacts = fetch_detail_artifacts(self.thread_get, self.log, url)
        record = self.adapter.normalize(self.cfg, raw_job, artifacts)
        record["artifacts"] = artifacts
        return record


class BrowserCompanyConfigScraper(CompanyConfigScraper):
    """
    Variant of CompanyConfigScraper for sites that block non-browser clients.

    - Listing still uses adapter.list_jobs(), but the adapter can call
      scraper.browser_get_html(...) to fetch HTML via a real browser.
    - Detail fetching uses Selenium to obtain HTML, then runs the same
      extractors via fetch_detail_artifacts_from_html().

    Driven by CompanyConfig.access_policy.requires_browser_bootstrap.
    """

    def __init__(self, cfg: CompanyConfig) -> None:
        super().__init__(cfg)

        import threading

        self._tl = threading.local()
        self._drivers = []
        self._drivers_lock = threading.Lock()

    def _new_driver(self):
        options = uc.ChromeOptions()

        # NON-HEADLESS (so you see the window)
        # Do NOT add any --headless flags.

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1400,900")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--remote-allow-origins=*")

        # Fresh profile to avoid “stuck session / weird redirects”
        profile_dir = tempfile.mkdtemp(prefix="job_scraper_uc_")
        options.add_argument(f"--user-data-dir={profile_dir}")

        # Optional: sometimes helps keep it visible / stable
        options.add_argument("--start-maximized")

        driver = uc.Chrome(options=options, use_subprocess=True)

        driver.minimize_window()
        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)

        with self._drivers_lock:
            self._drivers.append(driver)

        return driver

    def _driver(self):
        d = getattr(self._tl, "driver", None)
        if d is None:
            d = self._new_driver()
            self._tl.driver = d
        return d

    def close(self):
        # Close all Selenium drivers cleanly
        drivers = []
        with self._drivers_lock:
            drivers = list(self._drivers)
            self._drivers.clear()

        for d in drivers:
            try:
                d.quit()
            except Exception:
                pass

    def browser_get_html(
        self,
        url: str,
        *,
        wait_css: str | None = None,
        wait_js: str | None = None,
        timeout_s: float = 35.0,
    ) -> str:
        driver = self._driver()
        try:
            driver.get(url)
        except WebDriverException as e:
            self.log("browser:get:error", url=url, error=str(e))
            raise

        # Always log where we ended up (redirects, blocks, etc.)
        try:
            self.log(
                "browser:landed",
                requested=url,
                current=driver.current_url,
                title=driver.title or "",
            )
        except Exception:
            pass

        if wait_css or wait_js:
            w = WebDriverWait(driver, timeout_s)
            try:
                if wait_css:
                    from selenium.webdriver.common.by import By
                    from selenium.webdriver.support import expected_conditions as EC

                    w.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_css)))

                if wait_js:
                    # If execute_script throws, treat it as “not ready yet”
                    def _cond(drv):
                        try:
                            return bool(drv.execute_script(wait_js))
                        except Exception:
                            return False

                    w.until(_cond)

            except TimeoutException:
                # DO NOT crash. Return HTML so we can see what loaded.
                try:
                    snippet = (driver.page_source or "")[:800]
                except Exception:
                    snippet = ""
                self.log(
                    "browser:wait:timeout",
                    requested=url,
                    current=getattr(driver, "current_url", ""),
                    title=getattr(driver, "title", ""),
                    wait_css=wait_css or "",
                    wait_js=wait_js or "",
                    html_snippet=snippet,
                )
                return driver.page_source or ""

        # DEBUG: dump the first listing page HTML for selector tuning
        try:
            if "search/jobs" in url and "page=1" in url:
                with open("leidos_page1.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source or "")
                self.log("browser:dumped_html", file="leidos_page1.html", url=url)
        except Exception as e:
            self.log("browser:dump_html:error", url=url, error=str(e))

        return driver.page_source or ""

    def run(self) -> None:  # type: ignore[override]
        """
        Leidos-style run override:
        - fetch_data() (listing) uses Selenium via adapter + browser_get_html
        - parse phase uses a small pool of independent UC drivers
        """
        start = time()

        try:
            self.log("fetch:start")
            raw = self.fetch_data()  # calls adapter.list_jobs(...)
            self.log("fetch:done", n=len(raw))

            # Respect testing / limit behaviors from engine (simple version)
            if getattr(self, "testing", False):
                limit = int(getattr(self, "test_limit", 15) or 15)
                raw = raw[:limit]
            else:
                cap = getattr(self, "limit_per_scraper", None)
                if cap is not None:
                    raw = raw[: int(cap)]

            self.log("parse:start", total=len(raw))

            # Driver pool size: match leidos_scraper defaults (2, or 1 in testing)
            if getattr(self, "testing", False):
                driver_workers = 1
            else:
                try:
                    driver_workers = max(
                        1, int(os.environ.get("LEIDOS_DRIVER_WORKERS", "2"))
                    )
                except Exception:
                    driver_workers = 2

            # Partition items so each thread gets its own driver and reuses it
            chunks = [[] for _ in range(driver_workers)]
            for i, item in enumerate(raw):
                chunks[i % driver_workers].append(item)

            def _worker_wrapper(items):
                d = self._new_driver()
                self._tl.driver = d
                try:
                    out = []
                    for r in items:
                        rec = self.parse_job(r)
                        if rec:
                            out.append(rec)
                    return out
                finally:
                    try:
                        d.quit()
                    except Exception:
                        pass
                    self._tl.driver = None

            parsed = []
            with ThreadPoolExecutor(max_workers=driver_workers) as ex:
                futures = [ex.submit(_worker_wrapper, c) for c in chunks if c]
                for fut in as_completed(futures):
                    parsed.extend(fut.result())

            self.jobs = parsed
            self.log("done", count=len(self.jobs))
            self.log("run:duration", seconds=round(time() - start, 3))

        finally:
            # ✅ THIS is what closes the listing browser + any drivers created
            self.close()

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        url = (raw_job.get("Detail URL") or "").strip()
        if not url:
            return None

        # Use worker-owned driver via thread-local (set in run() wrapper)
        driver = getattr(self._tl, "driver", None)
        if driver is None:
            driver = self._new_driver()
            self._tl.driver = driver

        # Wait condition like your working scraper: h1 OR div.job-description-js
        from selenium.webdriver.support.ui import WebDriverWait

        driver.get(url)

        def _ready(d) -> bool:
            return bool(
                d.execute_script(
                    "return !!document.querySelector('h1') || !!document.querySelector('div.job-description-js');"
                )
            )

        WebDriverWait(driver, 30).until(_ready)

        html = driver.page_source or ""

        # Now parse like your leidos_scraper.py does, or feed into your artifact parser
        # (Keeping it simple and faithful to the working scraper)
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        title_el = soup.find("h1")
        title = (
            title_el.get_text(strip=True)
            if title_el
            else (raw_job.get("Position Title") or "")
        )

        desc_el = soup.find("div", class_="job-description-js")
        description = desc_el.get_text("\n", strip=True) if desc_el else ""

        # Minimal record; extend if you want the exact label extraction like old scraper
        rec = {
            "Vendor": getattr(self, "vendor", None)
            or getattr(self.cfg, "name", "")
            or self.cfg.company_id,
            "Posting ID": raw_job.get("Posting ID") or "",
            "Position Title": title,
            "Description": description,
            "Detail URL": url,
            "_page": raw_job.get("_page"),
        }
        return rec
