"""
Phase 0.3 Contract (Listing vs Detail split)
- Platform adapters do LISTING only: return stable IDs + detail URLs (+ optional minimal metadata)
- Driver owns DETAIL fetching: fetch_detail_artifacts() happens here, not inside adapters
- Adapters map artifacts -> raw record via normalize()
Legacy company-specific scrapers may exist until Phase 1 migration.
"""

# scrapers/company_driver.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from scrapers.engine import JobScraper
from utils.detail_fetchers import fetch_detail_artifacts
from utils.company_config import CompanyConfig
from utils.detail_fetchers import fetch_detail_artifacts_from_html
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
        import undetected_chromedriver as uc

        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1400,900")

        driver = uc.Chrome(options=options)

        with self._drivers_lock:
            self._drivers.append(driver)
        return driver

    def _driver(self):
        d = getattr(self._tl, "driver", None)
        if d is None:
            d = self._new_driver()
            self._tl.driver = d
        return d

    def close(self) -> None:
        with self._drivers_lock:
            drivers = list(self._drivers)
            self._drivers = []
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
        timeout_s: float = 25.0,
    ) -> str:
        driver = self._driver()
        driver.get(url)

        if wait_css or wait_js:
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support import expected_conditions as EC

            w = WebDriverWait(driver, timeout_s)
            if wait_css:
                w.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_css)))
            if wait_js:
                w.until(lambda drv: bool(drv.execute_script(wait_js)))

        return driver.page_source or ""

    def run(self) -> None:  # type: ignore[override]
        try:
            super().run()
        finally:
            self.close()

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        url = (raw_job.get("Detail URL") or "").strip()
        if not url:
            return None

        if getattr(self.adapter, "skip_detail_fetch", False):
            artifacts: Dict[str, Any] = {}
            record = self.adapter.normalize(self.cfg, raw_job, artifacts)
            record["artifacts"] = artifacts
            return record

        # optional wait hints for JS-heavy pages
        pag = getattr(self.cfg, "pagination", {}) or {}
        wait_css = pag.get("detail_wait_css") or pag.get("wait_css")
        wait_js = pag.get("detail_wait_js") or pag.get("wait_js")

        html = self.browser_get_html(url, wait_css=wait_css, wait_js=wait_js)
        artifacts = fetch_detail_artifacts_from_html(html, self.log, url)
        record = self.adapter.normalize(self.cfg, raw_job, artifacts)
        record["artifacts"] = artifacts
        return record
