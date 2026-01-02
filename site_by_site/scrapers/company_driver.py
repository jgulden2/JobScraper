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
from scrapers.platform_adapters.sitemap_job_urls import SitemapJobUrlsAdapter
from scrapers.platform_adapters.base import Adapter
from scrapers.platform_adapters.phenom_sitemap import PhenomSitemapAdapter
from scrapers.platform_adapters.apply_v2 import ApplyV2Adapter
from scrapers.platform_adapters.paged_html_search import PagedHtmlSearchAdapter
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
