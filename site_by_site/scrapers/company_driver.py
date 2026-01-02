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


# -----------------------------
# Adapter registry
# -----------------------------
ADAPTERS = {
    "sitemap_job_urls": SitemapJobUrlsAdapter,
    "phenom": PhenomSitemapAdapter,
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

        # The ONLY place detail fetching happens in the spine:
        artifacts = fetch_detail_artifacts(self.thread_get, self.log, url)
        record = self.adapter.normalize(self.cfg, raw_job, artifacts)
        record["artifacts"] = artifacts
        return record
