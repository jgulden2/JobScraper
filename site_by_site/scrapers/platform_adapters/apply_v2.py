# scrapers/platform_adapters/apply_v2.py
from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse

from scrapers.platform_adapters.base import Adapter


class ApplyV2Adapter(Adapter):
    """
    Generic adapter for ATS sites exposing:
      GET /api/apply/v2/jobs?domains=...&domain=...&start=...&num=...&sort_by=recent

    Listing-only:
      - returns stable Posting ID + Detail URL (+ optional minimal metadata)
    Detail is handled by CompanyConfigScraper via fetch_detail_artifacts().
    """

    @classmethod
    def probe(cls, cfg) -> float:
        pn = (getattr(cfg, "platform_name", "") or "").lower()
        if pn == "apply_v2":
            return 1.0

        api = ""
        pag = getattr(cfg, "pagination", None) or {}
        if isinstance(pag, dict):
            api = pag.get("api_url") or ""
        if "/api/apply/v2/jobs" in (api or "") or "/api/apply/v2/jobs" in (
            getattr(cfg, "search_url", "") or ""
        ):
            return 0.9
        return 0.0

    @staticmethod
    def _default_api_url(careers_home: str) -> str:
        # e.g. https://jobs.northropgrumman.com -> https://jobs.northropgrumman.com/api/apply/v2/jobs
        u = urlparse(careers_home)
        return f"{u.scheme}://{u.netloc}/api/apply/v2/jobs"

    @classmethod
    def list_jobs(cls, scraper, cfg) -> List[Dict[str, Any]]:
        # Warm-up: some ApplyV2 sites want a cookie primed by the HTML site
        # (Northrop’s legacy scraper explicitly primes cookies).
        try:
            scraper.get(cfg.careers_home, timeout=30)
        except Exception:
            # non-fatal; the API call may still work
            scraper.log("bootstrap:warmup_failed", url=cfg.careers_home)

        pag = cfg.pagination or {}

        api_url = (pag.get("api_url") or cfg.search_url or "").strip()
        if not api_url:
            api_url = cls._default_api_url(cfg.careers_home)

        domains = (pag.get("domains") or pag.get("domain") or "").strip() or cfg.domain
        domain = (pag.get("domain") or "").strip() or domains
        sort_by = (pag.get("sort_by") or "recent").strip()

        start = 0
        num = int(pag.get("page_size") or pag.get("num") or 100)

        # Keep listing lightweight; CompanyConfigScraper will slice in testing mode anyway,
        # but we can avoid fetching tons of pages in --testing.
        max_jobs = (
            int(getattr(scraper, "test_limit", 40))
            if getattr(scraper, "testing", False)
            else 10**9
        )
        if getattr(scraper, "testing", False):
            num = min(num, 25)

        jobs: List[Dict[str, Any]] = []
        total_count = None
        page_idx = 0

        while True:
            params = {
                "domains": domains,
                "domain": domain,
                "start": start,
                "num": num,
                "sort_by": sort_by,
            }
            r = scraper.get(api_url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json() or {}

            if total_count is None:
                try:
                    total_count = int(data.get("count", 0))
                except Exception:
                    total_count = None

            batch = data.get("positions", []) or []
            got = len(batch)

            scraper.log("list:page", page=page_idx, start=start, got=got, url=r.url)

            if not batch:
                break

            for j in batch:
                # Northrop’s legacy listing mapping: id, ats_job_id, name, location, department, canonicalPositionUrl.
                pid = str(j.get("id") or "")
                detail_url = (j.get("canonicalPositionUrl") or "").strip()

                if not detail_url:
                    continue

                jobs.append(
                    {
                        "Posting ID": pid or str(j.get("ats_job_id") or ""),
                        "Detail URL": detail_url,
                        "Position Title": (j.get("name") or "").strip(),
                        "Raw Location": (j.get("location") or "").strip(),
                        "Job Category": (j.get("department") or "").strip(),
                        # keep a tiny passthrough for debugging if you want
                        "_applyv2": {"ats_job_id": j.get("ats_job_id", "")},
                    }
                )

                if len(jobs) >= max_jobs:
                    break

            if len(jobs) >= max_jobs:
                break

            start += got
            page_idx += 1

            if total_count is not None and start >= total_count:
                break

        scraper.log("list:fetched", count=len(jobs))
        return jobs

    @classmethod
    def normalize(
        cls, cfg, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Convert listing + artifacts into your raw record dict.
        Canonicalization happens later in your pipeline.
        """
        url = (raw_job.get("Detail URL") or "").strip()
        detail_url = artifacts.get("_canonical_url") or url

        jsonld = artifacts.get("_jsonld") or {}
        meta = artifacts.get("_meta") or {}

        title = jsonld.get("title") or raw_job.get("Position Title") or ""
        desc = jsonld.get("description") or ""
        post_date = jsonld.get("datePosted") or raw_job.get("Post Date") or ""

        city = jsonld.get("jobLocation.0.address.addressLocality") or ""
        state = jsonld.get("jobLocation.0.address.addressRegion") or ""
        country = jsonld.get("jobLocation.0.address.addressCountry") or ""
        postal = jsonld.get("jobLocation.0.address.postalCode") or ""

        employment_type = (
            jsonld.get("employmentType") or raw_job.get("Full Time Status") or ""
        )

        posting_id = (
            (
                (jsonld.get("identifier") or {}).get("value")
                if isinstance(jsonld.get("identifier"), dict)
                else ""
            )
            or raw_job.get("Posting ID")
            or ""
        )

        return {
            "Posting ID": posting_id,
            "Position Title": title,
            "Detail URL": detail_url,
            "Description": desc,
            "Post Date": post_date,
            "Full Time Status": employment_type,
            "Job Category": raw_job.get("Job Category") or "",
            "Raw Location": raw_job.get("Raw Location") or "",
            "City": city,
            "State": state,
            "Country": country,
            "Postal Code": postal,
            # optional passthroughs if present
            "Business Sector": meta.get("meta.gtm_tbcn_division") or "",
        }
