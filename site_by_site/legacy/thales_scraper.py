from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse

from scrapers.engine import JobScraper
from utils.detail_fetchers import fetch_detail_artifacts
from utils.sitemap import parse_sitemap_index, parse_sitemap_xml


class ThalesScraper(JobScraper):
    """Scraper for Thales job postings."""

    VENDOR = "Thales"

    BASE_URL = "https://careers.thalesgroup.com"
    SEARCH_URL = f"{BASE_URL}/global/en"
    SITEMAP_INDEX_URL = f"{SEARCH_URL}/sitemap_index.xml"

    # Thales job URLs in the sitemap are of the form:
    #   https://careers.thalesgroup.com/global/en/job/R0123456/Some-Title
    JOB_URL_SUBSTR = "/global/en/job/"

    def __init__(self) -> None:
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})

    # ---------------------------------------------------------------------
    # Listing (via sitemap index)
    # ---------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, str]]:
        # 1) Sitemap index
        idx_resp = self.get(self.SITEMAP_INDEX_URL)
        idx_resp.raise_for_status()

        sm_entries = parse_sitemap_index(idx_resp.content)
        sitemap_urls = [e["loc"] for e in sm_entries if e.get("loc")]
        self.log(
            "list:sitemap_index", url=self.SITEMAP_INDEX_URL, sitemaps=len(sitemap_urls)
        )

        # 2) Child sitemaps -> job URLs
        job_urls: List[str] = []
        for sm_url in sitemap_urls:
            r = self.get(sm_url)
            r.raise_for_status()
            entries = parse_sitemap_xml(
                r.content,
                url_filter=lambda loc, _needle=self.JOB_URL_SUBSTR: _needle in loc,
            )
            batch = [e["loc"] for e in entries if e.get("loc")]
            job_urls.extend(batch)
            self.log("list:sitemap", sitemap=sm_url, urls=len(batch))

        # 3) De-dupe URLs deterministically (preserve order)
        seen: set[str] = set()
        uniq_urls: List[str] = []
        for u in job_urls:
            if u in seen:
                continue
            seen.add(u)
            uniq_urls.append(u)

        # 4) Testing limiter (applies after de-dupe for predictable counts)
        if getattr(self, "testing", False):
            try:
                limit = int(getattr(self, "test_limit", 15)) or 15
            except Exception:
                limit = 15
            uniq_urls = uniq_urls[:limit]

        jobs: List[Dict[str, str]] = []
        for url in uniq_urls:
            posting_id = self._extract_posting_id(url)
            jobs.append({"Posting ID": posting_id, "Detail URL": url})

        self.log(
            "list:sitemap_urls",
            total_urls=len(job_urls),
            unique_urls=len(uniq_urls),
            jobs=len(jobs),
        )
        self.log("list:done", total=len(jobs))
        return jobs

    @classmethod
    def _extract_posting_id(cls, url: str) -> str:
        """Extract the requisition id from a Thales job URL.

        Expected path structure: /global/en/job/<REQID>/<slug>
        """
        p = urlparse(url)
        parts = [x for x in (p.path or "").split("/") if x]
        # Find the 'job' segment and take the next element as the req id.
        for i, seg in enumerate(parts):
            if seg == "job" and i + 1 < len(parts):
                return parts[i + 1]
        raise ValueError(f"Unrecognized Thales job URL format: {url}")

    # ---------------------------------------------------------------------
    # Detail parsing
    # ---------------------------------------------------------------------
    def parse_job(self, job_entry: Dict[str, str]) -> Dict[str, Any]:
        url = job_entry["Detail URL"]
        job_id = job_entry["Posting ID"]

        artifacts = fetch_detail_artifacts(
            self.thread_get,
            self.log,
            url,
            get_vendor_blob=False,
            get_datalayer=False,
        )

        jsonld: Dict[str, Any] = artifacts.get("_jsonld") or {}
        meta: Dict[str, str] = artifacts.get("_meta") or {}

        # JSON-LD JobPosting fields (preferred)
        position_title = jsonld.get("title")
        description = jsonld.get("description")
        post_date = jsonld.get("datePosted")
        employment_type = jsonld.get("employmentType")

        city = jsonld.get("jobLocation.0.address.addressLocality")
        state = jsonld.get("jobLocation.0.address.addressRegion")
        country = jsonld.get("jobLocation.0.address.addressCountry")
        postal_code = jsonld.get("jobLocation.0.address.postalCode")

        # Meta tags (Thales pages often include GTM-ish metadata)
        raw_location = meta.get("meta.gtm_tbcn_location") or meta.get("meta.location")
        business_sector = meta.get("meta.gtm_tbcn_division")
        detail_url = artifacts.get("_canonical_url") or url

        record: Dict[str, Any] = {
            "Posting ID": job_id,
            "Position Title": position_title,
            "Detail URL": detail_url,
            "Description": description,
            "Post Date": post_date,
            "Full Time Status": employment_type,
            "Raw Location": raw_location,
            "Country": country,
            "State": state,
            "City": city,
            "Postal Code": postal_code,
            "Business Sector": business_sector,
        }

        return record
