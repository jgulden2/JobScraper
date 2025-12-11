from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse

from scrapers.base import JobScraper
from utils.detail_fetchers import fetch_detail_artifacts
from utils.sitemap import parse_sitemap_xml


class L3HarrisScraper(JobScraper):
    VENDOR = "L3Harris"

    BASE_URL = "https://careers.l3harris.com"
    SEARCH_URL = f"{BASE_URL}/en/search-jobs"
    SITEMAP_URL = f"{BASE_URL}/en/sitemap.xml"

    def __init__(self, delay: float = 0.5) -> None:
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.delay = delay

    def fetch_data(self) -> List[Dict[str, str]]:
        resp = self.get(self.SITEMAP_URL)
        resp.raise_for_status()

        # Keep only URLs that look like job postings.
        entries = parse_sitemap_xml(
            resp.text,
            url_filter=lambda loc: "/job/" in loc,
        )

        urls = [e["loc"] for e in entries]

        if getattr(self, "testing", False):
            try:
                limit = int(getattr(self, "test_limit", 15)) or 15
            except Exception:
                limit = 15
            urls = urls[:limit]

        jobs: List[Dict[str, str]] = []
        for url in urls:
            posting_id = self._extract_posting_id(url)
            jobs.append(
                {
                    "Posting ID": posting_id,
                    "Detail URL": url,
                    # "Last Modified": entry["lastmod"],
                }
            )

        self.log("list:done", total=len(jobs))
        return jobs

    @staticmethod
    def _extract_posting_id(url: str) -> str:
        path = urlparse(url).path.rstrip("/")
        return path.split("/")[-1]

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

        position_title = jsonld.get("title")
        description = jsonld.get("description")
        post_date = jsonld.get("datePosted")

        city = jsonld.get("jobLocation.0.address.addressLocality")
        state = jsonld.get("jobLocation.0.address.addressRegion")
        country = jsonld.get("jobLocation.0.address.addressCountry")
        postal_code = jsonld.get("jobLocation.0.address.postalCode")

        employment_type = jsonld.get("employmentType")

        ats_req_id = meta.get("meta.job-ats-req-id")
        raw_location = meta.get("meta.gtm_tbcn_location")
        business_sector = meta.get("meta.gtm_tbcn_division")

        detail_url = artifacts.get("_canonical_url") or url

        record: Dict[str, Any] = {
            "Posting ID": ats_req_id or job_id,
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
