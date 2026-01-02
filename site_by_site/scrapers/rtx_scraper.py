from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse

from scrapers.engine import JobScraper
from utils.detail_fetchers import fetch_detail_artifacts
from utils.sitemap import parse_sitemap_index, parse_sitemap_xml


class RTXScraper(JobScraper):
    VENDOR = "RTX"

    BASE_URL = "https://careers.rtx.com"
    SEARCH_URL = f"{BASE_URL}/global/en/search-results"
    SITEMAP_INDEX_URL = f"{BASE_URL}/global/en/sitemap_index.xml"

    def __init__(self) -> None:
        super().__init__(
            base_url=self.SEARCH_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/webp,*/*;q=0.8"
                ),
                "Connection": "keep-alive",
            },
        )

    # -------------------------------------------------------------------------
    # Listing via sitemap index
    # -------------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, Any]]:
        # Determine job limit in testing vs normal mode
        if getattr(self, "testing", False):
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 0
            except Exception:
                job_limit = 15
        else:
            job_limit = float("inf")  # type: ignore[assignment]

        # 1) Load sitemap index
        self.log("sitemap_index:fetch", url=self.SITEMAP_INDEX_URL)
        index_resp = self.get(self.SITEMAP_INDEX_URL)
        index_resp.raise_for_status()

        sitemap_entries = parse_sitemap_index(index_resp.content)
        sitemap_urls = [e["loc"] for e in sitemap_entries]

        self.log("sitemap_index:parsed", count=len(sitemap_urls))

        # 2) Walk each sitemap and collect job URLs
        all_urls: List[str] = []
        seen_urls = set()

        for sm_url in sitemap_urls:
            self.log("sitemap:fetch", url=sm_url)
            sm_resp = self.get(sm_url)
            sm_resp.raise_for_status()

            url_entries = parse_sitemap_xml(
                sm_resp.content,
                url_filter=lambda loc: "/job/" in loc,
            )
            for entry in url_entries:
                loc = entry["loc"]
                if loc in seen_urls:
                    continue
                seen_urls.add(loc)
                all_urls.append(loc)

                if len(all_urls) >= job_limit and job_limit != float("inf"):
                    break

            self.log("sitemap:parsed", sitemap=sm_url, urls=len(url_entries))

            if len(all_urls) >= job_limit and job_limit != float("inf"):
                break

        self.log("list:sitemap", total_urls=len(all_urls))

        # 3) Convert URLs into minimal listing dicts
        jobs: List[Dict[str, Any]] = []
        for url in all_urls:
            job_id = self._extract_job_id(url)
            jobs.append(
                {
                    "Posting ID": job_id,
                    "Detail URL": url,
                }
            )

        self.log("list:done", total=len(jobs))
        return jobs

    @staticmethod
    def _extract_job_id(url: str) -> str:
        path_parts = urlparse(url).path.rstrip("/").split("/")
        if len(path_parts) >= 2:
            return path_parts[-2]
        return path_parts[-1]

    # -------------------------------------------------------------------------
    # Detail parsing
    # -------------------------------------------------------------------------
    def parse_job(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        # Detail URL and job ID
        detail_url = raw_job.get("Detail URL") or ""
        if not detail_url:
            raise ValueError("Missing Detail URL in raw_job")

        job_id = raw_job.get("Posting ID") or self._extract_job_id(detail_url)

        artifacts = fetch_detail_artifacts(
            self.thread_get,
            self.log,
            detail_url,
            get_datalayer=False,
            get_meta=False,
        )
        jsonld: Dict[str, Any] = artifacts.get("_jsonld") or {}
        phapp: Dict[str, Any] = artifacts.get("_vendor_blob") or {}
        canonical_url = artifacts.get("_canonical_url") or detail_url

        position_title = phapp.get("title") or jsonld.get("title")

        description = jsonld.get("description") or phapp.get("description")

        raw_location = phapp.get("address") or phapp.get("location")

        employment_type = phapp.get("type") or jsonld.get("employmentType")

        remote_status = phapp.get("locationType")

        state = phapp.get("state")
        city = phapp.get("city")
        country = phapp.get("country")
        latitude = phapp.get("latitude")
        longitude = phapp.get("longitude")

        career_level = phapp.get("experienceLevel")
        post_date = phapp.get("postedDate")
        job_category = phapp.get("category")

        postal_code = jsonld.get("jobLocation.0.address.postalCode")
        hours_per_week = jsonld.get("workHours")

        business_area = phapp.get("businessUnit")
        relocation_available = phapp.get("relocationEligible")
        clearance_must_possess = phapp.get("clearanceType")

        return {
            "Position Title": position_title,
            "Description": description,
            "Raw Location": raw_location,
            "Posting ID": job_id,
            "Detail URL": canonical_url,
            "Full Time Status": employment_type,
            "Remote Status": remote_status,
            "State": state,
            "City": city,
            "Latitude": latitude,
            "Longitude": longitude,
            "Career Level": career_level,
            "Country": country,
            "Post Date": post_date,
            "Job Category": job_category,
            "Postal Code": postal_code,
            "Hours Per Week": hours_per_week,
            "Business Area": business_area,
            "Relocation Available": relocation_available,
            "Clearance Level Must Possess": clearance_must_possess,
        }
