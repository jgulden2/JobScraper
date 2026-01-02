from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup as BS

from scrapers.engine import JobScraper
from utils.extractors import extract_bold_block
from utils.detail_fetchers import fetch_detail_artifacts
from utils.sitemap import parse_sitemap_xml


class BoeingScraper(JobScraper):
    VENDOR = "Boeing"
    BASE_URL = "https://jobs.boeing.com"
    SEARCH_URL = f"{BASE_URL}/search-jobs"
    SITEMAP_URL = f"{BASE_URL}/sitemap.xml"

    def __init__(self, max_pages: Optional[int] = None) -> None:
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.max_pages: Optional[int] = max_pages

    def fetch_data(self) -> List[Dict[str, str]]:
        resp = self.get(self.SITEMAP_URL)
        resp.raise_for_status()

        entries = parse_sitemap_xml(
            resp.content,
            url_filter=lambda loc: "/job/" in loc,
        )

        urls = [e["loc"] for e in entries]

        if getattr(self, "testing", False):
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 0
            except Exception:
                job_limit = 15
        else:
            job_limit = float("inf")

        if job_limit and job_limit != float("inf"):
            urls = urls[: int(job_limit)]

        jobs: List[Dict[str, str]] = []
        seen_urls = set()

        for url in urls:
            if url in seen_urls:
                continue
            seen_urls.add(url)

            posting_id = self._extract_posting_id(url)
            jobs.append(
                {
                    "Posting ID": posting_id,
                    "Detail URL": url,
                }
            )

        self.log("list:sitemap", total_urls=len(urls), unique=len(jobs))
        self.log("list:done", total=len(jobs))
        return jobs

    @staticmethod
    def _extract_posting_id(url: str) -> str:
        path = urlparse(url).path.rstrip("/")
        return path.split("/")[-1]

    def parse_job(self, job_entry: Dict[str, str]) -> Dict[str, Any]:
        url = job_entry.get("Detail URL") or ""
        if not url:
            raise ValueError("Missing Detail URL in job_entry")

        artifacts = fetch_detail_artifacts(
            self.thread_get,
            self.log,
            url,
            get_vendor_blob=False,
            get_datalayer=False,
        )
        jsonld: Dict[str, Any] = artifacts.get("_jsonld") or {}
        meta: Dict[str, Any] = artifacts.get("_meta") or {}
        html = artifacts.get("_html") or ""
        soup = BS(html, "lxml") if html else None
        canonical_url = artifacts.get("_canonical_url") or url

        # ID & title
        job_id = (
            job_entry.get("Posting ID")
            or jsonld.get("identifier.value")
            or meta.get("gtm_jobid")
            or ""
        )
        title = (
            job_entry.get("Position Title")
            or jsonld.get("title")
            or meta.get("gtm_tbcn_jobtitle")
            or ""
        )

        # Location
        raw_location = (
            job_entry.get("Raw Location") or meta.get("gtm_tbcn_location") or ""
        )
        city = jsonld.get("jobLocation.0.address.addressLocality")
        state = jsonld.get("jobLocation.0.address.addressRegion")
        country = jsonld.get("jobLocation.0.address.addressCountry")

        # Business / function
        business_area = jsonld.get("industry")
        business_sector = meta.get("gtm_tbcn_division")
        job_function = meta.get("gtm_tbcn_jobcategory")

        # Description: prefer labeled bold blocks, fall back to JSON-LD description
        description = ""
        if soup is not None:
            blocks = extract_bold_block(soup)
            if blocks:
                description = "; ".join(blocks.values())
        if not description:
            description = jsonld.get("description") or ""

        employment_type = jsonld.get("employmentType")

        record: Dict[str, Any] = {
            "Vendor": self.VENDOR,
            "Posting ID": job_id,
            "Position Title": title,
            "Detail URL": canonical_url,
            "Raw Location": raw_location,
            "City": city,
            "State": state,
            "Country": country,
            "Business Area": business_area,
            "Business Sector": business_sector,
            "Job Function/Discipline": job_function,
            "Clearance Level Must Possess": employment_type,
            "Clearance Level Must be Able to Obtain": None,
            "Description": description,
        }

        # Surface a few useful meta fields for debugging/analysis
        record["Source Meta.gtm_tbcn_division"] = meta.get("gtm_tbcn_division")
        record["Source Meta.gtm_tbcn_jobcategory"] = meta.get("gtm_tbcn_jobcategory")
        record["Source Meta.gtm_tbcn_location"] = meta.get("gtm_tbcn_location")

        return record
