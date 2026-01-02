# scrapers/platform_adapters/selenium_paged_html_search.py
from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup as BS


class SeleniumPagedHtmlSearchAdapter:
    """
    Browser-backed variant of PagedHtmlSearchAdapter.

    Requirements:
      - scraper must implement browser_get_html(url, wait_css=..., wait_js=...)

    Config (discovery_hints.pagination):
      {
        "start_page": 1,
        "max_pages": 200,
        "job_link_selector": "div.jobs-section__item a[href]",
        "job_url_contains": "/job/",
        "posting_id_regex": "(?:/job/)([0-9]+)",
        "wait_js": "return document.querySelectorAll('div.jobs-section__item').length > 0;",
        "detail_wait_js": "return document.readyState === 'complete';"
      }
    """

    def list_jobs(self, scraper, cfg) -> List[Dict[str, Any]]:
        pag = cfg.pagination or {}
        base_url = (cfg.search_url or cfg.careers_home or "").strip()
        if not base_url:
            raise ValueError(f"{cfg.company_id}: missing search_url/careers_home")

        start_page = int(pag.get("start_page") or 1)
        max_pages = int(pag.get("max_pages") or 200)

        job_link_selector = (pag.get("job_link_selector") or "a[href]").strip()
        job_url_contains = (
            pag.get("job_url_contains") or cfg.job_url_contains or ""
        ).strip()

        posting_id_regex = (
            pag.get("posting_id_regex") or r"(?:jobId=|/job/)([A-Za-z0-9_-]+)"
        )
        posting_id_re = re.compile(posting_id_regex)

        wait_css = (pag.get("wait_css") or "").strip() or None
        wait_js = (pag.get("wait_js") or "").strip() or None

        max_jobs = (
            int(getattr(scraper, "test_limit", 40))
            if getattr(scraper, "testing", False)
            else 10**9
        )

        if not hasattr(scraper, "browser_get_html"):
            raise ValueError(
                f"{cfg.company_id}: selenium_paged_html_search requires BrowserCompanyConfigScraper (missing browser_get_html)"
            )

        jobs: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()

        for page in range(start_page, start_page + max_pages):
            if len(jobs) >= max_jobs:
                break

            url = base_url.format(page=page) if "{page}" in base_url else base_url

            html = scraper.browser_get_html(url, wait_css=wait_css, wait_js=wait_js)
            soup = BS(html, "lxml")

            links = soup.select(job_link_selector)
            found = 0

            for a in links:
                href = a.get("href") or ""
                if not href:
                    continue

                abs_url = urljoin(url, href)
                if job_url_contains and job_url_contains not in abs_url:
                    continue
                if abs_url in seen_urls:
                    continue

                seen_urls.add(abs_url)
                found += 1

                pid = ""
                m = posting_id_re.search(abs_url)
                if m:
                    pid = m.group(1)

                title = (a.get_text(" ", strip=True) or "").strip()

                jobs.append(
                    {
                        "Detail URL": abs_url,
                        "Posting ID": pid,
                        "Position Title": title,
                        "_page": page,
                    }
                )

                if len(jobs) >= max_jobs:
                    break

            scraper.log("list:page", page=page, found=found, url=url)

            if found == 0:
                scraper.log("list:done", reason="no_links", page=page)
                break

        scraper.log("list:fetched", count=len(jobs))
        return jobs

    def normalize(
        self, cfg, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
        # Same “detail JSON-LD/meta” normalization strategy as paged_html_search.
        url = (raw_job.get("Detail URL") or "").strip()
        detail_url = (artifacts.get("_canonical_url") or url).strip()

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
        if isinstance(employment_type, list):
            employment_type = employment_type[0] if employment_type else ""

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
            "Full Time Status": str(employment_type or "").strip(),
            "City": city,
            "State": state,
            "Country": country,
            "Postal Code": postal,
            "Raw Location": meta.get("meta.gtm_tbcn_location")
            or raw_job.get("Raw Location")
            or "",
        }
