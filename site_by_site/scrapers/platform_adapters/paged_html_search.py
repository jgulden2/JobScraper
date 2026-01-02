# scrapers/platform_adapters/paged_html_search.py
from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse, urlencode, parse_qs

from bs4 import BeautifulSoup as BS


class PagedHtmlSearchAdapter:
    """
    Generic adapter for "search jobs" pages that paginate in HTML and include job links.

    Supports two pagination styles:
      - page-based:  ?page=1,2,3...
      - offset-based: ?startrow=0,25,50... (HII style)

    Config via discovery_hints.pagination, examples:
      {
        "page_param": "page",
        "start_page": 1,
        "max_pages": 200,
        "job_link_selector": "a[href*='/job/']",
        "job_url_contains": "/job/",
        "posting_id_regex": "(?:jobId=|/job/)([A-Za-z0-9_-]+)"
      }

    Offset mode:
      {
        "offset_param": "startrow",
        "start_offset": 0,
        "page_size": 25,
        "max_pages": 400,
        "fixed_params": {"q":"","sortColumn":"referencedate","sortDirection":"desc"},
        "job_link_selector": "a.jobTitle-link",
        "job_url_contains": "/job/",
        "posting_id_regex": "(?:/job/)([A-Za-z0-9_-]+)"
      }
    """

    def probe(self, cfg) -> float:
        pn = (getattr(cfg, "platform_name", "") or "").lower()
        if pn == "paged_html_search":
            return 1.0

        dtype = (getattr(cfg, "discovery_type", "") or "").lower()
        if dtype == "html_search":
            return 0.6

        su = (
            (getattr(cfg, "search_url", "") or "")
            + " "
            + (getattr(cfg, "careers_home", "") or "")
        )
        if "search-jobs" in su or "/search/" in su:
            return 0.35
        return 0.0

    def list_jobs(self, scraper, cfg) -> List[Dict[str, Any]]:
        pag = cfg.pagination or {}
        base_url = (cfg.search_url or cfg.careers_home or "").strip()
        if not base_url:
            raise ValueError(f"{cfg.company_id}: missing search_url/careers_home")

        # Pagination controls
        page_param = (pag.get("page_param") or "page").strip()
        start_page = int(pag.get("start_page") or 1)

        offset_param = (pag.get("offset_param") or "").strip()
        start_offset = int(pag.get("start_offset") or 0)
        page_size = int(pag.get("page_size") or 25)

        max_pages = int(pag.get("max_pages") or 200)

        fixed_params = dict(pag.get("fixed_params") or {})
        if isinstance(fixed_params, list):
            fixed_params = dict(fixed_params)

        job_link_selector = (pag.get("job_link_selector") or "a[href]").strip()
        job_url_contains = (
            pag.get("job_url_contains") or cfg.job_url_contains or "/job/"
        ).strip()

        posting_id_regex = (
            pag.get("posting_id_regex") or r"(?:jobId=|/job/)([A-Za-z0-9_-]+)"
        )
        posting_id_re = re.compile(posting_id_regex)

        max_jobs = (
            int(getattr(scraper, "test_limit", 40))
            if getattr(scraper, "testing", False)
            else 10**9
        )

        jobs: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()

        # -----------------------------
        # Offset pagination (HII)
        # -----------------------------
        if offset_param:
            for page_idx in range(0, max_pages):
                if len(jobs) >= max_jobs:
                    break

                offset = start_offset + (page_idx * page_size)
                url = self._with_query(
                    base_url, {offset_param: str(offset), **fixed_params}
                )

                r = scraper.get(url, timeout=30)
                r.raise_for_status()

                soup = BS(r.text, "lxml")
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
                            "_page": page_idx,
                            "_offset": offset,
                        }
                    )

                    if len(jobs) >= max_jobs:
                        break

                scraper.log(
                    "list:page", page=page_idx, offset=offset, found=found, url=url
                )

                if found == 0:
                    scraper.log("list:done", reason="no_links", page=page_idx)
                    break

            scraper.log("list:fetched", count=len(jobs))
            return jobs

        # -----------------------------
        # Page pagination (default)
        # -----------------------------
        for page in range(start_page, start_page + max_pages):
            url = self._with_query(base_url, {page_param: str(page), **fixed_params})

            r = scraper.get(url, timeout=30)
            r.raise_for_status()

            soup = BS(r.text, "lxml")
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

            if len(jobs) >= max_jobs:
                break

        scraper.log("list:fetched", count=len(jobs))
        return jobs

    def normalize(
        self, cfg, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
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

    @staticmethod
    def _with_query(url: str, params: Dict[str, str]) -> str:
        u = urlparse(url)
        q = parse_qs(u.query)
        for k, v in params.items():
            q[k] = [v]
        new_q = urlencode({k: v[-1] for k, v in q.items() if v}, doseq=False)
        return u._replace(query=new_q).geturl()
