# scrapers/platform_adapters/selenium_paged_html_search.py
from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup as BS


class SeleniumPagedHtmlSearchAdapter:
    """
    Leidos-style Selenium listing adapter.

    Expects cfg.search_url like:
      "https://careers.leidos.com/search/jobs/in?page={page}&q=#"

    Behavior:
      - Loads page 1 (once), waits, extracts total_pages
      - Extracts job links from page 1
      - Computes how many pages are needed to reach max_jobs (based on page 1 yield)
      - Iterates only those pages
      - Safety: if we add 0 jobs on a page (or for several pages), we stop early
    """

    def probe(self, cfg) -> float:
        pn = (getattr(cfg, "platform_name", "") or "").lower()
        if pn == "selenium_paged_html_search":
            return 1.0

        if bool(getattr(cfg, "requires_browser_bootstrap", False)):
            # If it explicitly needs browser bootstrap and looks like paged HTML
            su = getattr(cfg, "search_url", "") or ""
            if "{page}" in su:
                return 0.75
        return 0.0

    def list_jobs(self, scraper, cfg) -> List[Dict[str, Any]]:
        pag = cfg.pagination or {}
        base_url = (cfg.search_url or cfg.careers_home or "").strip()
        if not base_url:
            raise ValueError(f"{cfg.company_id}: missing search_url/careers_home")

        if "{page}" not in base_url:
            raise ValueError(
                f"{cfg.company_id}: selenium listing expects search_url with '{{page}}' placeholder"
            )

        job_link_selector = (
            pag.get("job_link_selector")
            or "a[href*='/jobs/'], a[href*='careers.leidos.com/jobs/']"
        ).strip()

        # IMPORTANT: default should be tolerant; "careers.leidos.com/jobs/" is fine,
        # but if your site redirects to "https://careers.leidos.com/jobs/12345" it's OK.
        # If this mismatch occurs, you’ll add 0 jobs and previously you paged forever.
        job_url_contains = (pag.get("job_url_contains") or "/jobs/").strip()

        scraper.log(
            "list:selectors",
            job_link_selector=job_link_selector,
            job_url_contains=job_url_contains,
        )

        posting_id_regex = pag.get("posting_id_regex") or r"(?:/jobs/)([0-9]+)"
        posting_id_re = re.compile(posting_id_regex)

        wait_js = (
            pag.get("wait_js")
            or "return document.querySelectorAll('div.jobs-section__item').length > 0;"
        ).strip()
        wait_css = (pag.get("wait_css") or "").strip() or None

        # Cap logic
        if getattr(scraper, "testing", False):
            max_jobs = int(getattr(scraper, "test_limit", 15) or 15)
        else:
            cap = getattr(scraper, "limit_per_scraper", None)
            max_jobs = int(cap) if cap is not None else 10**9

        if not hasattr(scraper, "browser_get_html"):
            raise ValueError(
                f"{cfg.company_id}: requires BrowserCompanyConfigScraper (missing browser_get_html)"
            )

        jobs: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()

        # ---- Load & parse page 1 ONCE ----
        first_url = base_url.format(page=1)
        html1 = scraper.browser_get_html(first_url, wait_css=wait_css, wait_js=wait_js)
        soup1 = BS(html1, "lxml")
        total_pages = self._extract_total_pages(soup1)
        scraper.log("source:total_pages", total_pages=total_pages, url=first_url)

        # Extract jobs from page 1 now
        added1, got1 = self._extract_page_jobs(
            soup1,
            page=1,
            url=first_url,
            job_link_selector=job_link_selector,
            job_url_contains=job_url_contains,
            posting_id_re=posting_id_re,
            jobs=jobs,
            seen_urls=seen_urls,
            max_jobs=max_jobs,
        )
        scraper.log("list:page", page=1, got=got1, added=added1, url=first_url)

        if len(jobs) >= max_jobs:
            scraper.log("list:fetched", count=len(jobs), reason="hit_cap_on_page_1")
            return jobs

        # If page 1 yields 0 added jobs, do NOT walk all pages — that’s your current failure mode.
        if added1 == 0:
            scraper.log(
                "list:stop",
                reason="page1_added_zero",
                hint="job_link_selector/job_url_contains likely wrong OR blocked/redirected",
                url=first_url,
            )
            return jobs

        # ---- Compute how many pages we *need* to reach the cap ----
        # Use yield from page 1 as estimate.
        per_page = max(1, added1)
        pages_needed = (
            int(math.ceil(max_jobs / per_page)) if max_jobs < 10**8 else total_pages
        )
        page_limit = min(total_pages, max(1, pages_needed))

        scraper.log(
            "list:plan",
            max_jobs=max_jobs,
            per_page_est=per_page,
            pages_needed=pages_needed,
            page_limit=page_limit,
            total_pages=total_pages,
        )

        # ---- Iterate remaining pages only up to page_limit ----
        zero_add_streak = 0
        for page in range(2, page_limit + 1):
            if len(jobs) >= max_jobs:
                break

            url = base_url.format(page=page)
            html = scraper.browser_get_html(url, wait_css=wait_css, wait_js=wait_js)
            soup = BS(html, "lxml")

            added, got = self._extract_page_jobs(
                soup,
                page=page,
                url=url,
                job_link_selector=job_link_selector,
                job_url_contains=job_url_contains,
                posting_id_re=posting_id_re,
                jobs=jobs,
                seen_urls=seen_urls,
                max_jobs=max_jobs,
            )

            scraper.log("list:page", page=page, got=got, added=added, url=url)

            if added == 0:
                zero_add_streak += 1
            else:
                zero_add_streak = 0

            # Safety stop: if we're not adding anything, don't burn through pages
            if zero_add_streak >= 2:
                scraper.log(
                    "list:stop",
                    reason="zero_add_streak",
                    streak=zero_add_streak,
                    page=page,
                )
                break

        scraper.log("list:fetched", count=len(jobs))
        return jobs

    @staticmethod
    def _extract_total_pages(soup) -> int:
        paginate = soup.find("div", class_="jobs-section__paginate")
        if not paginate:
            return 1
        nums: List[int] = []
        for a in paginate.find_all("a", href=True):
            txt = a.get_text(strip=True)
            if txt.isdigit():
                try:
                    nums.append(int(txt))
                except Exception:
                    pass
        return max(nums) if nums else 1

    @staticmethod
    def _extract_page_jobs(
        soup,
        *,
        page: int,
        url: str,
        job_link_selector: str,
        job_url_contains: str,
        posting_id_re: re.Pattern,
        jobs: List[Dict[str, Any]],
        seen_urls: set[str],
        max_jobs: int,
    ) -> Tuple[int, int]:
        links = soup.select(job_link_selector)
        got = 0
        added = 0

        for a in links:
            href = (a.get("href") or "").strip()
            if not href:
                continue

            abs_url = href if href.startswith("http") else urljoin(url, href)

            # Filter
            if job_url_contains and job_url_contains not in abs_url:
                continue

            got += 1

            if abs_url in seen_urls:
                continue

            seen_urls.add(abs_url)

            title = (a.get_text(" ", strip=True) or "").strip()
            pid = ""
            m = posting_id_re.search(abs_url)
            if m:
                pid = m.group(1)

            jobs.append(
                {
                    "Detail URL": abs_url,
                    "Posting ID": pid,
                    "Position Title": title,
                    "_page": page,
                }
            )
            added += 1

            if len(jobs) >= max_jobs:
                break

        return added, got

    def normalize(
        self, cfg, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
        url = (raw_job.get("Detail URL") or "").strip()
        return {
            "Posting ID": raw_job.get("Posting ID") or "",
            "Position Title": raw_job.get("Position Title") or "",
            "Detail URL": url,
            "Raw Location": raw_job.get("Raw Location") or "",
        }
