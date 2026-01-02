# scrapers/platform_adapters/gd_api.py
from __future__ import annotations

from math import ceil
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup as BS

from utils.http import b64url_encode
from utils.extractors import extract_bold_block


class EncodedRequestApiAdapter:
    def _warm_session(self, scraper, cfg) -> None:
        pag = cfg.pagination or {}
        warmup_url = (
            pag.get("warmup_url") or cfg.search_url or cfg.careers_home or ""
        ).strip()
        referer = (pag.get("referer") or warmup_url).strip()

        scraper.session.headers.update(
            {
                "User-Agent": (cfg.headers or {}).get("User-Agent", "Mozilla/5.0"),
                "Accept": "application/json, text/plain, */*",
                "Referer": referer,
            }
        )

        if warmup_url:
            try:
                scraper.session.get(warmup_url, timeout=30)
            except Exception:
                scraper.log("bootstrap:warmup_failed", url=warmup_url)

            if warmup_url:
                try:
                    scraper.session.get(warmup_url, timeout=30)
                except Exception:
                    scraper.log("bootstrap:warmup_failed", url=warmup_url)

    def _call_api(
        self, scraper, api_url: str, request_token: str
    ) -> Tuple[Dict[str, Any], str]:
        url = f"{api_url}?{urlencode({'request': request_token})}"
        r = scraper.session.get(url, timeout=30)
        r.raise_for_status()
        return r.json(), url

    @staticmethod
    def _make_payload(page: int, page_size: int, use_facet: bool) -> Dict[str, Any]:
        return {
            "address": [],
            "facets": (
                [
                    {
                        "name": "career_page_size",
                        "values": [{"value": "200 Jobs Per Page"}],
                    }
                ]
                if use_facet
                else []
            ),
            "page": page,
            "pageSize": page_size,
            "what": "",
            "usedPlacesApi": False,
        }

    def probe(self, cfg) -> float:
        pn = (getattr(cfg, "platform_name", "") or "").lower()
        if pn == "encoded_request_api":
            return 1.0

        pag = getattr(cfg, "pagination", None) or {}
        api = pag.get("api_url") if isinstance(pag, dict) else ""
        if isinstance(api, str) and "API/Careers/CareerSearch" in api:
            return 0.9
        return 0.0

    def list_jobs(self, scraper, cfg) -> List[Dict[str, Any]]:
        self._warm_session(scraper, cfg)

        pag = cfg.pagination or {}
        page_size = int(pag.get("page_size") or 200)
        use_facet = bool(pag.get("use_facet", True))

        # testing cap
        target = (
            int(getattr(scraper, "test_limit", 40))
            if getattr(scraper, "testing", False)
            else 10**12
        )

        # Step 1: find a stable mode on page 0 (mirror legacy gd_scraper.py)
        page0_data: Optional[Dict[str, Any]] = None
        while True:
            try:
                api_url = (pag.get("api_url") or "").strip()
                if not api_url:
                    raise ValueError(
                        f"{cfg.company_id}: missing discovery_hints.pagination.api_url for EncodedRequestApiAdapter"
                    )
                tok0 = b64url_encode(self._make_payload(0, page_size, use_facet))
                page0_data, _ = self._call_api(scraper, api_url, tok0)
                break
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 400:
                    if use_facet:
                        use_facet = False
                        scraper.log(
                            "api:retry_mode",
                            reason="400",
                            use_facet=use_facet,
                            page_size=page_size,
                        )
                        continue
                    if page_size > 100:
                        page_size = 100
                        scraper.log(
                            "api:retry_mode",
                            reason="400",
                            use_facet=use_facet,
                            page_size=page_size,
                        )
                        continue
                raise

        total = int(page0_data.get("ResultTotal") or 0)
        scraper.log("source:total", total=total)

        pc = page0_data.get("PageCount")
        pc_int = (
            int(pc)
            if isinstance(pc, int) or (isinstance(pc, str) and str(pc).isdigit())
            else 0
        )
        calc = ceil(total / page_size) if total else 0
        max_pages = min(pc_int, calc) if (pc_int and calc) else (pc_int or calc or 1)

        def row_from_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            link = (item.get("Link") or {}).get("Url")
            if not link:
                return None
            loc0 = ((item.get("Locations") or [{}])[0]) or {}
            workplace_options = item.get("WorkplaceOptions") or []
            return {
                "Detail URL": urljoin("https://www.gd.com", link),
                "Posting ID": item.get("ReferenceCode"),
                "Full Time Status": ", ".join(item.get("EmploymentTypes") or []),
                "Job Category": item.get("Category"),
                "Clearance Level Must Possess": item.get("Clearance"),
                "Position Title": item.get("Title"),
                "Post Date": item.get("Date"),
                "Country": loc0.get("Country"),
                "State": loc0.get("State"),
                "Latitude": loc0.get("Latitude"),
                "Longitude": loc0.get("Longitude"),
                "Business Area": item.get("Company"),
                "Raw Location": loc0.get("Name"),
                "Remote Status": ""
                if len(workplace_options) == 0
                else ", ".join(workplace_options),
            }

        jobs: List[Dict[str, Any]] = []
        results0 = page0_data.get("Results") or []
        scraper.log("list:page", page=0, page_size=page_size, got=len(results0))

        for item in results0:
            if len(jobs) >= target:
                break
            row = row_from_item(item)
            if row:
                jobs.append(row)

        if len(jobs) >= target or max_pages <= 1:
            scraper.log("list:fetched", count=len(jobs))
            return jobs

        # Step 2: fetch remaining pages concurrently (like legacy)
        list_workers = int(
            pag.get("list_workers") or getattr(scraper, "list_workers", 6)
        )

        def fetch_page(p: int) -> List[Dict[str, Any]]:
            token = b64url_encode(self._make_payload(p, page_size, use_facet))
            data, _ = self._call_api(scraper, api_url, token)
            scraper.log(
                "list:page",
                page=p,
                page_size=page_size,
                got=len(data.get("Results") or []),
            )
            return data.get("Results") or []

        futures = {}
        page_results: List[Optional[List[Dict[str, Any]]]] = [None] * (max_pages - 1)

        with ThreadPoolExecutor(max_workers=list_workers) as ex:
            for p in range(1, max_pages):
                futures[ex.submit(fetch_page, p)] = p
            for fut in as_completed(futures):
                p = futures[fut]
                try:
                    page_results[p - 1] = fut.result()
                except Exception as e:
                    scraper.log(
                        "api:page_error",
                        level="warning",
                        page=p,
                        error=repr(e),
                        use_facet=use_facet,
                        page_size=page_size,
                    )

        for res in page_results:
            if not res:
                continue
            for item in res:
                if len(jobs) >= target:
                    break
                row = row_from_item(item)
                if row:
                    jobs.append(row)
            if len(jobs) >= target:
                break

        scraper.log("list:fetched", count=len(jobs))
        return jobs

    def normalize(
        self, cfg, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        GD detail pages are HTML with labeled bold blocks.
        Driver already fetched artifacts; use artifacts["_html"].
        """
        record = dict(raw_job)

        html = artifacts.get("_html") or ""
        if html:
            soup = BS(html, "lxml")
            blocks = extract_bold_block(soup)
            # legacy joined values into Description and extracted Career Level
            desc = "; ".join(
                [v for v in blocks.values() if isinstance(v, str) and v.strip()]
            )
            if desc:
                record["Description"] = desc
            if blocks.get("Career Level"):
                record["Career Level"] = blocks.get("Career Level")

        canon = artifacts.get("_canonical_url")
        if canon:
            record["Detail URL"] = canon

        return record
