# scrapers/platform_adapters/usajobs_api.py
from __future__ import annotations

import os

from typing import Any, Dict, List
from urllib.parse import urlencode


class USAJobsApiAdapter:
    """
    USAJOBS REST API adapter.

    Listing-only:
      - Calls USAJOBS Search API
      - Returns Posting ID + Detail URL (+ minimal metadata)
    Detail:
      - You can still run fetch_detail_artifacts() on the Detail URL, but for USAJOBS
        the listing payload already contains most fields; normalize() uses listing + meta.
    """

    skip_detail_fetch = True

    def list_jobs(self, scraper, cfg) -> List[Dict[str, Any]]:
        pag = cfg.pagination or {}

        api_url = (pag.get("api_url") or cfg.search_url or "").strip()
        if not api_url:
            raise ValueError(
                f"{cfg.company_id}: missing pagination.api_url or entry_points.search_url"
            )

        # USAJOBS requires these headers:
        # - User-Agent
        # - Host
        # - Authorization-Key (aka API key)
        # - (optionally) X-Api-Key / X-Authorization-Key depending on key name you use
        headers = dict(cfg.headers or {})

        # ENV-FIRST (match legacy behavior): env vars override config values
        env_ua = os.getenv("USAJOBS_USER_AGENT", "").strip()
        env_key = os.getenv("USAJOBS_API_KEY", "").strip()

        if env_ua:
            headers["User-Agent"] = env_ua
        if env_key:
            headers["Authorization-Key"] = env_key

        # Normalize alternate key header names if user used them
        key = (
            headers.get("Authorization-Key")
            or headers.get("X-Api-Key")
            or headers.get("X-Authorization-Key")
        )
        if key:
            headers["Authorization-Key"] = key

        if not headers.get("Authorization-Key"):
            raise ValueError(
                f"{cfg.company_id}: missing USAJOBS API key. Set access_policy.headers.Authorization-Key "
                f"or set USAJOBS_API_KEY env var."
            )

        if not headers.get("User-Agent"):
            raise ValueError(
                f"{cfg.company_id}: missing User-Agent. Set access_policy.headers.User-Agent "
                f"or set USAJOBS_USER_AGENT env var."
            )

        headers.setdefault("Host", "data.usajobs.gov")
        headers.setdefault("Accept", "application/json")

        # Query params (defaults are reasonable; override in config.pagination)
        params: Dict[str, Any] = {}
        # Common filters:
        for k in (
            "Keyword",
            "LocationName",
            "Organization",
            "JobCategoryCode",
            "PayGradeLow",
            "PayGradeHigh",
            "RemunerationMinimumAmount",
            "RemunerationMaximumAmount",
            "PositionScheduleTypeCode",
            "SecurityClearanceRequired",
            "TravelPercentage",
            "DatePosted",
        ):
            v = pag.get(k)
            if v is not None and str(v).strip() != "":
                params[k] = v

        # Pagination: Page / ResultsPerPage
        page = int(pag.get("start_page") or 1)
        rpp = int(pag.get("results_per_page") or 50)
        max_pages = int(pag.get("max_pages") or 50)

        max_jobs = (
            int(getattr(scraper, "test_limit", 40))
            if getattr(scraper, "testing", False)
            else 10**9
        )

        out: List[Dict[str, Any]] = []
        for p in range(page, page + max_pages):
            params["Page"] = p
            params["ResultsPerPage"] = rpp
            params.setdefault("Fields", "full")

            scraper.log("list:page", page=p, url=api_url + "?" + urlencode(params))

            scraper.session.headers.update(headers)
            r = scraper.session.get(api_url, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json() or {}

            # Expected structure: SearchResult -> SearchResultItems -> [ { "MatchedObjectDescriptor": {...}} ]
            sr = payload.get("SearchResult") or {}
            items = sr.get("SearchResultItems") or []
            if not items:
                scraper.log("list:done", reason="empty", page=p)
                break

            for it in items:
                d = (
                    (it.get("MatchedObjectDescriptor") or {})
                    if isinstance(it, dict)
                    else {}
                )
                pos_id = str(d.get("PositionID") or "")
                # USAJOBS canonical detail URL is usually under PositionURI
                detail = d.get("PositionURI") or d.get("ApplyURI") or ""
                if isinstance(detail, list):
                    detail = detail[0] if detail else ""
                detail = (detail or "").strip()
                if not detail:
                    continue

                title = (d.get("PositionTitle") or "").strip()

                # Location(s): PositionLocationDisplay can be a string, and PositionLocation can be list
                loc = (d.get("PositionLocationDisplay") or "").strip()

                out.append(
                    {
                        "Posting ID": pos_id,
                        "Detail URL": detail,
                        "Position Title": title,
                        "Raw Location": loc,
                        "_usajobs": d,  # keep full descriptor for normalize()
                    }
                )

                if len(out) >= max_jobs:
                    break

            if len(out) >= max_jobs:
                break

        scraper.log("list:fetched", count=len(out))
        return out

    def normalize(
        self, cfg, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normalize primarily from the USAJOBS API payload captured in _usajobs.
        """
        d = raw_job.get("_usajobs") or {}

        def first(v, default=""):
            if v is None:
                return default
            if isinstance(v, str):
                return v.strip()
            return v

        # Dates may appear in PublicationStartDate
        post_date = first(d.get("PublicationStartDate")) or first(
            d.get("PositionStartDate")
        )

        # Salary: PositionRemuneration is list of dicts
        sal_raw = ""
        sal_min = None
        sal_max = None
        rem = d.get("PositionRemuneration")
        if isinstance(rem, list) and rem:
            r0 = rem[0] if isinstance(rem[0], dict) else {}
            sal_min = r0.get("MinimumRange")
            sal_max = r0.get("MaximumRange")
            cur = r0.get("RateIntervalCode") or ""
            sal_raw = f"{sal_min}–{sal_max} {cur}".strip("– ")

        # Work schedule / telework info
        sched = d.get("PositionSchedule") or []
        sched_name = ""
        if isinstance(sched, list) and sched:
            x = sched[0] if isinstance(sched[0], dict) else {}
            sched_name = first(x.get("Name"))

        return {
            "Posting ID": first(d.get("PositionID"))
            or first(raw_job.get("Posting ID")),
            "Position Title": first(d.get("PositionTitle"))
            or first(raw_job.get("Position Title")),
            "Detail URL": (
                artifacts.get("_canonical_url") or raw_job.get("Detail URL") or ""
            ).strip(),
            "Description": first(
                d.get("UserArea", {}).get("Details", {}).get("JobSummary")
            )
            or first(d.get("QualificationSummary"))
            or "",
            "Post Date": post_date,
            "Salary Raw": sal_raw,
            "Salary Min (USD/yr)": sal_min,
            "Salary Max (USD/yr)": sal_max,
            "Full Time Status": sched_name,
            "Raw Location": first(d.get("PositionLocationDisplay"))
            or first(raw_job.get("Raw Location")),
            # Some extra useful fields (safe):
            "Job Category": first(d.get("JobCategory", [{}])[0].get("Name"))
            if isinstance(d.get("JobCategory"), list)
            else "",
        }
