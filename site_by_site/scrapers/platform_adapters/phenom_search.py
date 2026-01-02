# scrapers/platform_adapters/phenom_search.py
from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse, urlencode, parse_qs

from utils.extractors import extract_phapp_ddo, extract_total_results


class PhenomSearchAdapter:
    """
    Phenom pattern where listing is an HTML "search-results" page with pagination via offset.

    Example (BAE):
      https://jobs.baesystems.com/global/en/search-results?from=0&s=1

    Listing:
      - Fetch search_url with offset param (default: from)
      - Parse phApp.ddo from listing HTML to extract job IDs + minimal metadata
      - Return rows containing at least:
          "Posting ID", "Detail URL", "Position Title", "Raw Location"
    Detail:
      - CompanyConfigScraper will call fetch_detail_artifacts() on Detail URL
      - normalize() reads artifacts["_vendor_blob"] (Phenom job detail payload)
    """

    def probe(self, cfg) -> float:
        pn = (getattr(cfg, "platform_name", "") or "").lower()
        if pn == "phenom_search":
            return 1.0

        su = getattr(cfg, "search_url", "") or ""
        # Common phenom search pattern
        if "search-results" in su and "/en/" in su:
            return 0.8
        return 0.0

    def list_jobs(self, scraper, cfg) -> List[Dict[str, Any]]:
        pag = cfg.pagination or {}

        search_url = (cfg.search_url or "").strip()
        if not search_url:
            raise ValueError(
                f"{cfg.company_id}: missing entry_points.search_url for phenom_search"
            )

        # Pagination controls
        offset_param = (pag.get("offset_param") or "from").strip()
        page_size = int(pag.get("page_size") or 50)
        max_pages = int(pag.get("max_pages") or 200)

        # Extra fixed query params (e.g. {"s":"1"})
        fixed_params = dict(pag.get("fixed_params") or {})
        # Some configs may specify fixed_params as a list of pairs; normalize
        if isinstance(fixed_params, list):
            fixed_params = dict(fixed_params)

        # Testing cap
        max_jobs = (
            int(getattr(scraper, "test_limit", 40))
            if getattr(scraper, "testing", False)
            else 10**9
        )
        if getattr(scraper, "testing", False):
            page_size = min(page_size, 25)

        # Detail URL template
        # Default matches the common Phenom tenant pattern; BAE uses /global/en/job/{jobId}/
        detail_path_template = (
            pag.get("detail_path_template") or "/global/en/job/{jobId}/"
        ).strip()

        jobs: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        # First page to learn total
        total = None
        offset = 0
        page_idx = 0

        while page_idx < max_pages:
            url = self._with_query(
                search_url, {offset_param: str(offset), **fixed_params}
            )
            r = scraper.get(url, timeout=30)
            r.raise_for_status()
            html = r.text

            if total is None:
                try:
                    total = int(extract_total_results(html) or 0)
                except Exception:
                    total = None

            ddo = extract_phapp_ddo(html) or {}

            # Robustly locate jobs list inside DDO
            job_list = self._find_jobs_list(ddo)

            scraper.log(
                "list:page",
                page=page_idx,
                offset=offset,
                got=len(job_list),
                total=total or -1,
                url=url,
            )

            if not job_list:
                # If first page yields nothing, stop.
                break

            for j in job_list:
                job_id = str(
                    j.get("jobId") or j.get("jobID") or j.get("id") or ""
                ).strip()
                if not job_id or job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                detail_url = urljoin(
                    cfg.careers_home or search_url,
                    detail_path_template.format(jobId=job_id),
                )

                jobs.append(
                    {
                        "Posting ID": job_id,
                        "Detail URL": detail_url,
                        "Position Title": (
                            j.get("title") or j.get("jobTitle") or ""
                        ).strip(),
                        "Raw Location": (
                            j.get("location") or j.get("locations") or ""
                        ).strip(),
                        "_page": page_idx,
                        "_offset": offset,
                    }
                )

                if len(jobs) >= max_jobs:
                    break

            if len(jobs) >= max_jobs:
                break

            # Advance offset
            offset += page_size
            page_idx += 1

            # Stop if we know total and we've passed it
            if total is not None and total > 0 and offset >= total:
                break

            # Defensive: if page_size is wrong and the site repeats, stop if no new IDs were added
            # (We already dedupe by ID, so detect stagnation by checking last page yielded 0 new)
            # Simple version: if fewer than ~3 new jobs were added, assume end
            # (avoid overfetching if the site doesn't obey offset/page_size cleanly)
            # You can remove this if it causes premature stopping.
            # (Keep it for now; it prevents infinite loops.)
            # NOTE: we can’t easily count "new" without tracking, so rely on got==0 above.

        scraper.log("list:fetched", count=len(jobs))
        return jobs

    def normalize(
        self, cfg, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prefer Phenom vendor blob (phApp.ddo parsed from detail page) and fall back to JSON-LD/meta.
        """
        url = (raw_job.get("Detail URL") or "").strip()
        detail_url = (artifacts.get("_canonical_url") or url).strip()

        vb = artifacts.get("_vendor_blob") or {}
        jsonld = artifacts.get("_jsonld") or {}
        meta = artifacts.get("_meta") or {}

        def first_str(*vals: Any) -> str:
            for v in vals:
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return ""

        # Some Phenom blobs nest the actual job under a key; try a few common patterns:
        job = vb
        for k in ("job", "jobDetail", "data", "position", "posting"):
            if isinstance(job, dict) and isinstance(job.get(k), dict):
                job = job.get(k)

        posting_id = first_str(
            raw_job.get("Posting ID"),
            job.get("requisitionId"),
            job.get("requisitionID"),
            job.get("reqId"),
            job.get("jobId"),
            job.get("jobID"),
            job.get("id"),
            (jsonld.get("identifier") or {}).get("value")
            if isinstance(jsonld.get("identifier"), dict)
            else "",
            meta.get("meta.job-ats-req-id"),
        )

        title = first_str(
            raw_job.get("Position Title"),
            job.get("title"),
            job.get("jobTitle"),
            jsonld.get("title"),
            meta.get("meta.og:title"),
        )

        description = first_str(
            job.get("description"),
            job.get("jobDescription"),
            jsonld.get("description"),
        )

        post_date = first_str(
            job.get("datePosted"),
            job.get("postedDate"),
            job.get("postingDate"),
            jsonld.get("datePosted"),
        )

        raw_location = first_str(
            raw_job.get("Raw Location"),
            job.get("location"),
            meta.get("meta.gtm_tbcn_location"),
        )

        city = first_str(
            job.get("city"), jsonld.get("jobLocation.0.address.addressLocality")
        )
        state = first_str(
            job.get("state"), jsonld.get("jobLocation.0.address.addressRegion")
        )
        country = first_str(
            job.get("country"), jsonld.get("jobLocation.0.address.addressCountry")
        )
        postal = first_str(
            job.get("postalCode"), jsonld.get("jobLocation.0.address.postalCode")
        )

        employment_type = (
            jsonld.get("employmentType") or job.get("employmentType") or ""
        )
        if isinstance(employment_type, list):
            employment_type = employment_type[0] if employment_type else ""

        return {
            "Posting ID": posting_id,
            "Position Title": title,
            "Detail URL": detail_url,
            "Description": description,
            "Post Date": post_date,
            "Full Time Status": str(employment_type or "").strip(),
            "Raw Location": raw_location,
            "City": city,
            "State": state,
            "Country": country,
            "Postal Code": postal,
        }

    @staticmethod
    def _find_jobs_list(ddo: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Walk common phApp.ddo shapes and find a list of job dicts.
        Keeps this adapter resilient across Phenom tenants.
        """
        if not isinstance(ddo, dict):
            return []

        # Common: ddo["eagerLoadRefineSearch"]["data"]["jobs"]
        try:
            x = ddo.get("eagerLoadRefineSearch") or {}
            if isinstance(x, dict):
                data = x.get("data") or {}
                if isinstance(data, dict) and isinstance(data.get("jobs"), list):
                    return [j for j in data["jobs"] if isinstance(j, dict)]
        except Exception:
            pass

        # Another common: ddo["refineSearch"]["data"]["jobs"]
        try:
            x = ddo.get("refineSearch") or {}
            if isinstance(x, dict):
                data = x.get("data") or {}
                if isinstance(data, dict) and isinstance(data.get("jobs"), list):
                    return [j for j in data["jobs"] if isinstance(j, dict)]
        except Exception:
            pass

        # Fallback: DFS for first list of dicts containing "jobId" keys
        stack = [ddo]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                for v in cur.values():
                    if (
                        isinstance(v, list)
                        and v
                        and all(isinstance(i, dict) for i in v)
                    ):
                        # Heuristic: looks like jobs if any dict has jobId/title keys
                        if any(("jobId" in i or "jobID" in i or "id" in i) for i in v):
                            return v  # type: ignore[return-value]
                    elif isinstance(v, dict):
                        stack.append(v)
            elif isinstance(cur, list):
                for v in cur:
                    if isinstance(v, dict):
                        stack.append(v)
        return []

    @staticmethod
    def _with_query(url: str, params: Dict[str, str]) -> str:
        u = urlparse(url)
        q = parse_qs(u.query)
        for k, v in params.items():
            q[k] = [str(v)]
        new_q = urlencode({k: v[-1] for k, v in q.items() if v}, doseq=False)
        return u._replace(query=new_q).geturl()
