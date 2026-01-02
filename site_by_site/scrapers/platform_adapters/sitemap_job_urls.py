# scrapers/platform_adapters/sitemap_job_urls.py
from __future__ import annotations

import re

from typing import Any, Dict, List
from urllib.parse import urlparse

from utils.sitemap import parse_sitemap_index, parse_sitemap_xml


class SitemapJobUrlsAdapter:
    """
    Platform adapter:
      - Listing: sitemap or sitemap_index -> collect job URLs
      - Normalize: prefer JSON-LD (JobPosting) + fall back to raw listing fields
    """

    def probe(self, cfg) -> float:
        # High confidence if config explicitly uses sitemap discovery or provides sitemap URLs
        dtype = (
            getattr(cfg, "discovery_type", None)
            or getattr(cfg, "discovery", {}).get("type")
            if isinstance(getattr(cfg, "discovery", None), dict)
            else None
        )
        has_sm = bool(
            getattr(cfg, "sitemap_url", None) or getattr(cfg, "sitemap_index_url", None)
        )
        if dtype in ("sitemap", "sitemap_index"):
            return 1.0
        if has_sm:
            return 0.8
        return 0.0

    def list_jobs(self, scraper, cfg) -> List[Dict[str, Any]]:
        c = self._cfg(cfg)
        company_id = c["company_id"] or "unknown"

        # Determine job limit like your other scrapers do (testing vs infinite)
        if getattr(scraper, "testing", False):
            try:
                job_limit = int(getattr(scraper, "test_limit", 15)) or 15
            except Exception:
                job_limit = 15
        else:
            job_limit = float("inf")

        jobs: List[Dict[str, Any]] = []

        dtype = c["dtype"]
        if dtype == "sitemap_index":
            index_url = c["sitemap_index_url"]
            if not index_url:
                raise ValueError(
                    f"{company_id}: discovery.type=sitemap_index but missing sitemap_index_url"
                )

            scraper.log("sitemap_index:fetch", url=index_url)
            idx_resp = scraper.get(index_url)
            idx_resp.raise_for_status()

            sitemap_entries = parse_sitemap_index(idx_resp.content)
            sitemap_urls = [e.get("loc") for e in sitemap_entries if e.get("loc")]
            scraper.log("sitemap_index:parsed", count=len(sitemap_urls))

            needle = c["job_url_contains"] or "/job/"
            seen = set()
            for sm_url in sitemap_urls:
                if len(jobs) >= job_limit and job_limit != float("inf"):
                    break

                scraper.log("sitemap:fetch", url=sm_url)
                sm_resp = scraper.get(sm_url)
                sm_resp.raise_for_status()

                entries = parse_sitemap_xml(
                    sm_resp.content,
                    url_filter=lambda loc, _needle=needle: _needle in loc,
                )
                for ent in entries:
                    loc = ent.get("loc")
                    if not loc or loc in seen:
                        continue
                    seen.add(loc)
                    jobs.append(
                        {
                            "Detail URL": loc,
                            "Posting ID": self._posting_id_from_url(loc),
                        }
                    )
                    if len(jobs) >= job_limit and job_limit != float("inf"):
                        break

            scraper.log("list:sitemap", total=len(jobs))
            return jobs

        if dtype == "sitemap":
            sm_url = c["sitemap_url"]
            if not sm_url:
                raise ValueError(
                    f"{company_id}: discovery.type=sitemap but missing sitemap_url"
                )

            scraper.log("sitemap:fetch", url=sm_url)
            r = scraper.get(sm_url)
            r.raise_for_status()

            allowed_prefixes = c["allowed_prefixes"]
            needle = c["job_url_contains"] or "/job/"

            def _ok(loc: str) -> bool:
                if allowed_prefixes:
                    return any(loc.startswith(p) for p in allowed_prefixes)
                return needle in loc

            entries = parse_sitemap_xml(r.content, url_filter=lambda loc: _ok(loc))
            urls = [e.get("loc") for e in entries if e.get("loc")]

            # stable unique
            seen = set()
            uniq = []
            for u in urls:
                if u in seen:
                    continue
                seen.add(u)
                uniq.append(u)

            if job_limit != float("inf"):
                uniq = uniq[: int(job_limit)]

            for u in uniq:
                jobs.append(
                    {"Detail URL": u, "Posting ID": self._posting_id_from_url(u)}
                )

            scraper.log("list:sitemap", total_urls=len(urls), unique=len(jobs))
            return jobs

        raise ValueError(f"{company_id}: unknown discovery.type {dtype!r}")

    def normalize(
        self, cfg, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Return a raw record dict using your existing field names.
        canonicalize_record() will standardize types and enrich skills later. :contentReference[oaicite:7]{index=7}
        """
        url = (raw_job.get("Detail URL") or "").strip()
        detail_url = artifacts.get("_canonical_url") or url

        jsonld = artifacts.get("_jsonld") or {}
        meta = artifacts.get("_meta") or {}

        # Many of your sitemap-based scrapers lean on JSON-LD where present
        title = jsonld.get("title") or raw_job.get("Position Title") or ""
        desc = jsonld.get("description") or ""
        post_date = jsonld.get("datePosted") or raw_job.get("Post Date") or ""

        # Common JSON-LD paths (your code already uses these dotted keys in places, but artifacts may flatten)
        city = jsonld.get("jobLocation.0.address.addressLocality") or ""
        state = jsonld.get("jobLocation.0.address.addressRegion") or ""
        country = jsonld.get("jobLocation.0.address.addressCountry") or ""
        postal = jsonld.get("jobLocation.0.address.postalCode") or ""

        employment_type = (
            jsonld.get("employmentType") or raw_job.get("Full Time Status") or ""
        )

        # Posting ID: prefer embedded identifier.value when available, else listing-derived
        posting_id = (
            (
                (jsonld.get("identifier") or {}).get("value")
                if isinstance(jsonld.get("identifier"), dict)
                else ""
            )
            or raw_job.get("Posting ID")
            or self._posting_id_from_url(detail_url)
        )

        return {
            "Posting ID": posting_id,
            "Position Title": title,
            "Detail URL": detail_url,
            "Description": desc,
            "Post Date": post_date,
            "Full Time Status": employment_type,
            "City": city,
            "State": state,
            "Country": country,
            "Postal Code": postal,
            # helpful passthroughs (meta varies by company; safe to keep)
            "Raw Location": meta.get("meta.gtm_tbcn_location")
            or raw_job.get("Raw Location")
            or "",
            "Business Sector": meta.get("meta.gtm_tbcn_division")
            or raw_job.get("Business Sector")
            or "",
        }

    @staticmethod
    def _posting_id_from_url(url: str) -> str:
        """
        Extract a stable ID from common job URL shapes.

        Handles:
          - RTX/Thales Phenom:  .../job/01785759/Senior-Systems-Engineer
          - Thales:            .../job/R0210336/Mechanical-Architect
          - L3Harris:          .../job/rochester/lead-program-manager/4832/90178968272
        """
        try:
            path = urlparse(url).path
            parts = [p for p in path.split("/") if p]
            if not parts:
                return ""

            # Prefer the segment immediately after ".../job/"
            if "job" in parts:
                i = parts.index("job")
                if i + 1 < len(parts):
                    cand = parts[i + 1]
                    # RTX numeric IDs, Thales "R" + digits
                    if re.fullmatch(r"(?:R)?\d{5,}", cand):
                        return cand

            # Otherwise: pick the last numeric-like segment (works for L3Harris)
            for seg in reversed(parts):
                if re.fullmatch(r"(?:R)?\d{5,}", seg):
                    return seg

            # Last resort: last path segment
            return parts[-1]
        except Exception:
            return ""

    # -----------------------------
    # Config compatibility helpers
    # -----------------------------
    @staticmethod
    def _get(obj, key: str, default=None):
        """Support both dataclass (0.2) and dict-y (0.1) configs."""
        if obj is None:
            return default
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    def _cfg(self, cfg):
        """
        Return a unified view for 0.1 and 0.2 config shapes.
        0.1: cfg.entrypoints + cfg.discovery
        0.2: flattened fields on cfg
        """
        entrypoints = self._get(cfg, "entrypoints", {}) or {}
        discovery = self._get(cfg, "discovery", {}) or {}

        return {
            # identity
            "company_id": self._get(cfg, "company_id")
            or self._get(cfg, "identity", {}).get("company_id"),
            # discovery type
            "dtype": (self._get(cfg, "discovery_type") or discovery.get("type")),
            # sitemap urls
            "sitemap_url": self._get(cfg, "sitemap_url")
            or entrypoints.get("sitemap_url"),
            "sitemap_index_url": self._get(cfg, "sitemap_index_url")
            or entrypoints.get("sitemap_index_url"),
            # filters
            "job_url_contains": self._get(cfg, "job_url_contains")
            or discovery.get("job_url_contains"),
            "allowed_prefixes": self._get(cfg, "allowed_prefixes")
            or discovery.get("allowed_prefixes"),
        }
