# scrapers/platform_adapters/phenom_sitemap.py
from __future__ import annotations

from typing import Any, Dict

from scrapers.platform_adapters.sitemap_job_urls import SitemapJobUrlsAdapter


class PhenomSitemapAdapter(SitemapJobUrlsAdapter):
    """
    Listing: reuse sitemap/sitemap-index URL discovery.
    Normalize: prefer Phenom vendor blob (phApp.ddo extracted into artifacts["_vendor_blob"]).
    """

    def normalize(
        self, cfg: Any, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]:
        url = (raw_job.get("Detail URL") or raw_job.get("detail_url") or "").strip()
        canonical_url = (artifacts.get("_canonical_url") or url).strip()

        vb = artifacts.get("_vendor_blob") or {}
        jsonld = artifacts.get("_jsonld") or {}
        meta = artifacts.get("_meta") or {}

        def first(*vals: Any) -> str:
            for v in vals:
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return ""

        def first_any(*vals: Any) -> Any:
            for v in vals:
                if v is None:
                    continue
                if isinstance(v, str) and not v.strip():
                    continue
                return v
            return None

        # Posting / requisition id (Phenom blobs vary by tenant)
        posting_id = first(
            raw_job.get("Posting ID"),
            raw_job.get("posting_id"),
            vb.get("requisitionId"),
            vb.get("requisitionID"),
            vb.get("reqId"),
            vb.get("jobId"),
            vb.get("jobID"),
            vb.get("id"),
            jsonld.get("identifier.value"),
            jsonld.get("identifier"),
            meta.get("meta.job-ats-req-id"),
        )

        title = first(
            raw_job.get("Position Title"),
            raw_job.get("title"),
            vb.get("title"),
            vb.get("jobTitle"),
            jsonld.get("title"),
            meta.get("meta.og:title"),
        )

        description = first(
            vb.get("description"),
            vb.get("jobDescription"),
            jsonld.get("description"),
        )

        post_date = first(
            vb.get("datePosted"),
            vb.get("postedDate"),
            vb.get("postingDate"),
            jsonld.get("datePosted"),
        )

        # Location normalization: Phenom sometimes has a single string; sometimes components
        raw_location = first(
            vb.get("location"),
            vb.get("locations"),
            meta.get("meta.gtm_tbcn_location"),
            raw_job.get("Raw Location"),
        )

        city = first(
            vb.get("city"),
            jsonld.get("jobLocation.0.address.addressLocality"),
        )
        state = first(
            vb.get("state"),
            jsonld.get("jobLocation.0.address.addressRegion"),
        )
        country = first(
            vb.get("country"),
            jsonld.get("jobLocation.0.address.addressCountry"),
        )
        postal_code = first(
            vb.get("postalCode"),
            jsonld.get("jobLocation.0.address.postalCode"),
        )

        employment_type = first_any(
            vb.get("employmentType"),
            jsonld.get("employmentType"),
        )
        if isinstance(employment_type, list):
            employment_type = employment_type[0] if employment_type else ""

        record: Dict[str, Any] = {
            "Posting ID": posting_id,
            "Position Title": title,
            "Detail URL": canonical_url or url,
            "Description": description,
            "Post Date": post_date,
            "Raw Location": raw_location,
            "Country": country,
            "State": state,
            "City": city,
            "Postal Code": postal_code,
            "Full Time Status": str(employment_type or "").strip(),
        }

        # Keep any useful passthrough from listing
        for k in ("Req Number", "Clearance", "_page", "_rank"):
            if k in raw_job and k not in record:
                record[k] = raw_job[k]

        return record
