# utils/detail_fetchers.py

from __future__ import annotations

import requests

from typing import Callable, Any, Optional, Dict
from bs4 import BeautifulSoup as BS
from utils.extractors import (
    extract_phapp_ddo,
    extract_jsonld,
    extract_meta,
    extract_datalayer,
    extract_canonical_link,
)

Getter = Callable[..., requests.Response]  # e.g., self.get
Logger = Callable[..., None]  # e.g., self.log(event, **kwargs)


def dig(d: Dict[str, Any], *path: str) -> Optional[Any]:
    """
    Safe nested dict access: _dig(d, "a","b","c") -> d["a"]["b"]["c"] or None.
    """
    cur: Any = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def strip_prefix_keys(d, prefix):
    if not isinstance(d, dict):
        return d
    plen = len(prefix)
    out = {}
    for k, v in d.items():
        if isinstance(k, str) and k.startswith(prefix):
            out[k[plen:]] = v
        else:
            out[k] = v
    return out


def fetch_detail_artifacts(
    get: Getter,
    log: Logger,
    detail_url: str,
    *,
    timeout: Optional[float] = 30.0,
    get_vendor_blob: bool = True,
    get_jsonld: bool = True,
    get_meta: bool = True,
    get_datalayer: bool = True,
) -> Dict[str, Any]:
    """
    Fetch a job detail page (HTML) and extract standardized artifacts.

    This function performs exactly one HTTP GET using the caller's session
    (typically JobScraper.get), then parses the HTML for common embedded
    data formats used across vendors:

      - phApp.ddo (Phenom/Workday-style)      -> _vendor_blob (preferred)
      - #smartApplyData (Northrop-style)      -> _vendor_blob (fallback)
      - JSON-LD JobPosting blocks             -> _jsonld
      - <meta> tags                           -> _meta
      - window.dataLayer.push({...})         -> _datalayer
      - <link rel="canonical" ...>           -> _canonical_url

    Args:
        get:    Callable that issues HTTP GET (e.g., self.get). Must return a
                requests.Response and be configured with retries/headers.
        log:    Callable for logging (e.g., self.log). Accepts (event, **kwargs).
        detail_url: Absolute URL of the job detail page.
        timeout: Optional per-request timeout (seconds).
        include_jsonld/meta/datalayer: Toggle parsing of those artifacts.

    Returns:
        Dict[str, Any]: A bundle with:
            "_vendor_blob": dict | None
                Raw vendor blob when present (e.g., phApp.ddo or smartApply JSON), else None.
            "_jsonld": Dict[str, Any] | None
                Flattened JSON-LD key-value mapping (e.g., "ld.@type", "ld.title", "ld.hiringOrganization.name"), or None if absent.
            "_meta": Dict[str, str] | None
                Flattened meta tag mapping with "meta." prefix (e.g., "meta.og:title"), or None if absent.
            "_datalayer": Dict[str, Any] | None
                Flattened dataLayer mapping with "datalayer." prefix, or None if absent.
            "_canonical_url": str | None
                Canonical URL extracted from the page, or None if not found.

    Notes:
        - No network calls occur in the extractors; they only parse strings.
        - We prefer phApp over smartApply for _vendor_blob if both exist.
        - Callers typically pass this dict straight through to the Canonicalizer.
    """
    # --- 1) Fetch once with the scraper's retrying session ---
    resp = get(detail_url, timeout=timeout)
    resp.raise_for_status()
    html_text = resp.text

    soup = BS(html_text, "lxml")

    bundle: Dict[str, Any] = {
        "detail_url": detail_url,
        "_html": html_text,
        "_vendor_blob": None,
    }

    # --- 2) Try to extract vendor-native blobs (preferred -> fallback) ---
    # 2a) phApp.ddo (Phenom)
    if get_vendor_blob:
        ph = None
        try:
            ph = extract_phapp_ddo(html_text)
        except ValueError:
            # Expected on non-Phenom pages; don't pollute logs with "error"
            log("detail:extract:phapp:miss", level="debug", url=detail_url)
        except Exception as e:
            # Unexpected parse issues
            log("detail:extract:phapp:error", url=detail_url, error=str(e))

        if isinstance(ph, dict) and ph:
            # Common paths:
            #   ph.jobDetail.data.job
            #   ph.jobDetail.job
            #   ph.data.job
            job = (
                dig(ph, "jobDetail", "data", "job")
                or dig(ph, "jobDetail", "job")
                or dig(ph, "data", "job")
                or ph  # last resort: whole blob
            )
            if isinstance(job, dict) and job:
                bundle["_vendor_blob"] = job
                log("detail:extract:phapp:ok", url=detail_url)

    # --- 3) Optional secondary artifacts (schema/meta/analytics/canonical) ---
    if get_jsonld:
        try:
            bundle["_jsonld"] = strip_prefix_keys(extract_jsonld(soup), "ld.")
        except Exception as e:
            log("detail:extract:jsonld:error", url=detail_url, error=str(e))

    if get_meta:
        try:
            bundle["_meta"] = strip_prefix_keys(extract_meta(soup), "meta.")
        except Exception as e:
            log("detail:extract:meta:error", url=detail_url, error=str(e))

    if get_datalayer:
        try:
            bundle["_datalayer"] = extract_datalayer(html_text)
        except Exception as e:
            log("detail:extract:datalayer:error", url=detail_url, error=str(e))

    try:
        bundle["_canonical_url"] = extract_canonical_link(html_text)
    except Exception as e:
        log("detail:extract:canonical:error", url=detail_url, error=str(e))

    return bundle
