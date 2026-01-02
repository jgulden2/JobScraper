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
    Safe nested dict access: dig(d, "a","b","c") -> d["a"]["b"]["c"] or None.
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


def _parse_detail_artifacts_from_html(
    html_text: str,
    log: Logger,
    detail_url: str,
    *,
    get_vendor_blob: bool = True,
    get_jsonld: bool = True,
    get_meta: bool = True,
    get_datalayer: bool = True,
) -> Dict[str, Any]:
    """
    Parse standardized artifacts from already-fetched HTML (e.g., Selenium page_source).

    Returns the same bundle shape as fetch_detail_artifacts().
    """
    soup = BS(html_text, "lxml")

    bundle: Dict[str, Any] = {
        "detail_url": detail_url,
        "_html": html_text,
        "_vendor_blob": None,
    }

    # --- 1) Vendor-native blob (phApp.ddo) ---
    if get_vendor_blob:
        ph = None
        try:
            ph = extract_phapp_ddo(html_text)
        except ValueError:
            # Expected on non-Phenom pages
            log("detail:extract:phapp:miss", level="debug", url=detail_url)
        except Exception as e:
            log("detail:extract:phapp:error", url=detail_url, error=str(e))

        if isinstance(ph, dict) and ph:
            job = (
                dig(ph, "jobDetail", "data", "job")
                or dig(ph, "jobDetail", "job")
                or dig(ph, "data", "job")
                or ph
            )
            if isinstance(job, dict) and job:
                bundle["_vendor_blob"] = job
                log("detail:extract:phapp:ok", url=detail_url)

    # --- 2) Secondary artifacts (JSON-LD / meta / datalayer / canonical) ---
    if get_jsonld:
        try:
            # Your extractor returns flattened keys with "ld." prefix in many cases;
            # we strip it here to keep downstream consistent.
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

    Exactly one HTTP GET (via provided `get`), then parse HTML into:
      - _vendor_blob (phApp.ddo when present)
      - _jsonld
      - _meta
      - _datalayer
      - _canonical_url
    """
    resp = get(detail_url, timeout=timeout)
    resp.raise_for_status()
    html_text = resp.text

    return _parse_detail_artifacts_from_html(
        html_text,
        log,
        detail_url,
        get_vendor_blob=get_vendor_blob,
        get_jsonld=get_jsonld,
        get_meta=get_meta,
        get_datalayer=get_datalayer,
    )
