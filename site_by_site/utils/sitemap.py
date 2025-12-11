# utils/sitemap.py

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Callable, Dict, List, Optional, Union


def _normalize_sitemap_text(xml_text: Union[str, bytes]) -> str:
    """
    Normalize sitemap content to a clean Unicode string that ElementTree
    can parse, handling UTF-8 BOM and the common 'ï»¿' artifact from
    mis-decoding UTF-8 as Latin-1.
    """
    if isinstance(xml_text, bytes):
        # Decode as UTF-8 with BOM support
        text = xml_text.decode("utf-8-sig", errors="replace")
    else:
        text = xml_text

    # Case 1: proper BOM (U+FEFF) at start
    if text.startswith("\ufeff"):
        return text.lstrip("\ufeff")

    # Case 2: BOM bytes decoded as Latin-1 -> 'ï»¿'
    if text.startswith("ï»¿"):
        # Re-encode as Latin-1 to get the original bytes back,
        # then decode as UTF-8 with BOM stripping.
        return text.encode("latin-1").decode("utf-8-sig", errors="replace")

    return text


def parse_sitemap_xml(
    xml_text: Union[str, bytes],
    url_filter: Optional[Callable[[str], bool]] = None,
) -> List[Dict[str, str]]:
    """
    Parse a standard XML sitemap (<urlset>) and return a list of dicts:

        {"loc": "<url>", "lastmod": "<iso8601 or ''>"}
    """
    text = _normalize_sitemap_text(xml_text)
    root = ET.fromstring(text)

    # Handle namespace if present
    if root.tag.startswith("{"):
        uri = root.tag.split("}")[0].strip("{")
        ns = {"sm": uri}
        url_xpath = ".//sm:url"
        loc_tag = "sm:loc"
        lastmod_tag = "sm:lastmod"
    else:
        ns = {}
        url_xpath = ".//url"
        loc_tag = "loc"
        lastmod_tag = "lastmod"

    results: List[Dict[str, str]] = []

    for url_el in root.findall(url_xpath, ns):
        loc_el = url_el.find(loc_tag, ns)
        if loc_el is None or not loc_el.text:
            continue
        loc = loc_el.text.strip()

        if url_filter is not None and not url_filter(loc):
            continue

        lastmod_el = url_el.find(lastmod_tag, ns)
        lastmod = (
            lastmod_el.text.strip()
            if lastmod_el is not None and lastmod_el.text
            else ""
        )

        results.append({"loc": loc, "lastmod": lastmod})

    return results


def parse_sitemap_index(
    xml_text: Union[str, bytes],
    url_filter: Optional[Callable[[str], bool]] = None,
) -> List[Dict[str, str]]:
    """
    Parse a sitemap *index* (<sitemapindex>) and return a list of dicts:

        {"loc": "<sitemap_url>", "lastmod": "<iso8601 or ''>"}

    Args:
        xml_text: Raw XML bytes/string of the sitemap index.
        url_filter: Optional predicate to filter sitemap URLs by loc.

    Returns:
        List of {"loc": ..., "lastmod": ...} dictionaries.
    """
    text = _normalize_sitemap_text(xml_text)
    root = ET.fromstring(text)

    # Handle namespace if present
    if root.tag.startswith("{"):
        uri = root.tag.split("}")[0].strip("{")
        ns = {"sm": uri}
        sitemap_xpath = ".//sm:sitemap"
        loc_tag = "sm:loc"
        lastmod_tag = "sm:lastmod"
    else:
        ns = {}
        sitemap_xpath = ".//sitemap"
        loc_tag = "loc"
        lastmod_tag = "lastmod"

    results: List[Dict[str, str]] = []

    for sm_el in root.findall(sitemap_xpath, ns):
        loc_el = sm_el.find(loc_tag, ns)
        if loc_el is None or not loc_el.text:
            continue
        loc = loc_el.text.strip()

        if url_filter is not None and not url_filter(loc):
            continue

        lastmod_el = sm_el.find(lastmod_tag, ns)
        lastmod = (
            lastmod_el.text.strip()
            if lastmod_el is not None and lastmod_el.text
            else ""
        )

        results.append({"loc": loc, "lastmod": lastmod})

    return results
