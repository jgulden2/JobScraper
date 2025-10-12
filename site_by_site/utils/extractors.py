from __future__ import annotations

import json
import html
import re

from bs4 import BeautifulSoup as BS
from typing import Dict, Any, Optional, List, Tuple


def flatten(
    obj: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Flatten nested dicts/lists into dotted keys.

    Args:
        obj: Object to flatten (dict, list, or scalar).
        prefix: Key path prefix to apply to nested values.
        out: Destination mapping (created if None).

    Returns:
        The `out` mapping with flattened keys and scalar values.

    Raises:
        None
    """
    if out is None:
        out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            flatten(v, f"{prefix}{k}." if prefix else f"{k}.", out)
    elif isinstance(obj, list):
        if all(isinstance(x, (str, int, float, bool)) or x is None for x in obj):
            out[prefix[:-1]] = "; ".join("" if x is None else str(x) for x in obj)
        else:
            for i, v in enumerate(obj):
                flatten(v, f"{prefix}{i}.", out)
    else:
        out[prefix[:-1]] = "" if obj is None else obj
    return out


def extract_jsonld(soup: BS) -> Dict[str, Any]:
    """
    Extract and flatten JSON-LD script blocks into a dotted-key mapping.

    Args:
        soup: Parsed BeautifulSoup document.

    Returns:
        Dictionary containing flattened JSON-LD data with prefixes 'ld' or 'ld[i]'.

    Raises:
        None (malformed/empty blocks are ignored).
    """
    out: Dict[str, Any] = {}
    blocks = soup.find_all("script", attrs={"type": "application/ld+json"})
    for b in blocks:
        try:
            data = json.loads(b.string or b.get_text() or "")
        except Exception:
            continue
        if isinstance(data, list):
            for i, item in enumerate(data):
                flat = flatten(item)
                for k, v in flat.items():
                    out[f"ld[{i}].{k}"] = v
        else:
            flat = flatten(data)
            for k, v in flat.items():
                out[f"ld.{k}"] = v
    return out


def extract_meta(soup: BS) -> Dict[str, str]:
    """
    Extract basic meta values and the first h1 when available.

    Args:
        soup: Parsed BeautifulSoup document.

    Returns:
        Dictionary mapping meta names/properties to content (prefixed with 'meta.'),
        plus 'h1' when present.

    Raises:
        None
    """
    out: Dict[str, str] = {}
    for m in soup.find_all("meta"):
        name = m.get("name") or m.get("property")
        if not name:
            continue
        content = m.get("content")
        if content is None:
            continue
        key = f"meta.{name}"
        if key not in out:
            out[key] = content
    h1 = soup.find("h1")
    if h1 and "text" not in out:
        out["h1"] = h1.get_text(strip=True)
    return out


def extract_datalayer(html: str) -> Dict[str, str]:
    """
    Parse `window.dataLayer.push({...})` calls into a flat mapping.

    Args:
        html: Raw HTML of a job detail page.

    Returns:
        Dictionary mapping 'datalayer.<key>' to extracted values.

    Raises:
        None
    """
    out: Dict[str, str] = {}
    for m in re.finditer(
        r"window\.dataLayer\.push\(\{([^)]*?)\}\)", html, re.I | re.M | re.S
    ):
        body = m.group(1)
        for k, v in re.findall(r"['\"]([^'\"]+)['\"]\s*:\s*['\"]([^'\"]*)['\"]", body):
            out[f"datalayer.{k}"] = v
    return out


def extract_canonical_link(html_text: str) -> Optional[str]:
    soup = BS(html_text, "html.parser")
    link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    return link.get("href") if link and link.has_attr("href") else None


def extract_phapp_ddo(html: str) -> Dict[str, Any]:
    """
    Parse the page HTML and return the embedded phApp.ddo JSON object.

    Args:
        html: Full HTML of a listing or detail page.

    Returns:
        The decoded phApp.ddo JSON object as a dictionary.

    Raises:
        ValueError: If the phApp.ddo object is not found in the HTML.
        json.JSONDecodeError: If the embedded JSON cannot be decoded.
    """
    pattern = re.compile(r"phApp\.ddo\s*=\s*(\{.*?\});", re.DOTALL)
    match = pattern.search(html)
    if not match:
        raise ValueError("phApp.ddo object not found in HTML")
    phapp_ddo_str = match.group(1)
    data: Dict[str, Any] = json.loads(phapp_ddo_str)
    return data


def extract_total_results(phapp_data: Dict[str, Any]) -> int:
    """
    Extract the total job count from the phApp.ddo structure.

    Args:
        phapp_data: The parsed JSON data from phApp.ddo.

    Returns:
        Integer total number of job results.
    """
    return int(phapp_data.get("eagerLoadRefineSearch", {}).get("totalHits", 0))


def extract_smartapply(html_text: str) -> Dict[str, Any]:
    """
    Parse embedded JSON from the HTML detail page (when API detail is unavailable).

    Args:
        html_text: Raw HTML of a job detail page.

    Returns:
        Flattened mapping of embedded JSON fields. If a `positions` list is
        present, the first element is also flattened under `positions.0.*`.

    Raises:
        json.JSONDecodeError: If the embedded JSON block cannot be decoded.
        ValueError: If the expected container is present but contains invalid JSON.
    """
    soup = BS(html_text, "html.parser")
    code = soup.select_one("#smartApplyData")
    if not code:
        return {}
    raw = html.unescape(code.text)
    data = json.loads(raw)
    flat = flatten(data)
    if isinstance(data.get("positions"), list) and data["positions"]:
        pos = flatten(data["positions"][0])
        for k, v in pos.items():
            flat[f"positions.0.{k}"] = v
    return flat


def text(node: Any) -> str:
    """
    Extract plain text from HTML, collapsing whitespace.

    Args:
        node: HTML node or markup snippet.

    Returns:
        Plain text content with spaces normalized.

    Raises:
        None
    """
    return BS(str(node), "html.parser").get_text(" ", strip=True)


def collect_until_next_b(start_b: Any) -> str:
    """
    Collect text and list items that follow a <b> label until the next <b>.

    Handles sequences like:
        <b>Location:</b> USA AL Huntsville<br>...
        <b>Job Duties and Responsibilities</b><ul><li>...</li></ul>

    Args:
        start_b: A BeautifulSoup node pointing to a <b> element.

    Returns:
        A single string with line breaks preserved for list items.

    Raises:
        None
    """
    parts: List[str] = []
    list_items: Optional[List[str]] = None
    for sib in start_b.next_siblings:
        # Stop when we hit the next <b> label
        if getattr(sib, "name", None) == "b":
            break
        # Collect lists as arrays
        if getattr(sib, "name", None) == "ul":
            items: List[str] = []
            for li in sib.find_all("li"):
                items.append(text(li))
            list_items = (list_items or []) + items
            continue
        # Everything else as text (handles <br>, <p>, strings, etc.)
        if isinstance(sib, str):
            parts.append(sib)
        else:
            parts.append(text(sib))
    # Normalize text
    value = " ".join(p.strip() for p in parts if p and p.strip())
    val_raw = list_items if list_items is not None else value
    if isinstance(val_raw, list):
        val = "\n".join(x.strip() for x in val_raw if isinstance(x, str))
    else:
        val = (val_raw or "").strip()
    return val


def extract_insets(soup: BS) -> Dict[str, str]:
    """
    Extract inset values from the detail page (location, etc.).

    Args:
        soup: Parsed BeautifulSoup document for a job detail page.

    Returns:
        Mapping from 'inset.<Label>' to string values.

    Raises:
        None
    """
    out: Dict[str, str] = {}
    for dl in soup.select(".career-search-result__insets dl"):
        dt = dl.find("dt")
        dts = dt.get_text(" ", strip=True) if dt else ""
        # Some have icon-only <dt> (e.g., location). Use dd when dt is empty.
        dds = "; ".join(dd.get_text(" ", strip=True) for dd in dl.find_all("dd"))
        key = dts if dts else "Location"
        if key and dds:
            out[f"inset.{key}"] = dds
    return out


def extract_bold_block(soup: BS) -> Dict[str, str]:
    """
    Parse labeled blocks under '.career-detail-description'.

    Args:
        soup: Parsed BeautifulSoup document.

    Returns:
        Flat mapping of labels to strings. For sections that are lists,
        items are joined with '; '. Adds 'Page Title' if a title is found.

    Raises:
        None
    """
    data: Dict[str, Any] = {}
    container = soup.select_one(".career-detail-description") or soup
    for b in container.find_all("b"):
        label = text(b).rstrip(":").strip()
        if not label:
            continue
        val = collect_until_next_b(b)
        # If multiple same labels appear, keep the richest (list beats text, longer text beats shorter)
        if label in data:
            cur = data[label]

            def score(v: Any) -> Tuple[int, int]:
                return (1, len(v)) if isinstance(v, list) else (0, len(v or ""))

            if score(val) > score(cur):
                data[label] = val
        else:
            data[label] = val
    # Flatten lists into strings (or keep lists if you prefer arrays)
    for k, v in list(data.items()):
        if isinstance(v, list):
            data[k] = "; ".join(v)
    # Add the H1 title as a convenience if present
    h1 = soup.select_one(".career-detail-title, h1")
    if h1 and "Page Title" not in data:
        data["Page Title"] = text(h1)
    # Type-narrow to str values
    return {k: str(v) for k, v in data.items()}


def extract_tagged_value(soup: BS, label: str) -> str:
    """
    Read a value that appears immediately after a bolded label.

    Example HTML:
        <b>Clearance Level:</b> Secret

    Args:
        soup: Parsed BeautifulSoup document.
        label: The exact label text to locate (case-sensitive substring).

    Returns:
        The stripped value following the label, or an empty string if not found.

    Raises:
        None
    """
    tag = soup.find("b", string=lambda text: text and label in text)
    if tag and tag.next_sibling:
        return str(tag.next_sibling).strip(": ").strip()
    return ""


def extract_job_category(soup: BS, json_ld_industry: str) -> str:
    """
    Determine the job category, favoring on-page tags over JSON-LD.

    Args:
        soup: Parsed BeautifulSoup document.
        json_ld_industry: The 'industry' field from JSON-LD (if present).

    Returns:
        A job category string or an empty string.

    Raises:
        None
    """
    value = extract_tagged_value(soup, "Career Area")
    if value:
        return value.strip()
    if json_ld_industry:
        return re.sub(r"^\d+:\s*", "", json_ld_industry).strip()
    return ""


def extract_section(text: str, start_markers: List[str], end_markers: List[str]) -> str:
    """
    Extract a section of text bounded by specific start and end markers.

    Args:
        text: The full text from which to extract.
        start_markers: List of possible starting phrases.
        end_markers: List of possible ending phrases.

    Returns:
        The text content between markers, stripped of colons and whitespace,
        or an empty string if no section is found.
    """
    for start_marker in start_markers:
        start_idx = text.find(start_marker)
        if start_idx != -1:
            for end_marker in end_markers:
                end_idx = text.find(end_marker, start_idx)
                if end_idx != -1:
                    section = text[start_idx + len(start_marker) : end_idx].strip()
                    return section.lstrip(": ").strip()
            # If no end_marker found, grab till end
            section = text[start_idx + len(start_marker) :].strip()
            return section.lstrip(": ").strip()
    return ""
