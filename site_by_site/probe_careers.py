"""
probe_careers.py

A probing utility to:
  1) Discover sitemaps for a company website + careers site (and robots.txt)
  2) If sitemaps (or sitemap indexes) exist, collect job-like URLs per sitemap
  3) Visit one job URL and infer how job data is delivered:
       - JSON-LD JobPosting
       - Embedded JS global state (phApp.ddo, __NEXT_DATA__, etc.)
       - Server-rendered HTML (title/description visible in DOM)
       - XHR/fetch endpoints that contain job tokens (via Playwright)

Usage (CLI):
  python probe_careers.py --input companies.json --json-out results.json

companies.json format:
{
  "Acme Corp": {
    "website": "https://www.acme.com",
    "careers": "https://careers.acme.com"
  }
}
"""

from __future__ import annotations

import argparse
import json
import re
import nltk
import requests
import logging

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse, unquote
from nltk.corpus import stopwords

import xml.etree.ElementTree as ET

# Playwright is used ONLY for the "detail probing" stage to inspect XHR/fetch.
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Common sitemap locations to try under each base (website + careers).
SITEMAP_CANDIDATE_PATHS = (
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/sitemap/sitemap.xml",
    "/sitemaps/sitemap.xml",
    "/sitemap1.xml",
    "/sitemap/sitemap_index.xml",
    "/sitemaps/sitemap_index.xml",
)

# Patterns used to decide "job-ish" URLs.
JOB_URL_SUBSTRINGS = (
    "/job/",  # common (Phenom, many)
    "/jobs/",  # common
    "/careers/job/",
    "/career/job/",
    "/vacancy/",  # some EU sites
    "/vacancies/",
    "/position/",  # some vendors
    "/positions/",
    "/opportunity/",
    "/opportunities/",
)

# Additional regex patterns (fallback)
JOB_URL_REGEXES = (
    re.compile(r"/job/\w+", re.IGNORECASE),
    re.compile(r"/jobs/\w+", re.IGNORECASE),
    re.compile(r"/career[s]?/job/\w+", re.IGNORECASE),
)

# If we can't enumerate job URLs from sitemaps, probe the search page's DOM + XHR/fetch
# for "job posting payload" keywords.
JOB_DATA_KEYWORDS = (
    # General job posting terms
    "job description",
    "description",
    "responsibilities",
    "qualifications",
    "requirements",
    "requisition",
    "requisitionid",
    "jobid",
    "postingdate",
    "apply",
    "employmenttype",
    "salary",
    "compensation",
    "location",
    # JSON-LD / schema.org signals
    "jobposting",
    "@type",
    "hiringorganization",
    # Oracle HCM/Fusion common fields
    "requisitionlist",
    "totaljobscount",
    "externaldescriptionstr",
    "externalqualificationsstr",
    "externalresponsibilitiesstr",
)

JOB_DATA_PATTERNS = [re.compile(re.escape(k), re.IGNORECASE) for k in JOB_DATA_KEYWORDS]


@dataclass
class SitemapHit:
    base: str  # normalized base we tried (scheme+host)
    url: str  # sitemap url
    via: str  # "robots" | "direct"
    evidence: str  # robots line or path tried


@dataclass
class SitemapDiscovery:
    tried: List[str]
    found: List[SitemapHit]


@dataclass
class SitemapMatchSet:
    """Per-sitemap job URL match details (possibly capped)."""

    sitemap_url: str
    is_index: bool
    child_sitemaps: List[str]
    total_urls: int

    matched_job_urls: int
    unique_job_urls: int

    # Store matched URLs (capped for safety) + always include a small sample.
    matched_job_urls_capped: List[str]
    sample_job_urls: List[str]

    # For sitemapindex: per child sitemap counts (and samples)
    per_child: List[Dict[str, Any]]


@dataclass
class DetailSignal:
    kind: str  # e.g. json_ld, phapp_ddo, next_data, html, xhr
    evidence: Dict[str, Any]


@dataclass
class DetailProbeResult:
    job_url: str
    inferred_detail_source: str
    tokens: Dict[str, Any]
    signals: List[DetailSignal]
    xhr_hits: List[Dict[str, Any]]

    # Optional: snippet to highlight from DOM/embedded payload (preferred over XHR when present)
    dom_excerpt: str
    highlight_source: str  # "DOM" | "XHR"


@dataclass
class SearchProbeResult:
    search_url: str
    inferred_search_source: (
        str  # ORACLE_HCM_API | GENERIC_XHR_JSON | DOM_ONLY | UNKNOWN
    )
    dom_keyword_hits: List[str]
    xhr_keyword_hits: List[Dict[str, Any]]  # url/status/content_type/matched/snippet
    highlight_source: str  # "DOM" | "XHR"
    highlight_excerpt: str


@dataclass
class ProbeResult:
    company: str
    website: str
    careers: str
    sitemap: SitemapDiscovery
    sitemap_matches: List[SitemapMatchSet]
    chosen: Optional[Dict[str, Any]]  # which sitemap was chosen for detail probe + why
    detail: Optional[DetailProbeResult]
    search: str
    search_probe: Optional[SearchProbeResult]


LOG_FORMAT = "[%(levelname)s] %(message)s"


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT,
    )


ANSI_BOLD_RED = "\x1b[1;31m"
ANSI_RESET = "\x1b[0m"


def _supports_color() -> bool:
    # conservative: only colorize if stdout looks like a terminal
    try:
        import sys

        return sys.stdout.isatty()
    except Exception:
        return False


def _excerpt_around_matches(
    body: str,
    patterns: List[re.Pattern],
    *,
    context: int = 90,
    max_len: int = 700,
    color: bool = False,
) -> str:
    """Return a compact excerpt centered around the first match of any pattern.
    If color=True, ANSI-highlight all matches in the excerpt.
    """
    if not body:
        return ""

    first: Optional[re.Match] = None
    for pat in patterns:
        m = pat.search(body)
        if m:
            first = m
            break

    # Fallback: just truncate from start
    if not first:
        excerpt = body[:max_len]
        return excerpt

    start = max(0, first.start() - context)
    end = min(len(body), first.end() + context)
    excerpt = body[start:end]

    # Add ellipses if we didn't start at beginning / end at end
    if start > 0:
        excerpt = "…" + excerpt
    if end < len(body):
        excerpt = excerpt + "…"

    # Normalize whitespace a bit for CLI readability
    excerpt = re.sub(r"[ \t]+", " ", excerpt)
    excerpt = re.sub(r"\n{3,}", "\n\n", excerpt)

    if len(excerpt) > max_len:
        excerpt = excerpt[:max_len] + "…"

    if not color:
        return excerpt

    # Highlight all matches inside excerpt
    def repl(m: re.Match) -> str:
        return f"{ANSI_BOLD_RED}{m.group(0)}{ANSI_RESET}"

    for pat in patterns:
        excerpt = pat.sub(repl, excerpt)

    return excerpt


def _highlight_only(text: str, patterns: List[re.Pattern], *, color: bool) -> str:
    """Highlight matches in an existing snippet WITHOUT re-slicing it."""
    if not text or not color or not patterns:
        return text

    def repl(m: re.Match) -> str:
        return f"{ANSI_BOLD_RED}{m.group(0)}{ANSI_RESET}"

    out = text
    for pat in patterns:
        out = pat.sub(repl, out)
    return out


_STOPWORDS: Optional[Set[str]] = None


def _get_stopwords() -> Set[str]:
    """Lazy-load English stopwords. If NLTK data isn't present, download once."""
    global _STOPWORDS
    if _STOPWORDS is not None:
        return _STOPWORDS
    try:
        _STOPWORDS = set(stopwords.words("english"))
    except LookupError:
        nltk.download("stopwords", quiet=True)
        _STOPWORDS = set(stopwords.words("english"))
    return _STOPWORDS


def _norm_base(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_UA, "Accept": "*/*"})
    return s


def _get_text(sess: requests.Session, url: str, timeout: int = 20) -> Optional[str]:
    try:
        r = sess.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code >= 400:
            return None
        return r.text
    except Exception:
        return None


def _head_ok(sess: requests.Session, url: str, timeout: int = 15) -> bool:
    try:
        r = sess.head(url, timeout=timeout, allow_redirects=True)
        # Some servers don't like HEAD; treat 405/403 as "maybe" and fall back to GET
        if r.status_code in (405, 403):
            rt = sess.get(url, timeout=timeout, allow_redirects=True)
            return rt.status_code < 400
        return r.status_code < 400
    except Exception:
        return False


def _robots_sitemaps(robots_text: str) -> List[str]:
    out: List[str] = []
    for line in (robots_text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("sitemap:"):
            sm = line.split(":", 1)[1].strip()
            if sm:
                out.append(sm)
    # stable unique
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def discover_sitemaps(website: str, careers: str) -> SitemapDiscovery:
    """
    Probe BOTH bases (website + careers). Do not short-circuit after the first hit.

    Strategy:
      - Try robots.txt for each base, collect all Sitemap: lines that are reachable
      - Try common sitemap paths under each base
    """
    log = logging.getLogger("probe")

    log.info("Discovering sitemaps")

    sess = _requests_session()
    bases = [b for b in [_norm_base(website), _norm_base(careers)] if b]
    log.debug("Bases to probe: %s", bases)
    tried: List[str] = []
    found: List[SitemapHit] = []

    # 1) robots.txt discovery
    for base in bases:
        robots_url = urljoin(base + "/", "robots.txt")
        log.debug("Fetching robots.txt: %s", robots_url)

        tried.append(robots_url)
        txt = _get_text(sess, robots_url)
        if not txt:
            continue
        sitemaps = _robots_sitemaps(txt)
        for sm in sitemaps:
            tried.append(sm)
            if _head_ok(sess, sm):
                found.append(
                    SitemapHit(base=base, url=sm, via="robots", evidence=robots_url)
                )

    # 2) direct common paths (still try even if robots had hits)
    for base in bases:
        for path in SITEMAP_CANDIDATE_PATHS:
            sm = urljoin(base + "/", path.lstrip("/"))
            tried.append(sm)
            if _head_ok(sess, sm):
                found.append(SitemapHit(base=base, url=sm, via="direct", evidence=path))

    # de-dupe by sitemap URL stable
    seen: Set[str] = set()
    uniq: List[SitemapHit] = []
    for hit in found:
        if hit.url in seen:
            continue
        seen.add(hit.url)
        uniq.append(hit)
        log.info("Found sitemap via %s: %s", hit.via, hit.url)
    if not uniq:
        log.warning("No sitemaps found for provided bases")

    return SitemapDiscovery(tried=tried, found=uniq)


def _parse_xml_root(xml_bytes: bytes) -> Tuple[ET.Element, Dict[str, str], str, str]:
    """Returns (root, ns, url_xpath, loc_xpath) handling namespace."""
    root = ET.fromstring(xml_bytes)
    ns: Dict[str, str] = {}
    if root.tag.startswith("{"):
        uri = root.tag.split("}")[0].strip("{")
        ns["sm"] = uri
        url_xpath = ".//sm:url"
        loc_xpath = ".//sm:loc"
    else:
        url_xpath = ".//url"
        loc_xpath = ".//loc"
    return root, ns, url_xpath, loc_xpath


def _looks_like_sitemap_index(root: ET.Element) -> bool:
    return root.tag.lower().endswith("sitemapindex")


def _collect_locs(xml_bytes: bytes) -> List[str]:
    root, ns, _, loc_xpath = _parse_xml_root(xml_bytes)
    locs: List[str] = []
    for loc in root.findall(loc_xpath, ns):
        if loc is None or not loc.text:
            continue
        u = loc.text.strip()
        if u:
            locs.append(u)
    return locs


def _is_job_url(u: str) -> bool:
    if not u:
        return False
    lu = u.lower()
    if any(needle in lu for needle in JOB_URL_SUBSTRINGS):
        return True
    return any(rx.search(u) for rx in JOB_URL_REGEXES)


def collect_job_urls_from_sitemap(
    sitemap_url: str,
    *,
    max_child_sitemaps: int = 200,
    max_urls_total: int = 500_000,
    max_store_job_urls: int = 5_000,
    timeout: int = 30,
) -> SitemapMatchSet:
    """
    Fetch sitemap_url, handle sitemapindex recursively, and collect job-like URLs.

    Returns counts AND (capped) matched URLs so you can inspect what was found.
    Also returns per-child sitemap stats when this is an index.
    """
    log = logging.getLogger("probe")
    log.info("Parsing sitemap: %s", sitemap_url)

    sess = _requests_session()

    def fetch_bytes(url: str) -> bytes:
        r = sess.get(url, timeout=timeout)
        r.raise_for_status()
        return r.content

    xml0 = fetch_bytes(sitemap_url)
    root0, _, _, _ = _parse_xml_root(xml0)
    is_index = _looks_like_sitemap_index(root0)

    child_sitemaps: List[str] = []
    all_locs: List[str] = []
    job_locs: List[str] = []
    per_child: List[Dict[str, Any]] = []

    if is_index:
        log.info("Sitemap is an index (%d child sitemaps max)", max_child_sitemaps)
        locs = _collect_locs(xml0)
        child_sitemaps = locs[:max_child_sitemaps]

        for sm in child_sitemaps:
            log.debug("Parsing child sitemap: %s", sm)
            try:
                xml = fetch_bytes(sm)
            except Exception:
                per_child.append({"sitemap": sm, "error": "fetch_failed"})
                continue

            locs2 = _collect_locs(xml)
            all_locs.extend(locs2)

            matched = [u for u in locs2 if _is_job_url(u)]
            # stable unique within child
            seen_c: Set[str] = set()
            uniq_c: List[str] = []
            for u in matched:
                if u in seen_c:
                    continue
                seen_c.add(u)
                uniq_c.append(u)

            per_child.append(
                {
                    "sitemap": sm,
                    "total_urls": len(locs2),
                    "matched_job_urls": len(matched),
                    "unique_job_urls": len(uniq_c),
                    "sample_job_urls": uniq_c[:10],
                    "matched_job_urls_capped": uniq_c[: min(max_store_job_urls, 500)],
                }
            )

            if len(all_locs) >= max_urls_total:
                break
    else:
        all_locs = _collect_locs(xml0)

    for u in all_locs:
        if _is_job_url(u):
            job_locs.append(u)

    # stable unique across all
    seen: Set[str] = set()
    uniq_jobs: List[str] = []
    for u in job_locs:
        if u in seen:
            continue
        seen.add(u)
        uniq_jobs.append(u)

    log.info(
        "Sitemap results: total_urls=%d, matched_job_urls=%d, unique_job_urls=%d",
        len(all_locs),
        len(job_locs),
        len(uniq_jobs),
    )

    if not uniq_jobs:
        log.warning("No job-like URLs found in sitemap: %s", sitemap_url)

    return SitemapMatchSet(
        sitemap_url=sitemap_url,
        is_index=is_index,
        child_sitemaps=child_sitemaps,
        total_urls=len(all_locs),
        matched_job_urls=len(job_locs),
        unique_job_urls=len(uniq_jobs),
        matched_job_urls_capped=uniq_jobs[:max_store_job_urls],
        sample_job_urls=uniq_jobs[:10],
        per_child=per_child,
    )


def _infer_tokens_from_job_url(job_url: str) -> Dict[str, Any]:
    """Tokens for searching in DOM/XHR."""
    p = urlparse(job_url)
    path = p.path or ""
    parts = [x for x in path.split("/") if x]
    last = parts[-1] if parts else ""

    job_id = ""
    for i, seg in enumerate(parts):
        if seg.lower() == "job" and i + 1 < len(parts):
            job_id = parts[i + 1]
            break
    if not job_id:
        for i, seg in enumerate(parts):
            if seg.lower() == "jobs" and i + 1 < len(parts):
                job_id = parts[i + 1]
                break
    if not job_id:
        m = re.search(r"([A-Za-z]?\d{5,}|\d{5,}|R\d{5,}|REQ\d+)", last, re.IGNORECASE)
        job_id = m.group(1) if m else last

    slug = unquote(last)
    slug = slug.replace("+", " ").replace("-", " ").replace("_", " ")
    slug_words = [w.strip() for w in re.split(r"\s+", slug) if w.strip()]

    # Filter out stopwords + tiny tokens to reduce false-positive XHR hits
    sw = _get_stopwords()
    slug_words = [w for w in slug_words if len(w) >= 3 and w.lower() not in sw][:12]

    return {"job_id": job_id, "slug_words": slug_words, "path_last": last}


def _detect_detail_from_html(html: str) -> List[DetailSignal]:
    signals: List[DetailSignal] = []
    h = html or ""

    if re.search(r'type=["\']application/ld\+json["\']', h, re.IGNORECASE) and (
        '"JobPosting"' in h
        or '"@type":"JobPosting"' in h
        or '"@type": "JobPosting"' in h
    ):
        signals.append(
            DetailSignal(kind="json_ld", evidence={"marker": "JobPosting JSON-LD"})
        )

    if "phApp.ddo" in h:
        signals.append(DetailSignal(kind="phapp_ddo", evidence={"marker": "phApp.ddo"}))

    if "smartApplyData" in h or 'id="smartApplyData"' in h:
        signals.append(
            DetailSignal(kind="smartapply", evidence={"marker": "smartApplyData"})
        )

    if 'id="__NEXT_DATA__"' in h:
        signals.append(
            DetailSignal(kind="next_data", evidence={"marker": "__NEXT_DATA__"})
        )
    if "window.__NUXT__" in h:
        signals.append(
            DetailSignal(kind="nuxt_data", evidence={"marker": "window.__NUXT__"})
        )
    if "window.__INITIAL_STATE__" in h or "__INITIAL_STATE__" in h:
        signals.append(
            DetailSignal(kind="initial_state", evidence={"marker": "__INITIAL_STATE__"})
        )

    return signals


def _extract_jsonld_blocks(html: str) -> List[str]:
    """Return contents of <script type="application/ld+json">...</script> blocks."""
    if not html:
        return []
    blocks: List[str] = []
    # non-greedy capture; DOTALL to include newlines
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        txt = (m.group(1) or "").strip()
        if txt:
            blocks.append(txt)
    return blocks


def probe_job_detail(
    job_url: str,
    *,
    timeout_ms: int = 30_000,
    max_xhr_bodies: int = 80,
    max_body_chars: int = 1_200_000,
) -> DetailProbeResult:
    """
    Visit the job URL with Playwright, capture DOM and XHR/fetch responses,
    and infer where job data lives.
    """
    log = logging.getLogger("probe")

    log.info("Probing job detail page: %s", job_url)

    tokens = _infer_tokens_from_job_url(job_url)
    log.debug("Inferred tokens: %s", tokens)
    job_id = str(tokens.get("job_id") or "")
    slug_words: List[str] = tokens.get("slug_words") or []

    token_patterns: List[re.Pattern] = []
    if job_id and len(job_id) >= 4:
        token_patterns.append(re.compile(re.escape(job_id), re.IGNORECASE))
    for w in slug_words:
        token_patterns.append(re.compile(re.escape(w), re.IGNORECASE))

    xhr_candidates: List[Dict[str, Any]] = []
    dom_signals: List[DetailSignal] = []
    dom_excerpt: str = ""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=DEFAULT_UA, viewport={"width": 1280, "height": 800}
        )
        page = ctx.new_page()

        def on_response(resp):
            try:
                req = resp.request
                if req.resource_type not in ("xhr", "fetch"):
                    return
                if len(xhr_candidates) >= max_xhr_bodies:
                    return

                ct = (resp.headers or {}).get("content-type", "")
                cl = (resp.headers or {}).get("content-length")
                if cl and cl.isdigit() and int(cl) > max_body_chars * 2:
                    return

                # Only read likely-text content; allow empty ct
                if ct and not any(
                    x in ct for x in ("json", "text", "javascript", "xml", "html")
                ):
                    return

                body = resp.text()
                if not body:
                    return
                if len(body) > max_body_chars:
                    body = body[:max_body_chars]

                hit = (
                    any(pat.search(body) for pat in token_patterns)
                    if token_patterns
                    else False
                )
                if hit:
                    matched = [
                        pat.pattern for pat in token_patterns if pat.search(body)
                    ][:10]
                    excerpt = _excerpt_around_matches(
                        body,
                        token_patterns,
                        context=90,
                        max_len=700,
                        color=False,  # IMPORTANT: keep JSON output clean
                    )

                    xhr_candidates.append(
                        {
                            "url": resp.url,
                            "status": resp.status,
                            "content_type": ct,
                            "matched_tokens": matched,
                            "snippet": excerpt,
                        }
                    )

            except Exception:
                return

        page.on("response", on_response)

        try:
            page.goto(job_url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(2000)
        except PWTimeoutError:
            pass

        html = page.content()
        dom_signals = _detect_detail_from_html(html)

        # fallback: does DOM contain tokens?
        try:
            dom_text = page.inner_text("body") if page.is_visible("body") else ""
        except Exception:
            dom_text = ""
        dom_hits: List[str] = []
        if dom_text and token_patterns:
            for pat in token_patterns:
                if pat.search(dom_text):
                    dom_hits.append(pat.pattern)
        if dom_hits:
            dom_signals.append(
                DetailSignal(
                    kind="dom_contains_tokens", evidence={"matched": dom_hits[:10]}
                )
            )
        # Build a DOM excerpt around the first token match (for CLI highlighting).
        dom_excerpt = ""
        kinds_now = {s.kind for s in dom_signals}
        if "json_ld" in kinds_now and token_patterns:
            # 1) Try excerpting from JSON-LD blocks directly (best signal)
            for blk in _extract_jsonld_blocks(html):
                if any(p.search(blk) for p in token_patterns):
                    dom_excerpt = _excerpt_around_matches(
                        blk,
                        token_patterns,
                        context=220,
                        max_len=1400,
                        color=False,
                    )
                    break

        # 2) If JSON-LD present but tokens didn't hit in block text, try full HTML
        if not dom_excerpt and "json_ld" in kinds_now and html and token_patterns:
            dom_excerpt = _excerpt_around_matches(
                html,
                token_patterns,
                context=220,
                max_len=1400,
                color=False,
            )

        # 3) Final fallback: visible body text
        if not dom_excerpt and dom_text and token_patterns:
            dom_excerpt = _excerpt_around_matches(
                dom_text,
                token_patterns,
                context=160,
                max_len=1200,
                color=False,
            )

        browser.close()

    kinds = {s.kind for s in dom_signals}
    if "json_ld" in kinds:
        inferred = "JSON_LD"
        log.info("Inferred detail source: JSON-LD JobPosting embedded in HTML")
    elif "phapp_ddo" in kinds:
        inferred = "PHAPP_DDO"
        log.info("Inferred detail source: phApp.ddo embedded JS state (Phenom-style)")
    elif "smartapply" in kinds:
        inferred = "SMARTAPPLY_EMBED"
        log.info("Inferred detail source: smartApplyData embedded payload")
    elif any(k in kinds for k in ("next_data", "nuxt_data", "initial_state")):
        inferred = "APP_STATE_EMBED"
        hits = [k for k in ("next_data", "nuxt_data", "initial_state") if k in kinds]
        log.info("Inferred detail source: app state embedded (%s)", ",".join(hits))
    elif xhr_candidates:
        inferred = "XHR_JSON_OR_HTML"
        log.info(
            "Matched %d XHR/fetch responses containing job tokens",
            len(xhr_candidates),
        )
    elif "dom_contains_tokens" in kinds:
        inferred = "SERVER_HTML"
        log.info("Inferred detail source: server-rendered HTML (tokens found in DOM)")
    else:
        inferred = "UNKNOWN"
        log.warning(
            "Inferred detail source: UNKNOWN (no DOM signals, no XHR token hits)"
        )

    # Prefer DOM highlight when job data is embedded (JSON-LD / app state / phapp / or tokens in DOM)
    kinds2 = {s.kind for s in dom_signals}
    if any(
        k in kinds2
        for k in (
            "json_ld",
            "phapp_ddo",
            "smartapply",
            "next_data",
            "nuxt_data",
            "initial_state",
            "dom_contains_tokens",
        )
    ):
        highlight_source = "DOM"
    else:
        highlight_source = "XHR" if xhr_candidates else "DOM"

    return DetailProbeResult(
        job_url=job_url,
        inferred_detail_source=inferred,
        tokens=tokens,
        signals=dom_signals,
        xhr_hits=xhr_candidates,
        dom_excerpt=dom_excerpt,
        highlight_source=highlight_source,
    )


def _infer_search_source_from_xhr(url: str, body: str) -> Optional[str]:
    """Lightweight vendor hints from request URL/body."""
    lu = (url or "").lower()
    lb = (body or "").lower()

    # Oracle HCM / Fusion
    if "/hcmrestapi/resources/" in lu and "recruitingce" in lu:
        return "ORACLE_HCM_API"
    if "requisitionlist" in lb and "totaljobscount" in lb:
        return "ORACLE_HCM_API"

    return None


def probe_search_page(
    search_url: str,
    *,
    timeout_ms: int = 30_000,
    scroll_steps: int = 4,
    scroll_px: int = 2000,
    max_xhr_bodies: int = 120,
    max_body_chars: int = 900_000,
) -> SearchProbeResult:
    """
    Visit a careers search page (infinite scroll / modal UI) and identify which
    XHR/fetch responses likely contain job listing payloads and/or job details.
    """
    log = logging.getLogger("probe")
    log.info("Probing search page (fallback): %s", search_url)

    xhr_hits: List[Dict[str, Any]] = []
    inferred: Optional[str] = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=DEFAULT_UA, viewport={"width": 1280, "height": 900}
        )
        page = ctx.new_page()

        def on_response(resp):
            nonlocal inferred
            try:
                req = resp.request
                if req.resource_type not in ("xhr", "fetch"):
                    return
                if len(xhr_hits) >= max_xhr_bodies:
                    return

                ct = (resp.headers or {}).get("content-type", "")
                if ct and not any(
                    x in ct for x in ("json", "text", "javascript", "xml", "html")
                ):
                    return

                body = resp.text()
                if not body:
                    return
                if len(body) > max_body_chars:
                    body = body[:max_body_chars]

                # keyword match
                matched = [
                    pat.pattern for pat in JOB_DATA_PATTERNS if pat.search(body)
                ][:12]
                if not matched:
                    return

                # infer vendor/source if we can
                if inferred is None:
                    inferred = _infer_search_source_from_xhr(resp.url, body)

                excerpt = _excerpt_around_matches(
                    body,
                    JOB_DATA_PATTERNS,
                    context=180,
                    max_len=1200,
                    color=False,
                )

                xhr_hits.append(
                    {
                        "url": resp.url,
                        "status": resp.status,
                        "content_type": ct,
                        "matched_keywords": matched,
                        "snippet": excerpt,
                    }
                )
            except Exception:
                return

        page.on("response", on_response)

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1500)
        except PWTimeoutError:
            pass

        # Scroll a few times to trigger additional "load more" XHRs
        for _ in range(scroll_steps):
            try:
                page.mouse.wheel(0, scroll_px)
                page.wait_for_timeout(1200)
            except Exception:
                break

        html = page.content()
        try:
            dom_text = page.inner_text("body") if page.is_visible("body") else ""
        except Exception:
            dom_text = ""

        browser.close()

    # DOM keyword hits (visible text + raw HTML)
    dom_hits: List[str] = []
    for pat in JOB_DATA_PATTERNS:
        if (dom_text and pat.search(dom_text)) or (html and pat.search(html)):
            dom_hits.append(pat.pattern)
    dom_hits = dom_hits[:15]

    # Decide highlight: prefer XHR if we found strong XHR hits, otherwise DOM
    # (In practice, ORACLE_HCM_API will almost always produce XHR hits.)
    highlight_source = "XHR" if xhr_hits else "DOM"
    if highlight_source == "XHR":
        highlight_excerpt = xhr_hits[0].get("snippet", "")
    else:
        highlight_excerpt = _excerpt_around_matches(
            html or dom_text,
            JOB_DATA_PATTERNS,
            context=220,
            max_len=1200,
            color=False,
        )

    inferred_final = inferred
    if inferred_final is None:
        if xhr_hits:
            inferred_final = "GENERIC_XHR_JSON"
        elif dom_hits:
            inferred_final = "DOM_ONLY"
        else:
            inferred_final = "UNKNOWN"

    return SearchProbeResult(
        search_url=search_url,
        inferred_search_source=inferred_final,
        dom_keyword_hits=dom_hits,
        xhr_keyword_hits=xhr_hits,
        highlight_source=highlight_source,
        highlight_excerpt=highlight_excerpt,
    )


def _choose_best_sitemap(matches: List[SitemapMatchSet]) -> Optional[Dict[str, Any]]:
    """
    Choose a sitemap to drive the detail probe. Preference order:
      1) highest unique_job_urls
      2) if tied, highest matched_job_urls
      3) if tied, non-index (often faster/more direct)
    """
    if not matches:
        return None
    scored = []
    for m in matches:
        scored.append(
            (m.unique_job_urls, m.matched_job_urls, 0 if not m.is_index else -1, m)
        )
    scored.sort(reverse=True, key=lambda t: (t[0], t[1], t[2]))
    best = scored[0][3]

    log = logging.getLogger("probe")
    log.info(
        "Chosen sitemap: %s (unique_job_urls=%d)",
        best.sitemap_url,
        best.unique_job_urls,
    )

    return {
        "sitemap_url": best.sitemap_url,
        "unique_job_urls": best.unique_job_urls,
        "matched_job_urls": best.matched_job_urls,
        "is_index": best.is_index,
        "reason": "max_unique_job_urls",
    }


def probe_company(
    company: str,
    pair: Dict[str, str],
    *,
    max_child_sitemaps: int = 200,
    max_urls_total: int = 500_000,
    max_store_job_urls: int = 5_000,
) -> ProbeResult:
    log = logging.getLogger("probe")

    log.info("==== Probing company: %s ====", company)

    website = pair.get("website") or pair.get("site") or ""
    careers = pair.get("careers") or pair.get("career") or ""
    search = pair.get("search") or ""

    sm = discover_sitemaps(website, careers)

    matches: List[SitemapMatchSet] = []
    for hit in sm.found:
        try:
            matches.append(
                collect_job_urls_from_sitemap(
                    hit.url,
                    max_child_sitemaps=max_child_sitemaps,
                    max_urls_total=max_urls_total,
                    max_store_job_urls=max_store_job_urls,
                )
            )
        except Exception:
            continue

    chosen = _choose_best_sitemap(matches)
    detail_result: Optional[DetailProbeResult] = None
    search_probe: Optional[SearchProbeResult] = None

    if chosen:
        best = next(
            (m for m in matches if m.sitemap_url == chosen["sitemap_url"]), None
        )

        # If sitemap enumeration worked, do detail probe on a sample job URL.
        if best and best.sample_job_urls:
            try:
                detail_result = probe_job_detail(best.sample_job_urls[0])
            except Exception:
                detail_result = None

        # If the chosen sitemap has 0 job URLs, fall back to probing the search page
        elif best and best.unique_job_urls == 0:
            if search:
                try:
                    search_probe = probe_search_page(search)
                except Exception:
                    search_probe = None
            else:
                log.warning(
                    "Chosen sitemap has 0 job URLs, but no 'search' URL provided"
                )
    else:
        log.warning("No suitable sitemap found for detail probing")
        # If no sitemap usable at all, still try search probe if provided
        if search:
            try:
                search_probe = probe_search_page(search)
            except Exception:
                search_probe = None

    log.info("Completed probe for %s", company)

    return ProbeResult(
        company=company,
        website=website,
        careers=careers,
        sitemap=sm,
        sitemap_matches=matches,
        chosen=chosen,
        detail=detail_result,
        search=search,
        search_probe=search_probe,
    )


def _print_human(res: ProbeResult) -> None:
    print("=" * 80)
    print(f"Company: {res.company}")
    print(f"Website: {res.website}")
    print(f"Careers: {res.careers}")
    print("-" * 80)

    if not res.sitemap.found:
        print("Sitemaps: NOT FOUND")
        print(f"Tried ({len(res.sitemap.tried)}):")
        for u in res.sitemap.tried[:12]:
            print(f"  - {u}")
        if len(res.sitemap.tried) > 12:
            print("  ...")
        return

    print(f"Sitemaps found: {len(res.sitemap.found)}")
    for hit in res.sitemap.found[:12]:
        print(
            f"  - {hit.url} (base={hit.base}, via={hit.via}, evidence={hit.evidence})"
        )
    if len(res.sitemap.found) > 12:
        print("  ...")

    if res.sitemap_matches:
        print("-" * 80)
        for m in res.sitemap_matches[:8]:
            print(f"Sitemap: {m.sitemap_url}")
            print(f"  Is index: {m.is_index}")
            print(f"  Total URLs: {m.total_urls}")
            print(f"  Matched job-ish: {m.matched_job_urls}")
            print(f"  Unique job-ish:  {m.unique_job_urls}")
            print("  Sample job URLs:")
            for u in m.sample_job_urls[:10]:
                print(f"    - {u}")
        if len(res.sitemap_matches) > 8:
            print("  ...")

    if res.chosen:
        print("-" * 80)
        print(f"Chosen sitemap for detail probe: {res.chosen}")

    if res.detail:
        d = res.detail
        print("-" * 80)
        print(f"Detail probe job URL: {d.job_url}")
        print(f"Inferred detail source: {d.inferred_detail_source}")
        print(f"Tokens: {d.tokens}")
        print("Signals:")
        for s in d.signals:
            print(f"  - {s.kind}: {s.evidence}")
        # Decide what to show as the primary highlight
        color = _supports_color()

        # Rebuild patterns from tokens for highlighting
        pats: List[re.Pattern] = []
        job_id = str((d.tokens or {}).get("job_id") or "")
        if job_id and len(job_id) >= 4:
            pats.append(re.compile(re.escape(job_id), re.IGNORECASE))
        for w in (d.tokens or {}).get("slug_words") or []:
            try:
                pats.append(re.compile(re.escape(str(w)), re.IGNORECASE))
            except Exception:
                pass

        if getattr(d, "highlight_source", "DOM") == "DOM" and getattr(
            d, "dom_excerpt", ""
        ):
            print("Highlight (DOM):")
            snip = d.dom_excerpt
            snip = _highlight_only(snip, pats, color=color)
            print("  " + "\n  ".join(snip.splitlines()))

        elif d.xhr_hits:
            print("Highlight (XHR/fetch):")
            h0 = d.xhr_hits[0]
            print(f"  - {h0['status']} {h0['url']}")
            print(f"    content_type={h0.get('content_type')}")
            print(f"    matched={h0.get('matched_tokens')}")
            snip = h0.get("snippet") or ""
            snip = _highlight_only(snip, pats, color=color)
            print("    snippet:")
            print("      " + "\n      ".join(snip.splitlines()))
        else:
            print("No highlight candidate found.")
    if res.search_probe:
        sp = res.search_probe
        print("-" * 80)
        print(f"Search probe URL: {sp.search_url}")
        print(f"Inferred search source: {sp.inferred_search_source}")
        print(f"DOM keyword hits: {sp.dom_keyword_hits}")

        if sp.xhr_keyword_hits:
            print("XHR/fetch keyword hits (top 8):")
            for h in sp.xhr_keyword_hits[:8]:
                print(f"  - {h['status']} {h['url']}")
                print(f"    content_type={h.get('content_type')}")
                print(f"    matched={h.get('matched_keywords')}")
        else:
            print("No XHR/fetch keyword hits detected.")

        print(f"Highlight ({sp.highlight_source}):")
        color = _supports_color()
        pats = JOB_DATA_PATTERNS[:]
        snip = sp.highlight_excerpt or ""
        snip = _highlight_only(snip, pats, color=color)
        print("  " + "\n  ".join(snip.splitlines()))


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="Path to JSON dict of companies", required=True)
    ap.add_argument("--json-out", help="Optional path to write full JSON results")
    ap.add_argument(
        "--max-store-job-urls",
        type=int,
        default=5000,
        help="Cap on number of matched job URLs stored per sitemap in JSON output",
    )
    ap.add_argument(
        "--max-child-sitemaps",
        type=int,
        default=200,
        help="Cap on number of child sitemaps followed for sitemap indexes",
    )
    ap.add_argument(
        "--max-urls-total",
        type=int,
        default=500000,
        help="Cap on total URLs scanned per sitemap to avoid runaway memory/time",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )

    args = ap.parse_args(argv)

    setup_logging(args.verbose)

    with open(args.input, "r", encoding="utf-8") as f:
        companies = json.load(f)

    if not isinstance(companies, dict):
        raise SystemExit("Input JSON must be an object/dict at top-level")

    results: Dict[str, Any] = {}
    for name, pair in companies.items():
        if not isinstance(pair, dict):
            continue
        res = probe_company(
            name,
            pair,
            max_child_sitemaps=args.max_child_sitemaps,
            max_urls_total=args.max_urls_total,
            max_store_job_urls=args.max_store_job_urls,
        )
        _print_human(res)
        results[name] = asdict(res)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
