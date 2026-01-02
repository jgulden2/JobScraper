"""
Base scraper class and common utilities.

`JobScraper` defines the standard lifecycle (fetch → dedupe → parse → export)
and shared helpers for logging, HTML cleaning, and record de-duplication.
Subclasses should override `fetch_data` and `parse_job`.

Typical usage (via the CLI):
    scraper = SomeVendorScraper()
    scraper.testing = True
    scraper.run()
    scraper.export("scraped_data/somevendor_jobs.csv")
"""

from __future__ import annotations

import logging
import requests
import threading

import pandas as pd

from time import time
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from typing import Any, Dict, List, Optional

from utils.schema import CANON_COLUMNS
from utils.canonicalize import canonicalize_record
from utils.metrics import Metrics


class JobScraper:
    """
    Abstract base class for all job scrapers.

    Subclasses must implement `fetch_data` and `parse_job`. The
    `run()` method orchestrates the full scraping pipeline and stores parsed
    records on `self.jobs`. Results can be exported with `export()`.

    Attributes:
        base_url: Root or listing URL for the target site.
        headers: HTTP headers to use for network requests.
        params: Query parameters to use for network requests.
        jobs: Collected, parsed job records ready for export.
        testing: When True, limits the number of raw items processed.
        test_limit: Maximum number of raw items to keep in testing mode.
        logger: LoggerAdapter that injects a `scraper` field for uniform logs.
        log_every: How often to emit parse progress (every N items).
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize a scraper with HTTP defaults and logging.

        Args:
            base_url: Root or listing URL used by the scraper.
            headers: Optional request headers; defaults to an empty dict.
            params: Optional request query params; defaults to an empty dict.

        Returns:
            None
        """
        self.base_url = base_url
        self.headers = headers or {}
        self.params = params or {}

        self.jobs: list[Dict[str, Any]] = []
        self.testing = False
        self.test_limit = 15

        # Standardized logger with a 'scraper' token for consistent formatting.
        self.logger = logging.LoggerAdapter(
            logging.getLogger(self.__class__.__name__),
            {"scraper": self.__class__.__name__.replace("Scraper", "").lower()},
        )
        self.log_every = 25
        # Default per-request timeout (seconds) if caller doesn't supply one
        self.default_timeout = 10.0
        # Shared requests.Session with retry/backoff enabled
        self.session = self.build_session_with_retries()
        self.write_full_also = True
        self.jobs_full: List[dict] = []

        # --- optional persistent storage knobs (set via CLI) ---
        self.db_url: str | None = (
            None  # e.g. sqlite:///./jobs.sqlite or postgresql://user:pwd@host/db
        )
        self.db_table: str = "jobs"
        self.db_mode: str = (
            "min"  # "min" -> self.jobs (canonical), "full" -> self.jobs_full
        )
        # When True and a DB is configured, skip raw listings already present
        # in the DB by (Vendor, Dedupe Key). Can be toggled from CLI.
        self.db_skip_existing: bool = True

        # --- concurrency knobs ---
        self.max_workers: int = 24  # good default for I/O; override per-scraper/CLI
        self._thread_local = threading.local()
        # --- metrics ---
        self.metrics = Metrics(self.__class__.__name__.replace("Scraper", "").lower())

    # ------------------------------------------------------------------
    # Thread-local requests.Session to keep sessions safe across threads
    # ------------------------------------------------------------------
    def _get_thread_session(self) -> requests.Session:
        s = getattr(self._thread_local, "session", None)
        if s is not None:
            return s
        s = requests.Session()
        s.headers.update(self.headers or {})
        # same retry policy as build_session_with_retries()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        setattr(self._thread_local, "session", s)
        return s

    def thread_get(self, url: str, **kwargs: Any) -> requests.Response:
        """
        GET using a thread-local requests.Session (safe under a ThreadPool).
        Mirrors self.get(...) behavior but isolates sockets/cookies per thread.
        """
        headers = {**(self.headers or {}), **(kwargs.pop("headers", {}) or {})}
        params = {**(self.params or {}), **(kwargs.pop("params", {}) or {})}
        timeout = kwargs.pop("timeout", self.default_timeout)
        return self._get_thread_session().get(
            url, headers=headers, params=params, timeout=timeout, **kwargs
        )

    def fmt_pairs(self, **kv: Any) -> str:
        """
        Render key/value pairs as a single space-prefixed string.

        Args:
            **kv: Arbitrary key/value pairs to serialize.

        Returns:
            Concatenated `key=value` pairs with a leading space, or an empty
            string if no pairs are provided.
        """
        if not kv:
            return ""
        parts = [f"{k}={v}" for k, v in kv.items()]
        return " " + " ".join(parts)

    def log(self, event: str, level: str = "info", **kv: Any) -> None:
        """
        Emit a standardized log line as: `event key=value ...`.

        Args:
            event: Short event token (e.g., 'fetch:start', 'list:fetched').
            level: Logging level name (e.g., 'info', 'warning', 'error').
            **kv: Structured context fields to include alongside the event.

        Returns:
            None
        """
        msg = f"{event}{self.fmt_pairs(**kv)}"
        getattr(self.logger, level)(msg)

    # -----------------------------
    # Methods to override in subclasses
    # -----------------------------
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Retrieve raw listing items from the target site.

        Returns:
            A list of raw listing items (dict-like objects) that will be fed to
            `parse_job`.

        Raises:
            NotImplementedError: Subclasses must implement this method.
            Exception: Any network or parsing exception raised by a subclass
                implementation may propagate if not handled there.
        """
        raise NotImplementedError

    def parse_job(self, raw_job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert a raw listing item into a normalized job record.

        Args:
            raw_job: One raw listing item as returned by `fetch_data`.

        Returns:
            A dict containing normalized fields for export (e.g., 'Position Title',
            'Location', 'Posting ID', etc.), or None to skip the record.

        Raises:
            NotImplementedError: Subclasses must implement this method.
            Exception: Subclass parsing errors may be raised; note that `run()`
                catches exceptions thrown inside `parse_job` and logs them as
                `parse:error`, continuing with the next item.
        """
        raise NotImplementedError

    # -----------------------------
    # Shared utilities
    # -----------------------------
    def dedupe_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate parsed job records.

        Args:
            records: Parsed job records to de-duplicate.

        Returns:
            A list with duplicates removed based on 'Posting ID' if present,
            otherwise 'Detail URL' or 'Position Title' as a fallback key.
        """
        # Track which canonical record "won" for each key so we can emit
        # (winner, duplicate) pairs for audit/review later.
        seen: Dict[str, Dict[str, Any]] = {}
        out: List[Dict[str, Any]] = []
        pairs: List[Dict[str, Any]] = []
        for r in records:
            k = r.get("Posting ID") or r.get("Detail URL") or r.get("Position Title")
            if not k:
                out.append(r)
                continue
            if k in seen:
                winner = seen[k]
                self.log(
                    "dedupe:drop",
                    key=k,
                    kept_posting=winner.get("Posting ID"),
                    kept_url=winner.get("Detail URL"),
                    kept_title=winner.get("Position Title"),
                    drop_posting=r.get("Posting ID"),
                    drop_url=r.get("Detail URL"),
                    drop_title=r.get("Position Title"),
                )
                pairs.append(
                    {
                        "Key": k,
                        "Kept Posting ID": winner.get("Posting ID"),
                        "Kept Detail URL": winner.get("Detail URL"),
                        "Kept Title": winner.get("Position Title"),
                        "Dropped Posting ID": r.get("Posting ID"),
                        "Dropped Detail URL": r.get("Detail URL"),
                        "Dropped Title": r.get("Position Title"),
                    }
                )
                continue
            seen[k] = r
            out.append(r)

        # Expose the pairs so the CLI can optionally write a CSV later
        self._dedupe_pairs = pairs
        self.log("dedupe_records:unique", n=len(out))
        return out

    # -----------------------------
    # HTTP session with retries/backoff
    # -----------------------------
    def build_session_with_retries(
        self,
        total: int = 3,
        backoff_factor: float = 0.5,
        status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
        allowed_methods: frozenset[str] = frozenset({"GET", "POST"}),
    ) -> requests.Session:
        """
        Create and return a `requests.Session` configured with retry/backoff.

        The session retries idempotent (and optionally POST) requests on transient
        network failures and specific HTTP status codes using exponential backoff.
        `Retry-After` headers are respected when present.

        Args:
            total: Maximum retry attempts for connect/read/status errors. Applies
                individually to each error category.
            backoff_factor: Multiplier for exponential backoff delays. Sleep is
                computed as: {backoff_factor} * (2 ** (retry_number - 1)), capped
                by urllib3's internal policies.
            status_forcelist: HTTP status codes that should trigger a retry.
                Typically includes 429 and 5xx.
            allowed_methods: HTTP methods that are eligible for retries.
                By default includes 'GET' and 'POST'. Use a frozenset for safety.

        Returns:
            A `requests.Session` with retry-enabled `HTTPAdapter`s mounted for both
            HTTP and HTTPS, and initialized with `self.headers`.

        Raises:
            Exception: Any unexpected error raised while constructing the session
                or mounting adapters (rare; surfaced for visibility).
        """
        s = requests.Session()
        s.headers.update(self.headers or {})
        retry = Retry(
            total=total,
            connect=total,
            read=total,
            status=total,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=allowed_methods,
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def enable_retries(self, session: requests.Session) -> None:
        """
        Attach the standard retry/backoff policy to an existing `requests.Session`.

        Use this when a scraper constructs its own session (e.g., to manage cookies
        or referers) but still wants the shared retry behavior.

        Args:
            session: The session to modify. Adapters for HTTP/HTTPS are replaced
                with versions configured for retries.

        Returns:
            None

        Raises:
            Exception: Any unexpected error raised while mounting the adapters.
        """
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """
        Issue an HTTP request via the scraper's retry-enabled session.

        This wrapper injects default headers/params from the scraper and applies a
        per-request timeout when one is not provided.

        Args:
            method: HTTP method (e.g., 'GET', 'POST', 'HEAD').
            url: Absolute or relative URL to request.
            **kwargs: Passed through to `Session.request`. Common options include:
                - headers (dict[str, str]): Per-request headers to merge with defaults.
                - params (dict[str, Any]): Query params to merge with defaults.
                - data/json/files: Request body payloads.
                - timeout (float | tuple[float, float]): Per-request timeout in seconds.

        Returns:
            The `requests.Response` object returned by the underlying session.

        Raises:
            requests.exceptions.Timeout: If the request exceeds the timeout.
            requests.exceptions.ConnectionError: On DNS/TCP failures.
            requests.exceptions.HTTPError: If `raise_for_status()` is called by
                the caller and the response indicates an error.
            requests.exceptions.RequestException: For any other request issue.
        """
        headers = kwargs.pop("headers", None)
        params = kwargs.pop("params", None)
        timeout = kwargs.pop("timeout", self.default_timeout)

        # Merge base headers/params with per-call ones
        merged_headers = {**(self.headers or {}), **(headers or {})}
        merged_params = {**(self.params or {}), **(params or {})}

        resp = self.session.request(
            method=method,
            url=url,
            headers=merged_headers,
            params=merged_params,
            timeout=timeout,
            **kwargs,
        )
        return resp

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        """
        Convenience wrapper for HTTP GET using the retry-enabled session.

        Args:
            url: Absolute or relative URL to request.
            **kwargs: Passed through to `request` (e.g., headers, params, timeout).

        Returns:
            The `requests.Response` from the GET request.

        Raises:
            requests.exceptions.Timeout
            requests.exceptions.ConnectionError
            requests.exceptions.RequestException
                (See `request` for detailed failure modes.)
        """
        return self.request("GET", url, **kwargs)

    # -----------------------------
    # Orchestrator
    # -----------------------------
    def run(self) -> None:
        """
        Execute the full scraper lifecycle and store parsed results on `self.jobs`.

        Lifecycle:
            1) fetch_data()
            2) optional slice for testing mode
            4) parse each raw item → accumulate parsed records
            5) dedupe_records()
            6) finalize self.jobs

        Args:
            None

        Returns:
            None

        Raises:
            Exception: Any exception raised by `fetch_data()` will propagate
                (parsing errors from `parse_job()` are caught and logged per item).
        """
        start = time()
        errors = 0

        self.log("fetch:start")
        with self.metrics.time("fetch.seconds"):
            data = self.fetch_data()

        if self.testing:
            # Central, consistent testing enforcement + message
            self.log("testing:limit", limit=self.test_limit)
            data = data[: self.test_limit]
        elif getattr(self, "limit_per_scraper", None):
            # Non-testing per-scraper cap (driven by --limit)
            try:
                cap = int(getattr(self, "limit_per_scraper"))
            except Exception:
                cap = None
            if cap is not None and cap > 0:
                self.log("limit:per_scraper", limit=cap)
                data = data[:cap]

        # Record how many items will be parsed (pre-dedupe), so global budgeting
        # can consume by the assigned/kept count rather than the final deduped size.
        self._kept_count = len(data)
        self.log("fetch:done", n=len(data))
        self.metrics.set_gauge("fetch.items", len(data))

        # -----------------------------
        # Optional: incremental skip via DB
        # -----------------------------
        # Use a stable "Vendor" value if the scraper defines one; otherwise None
        vendor_name = getattr(self, "vendor", None) or getattr(self, "VENDOR", None)
        if not vendor_name:
            # Last-resort fallback: readable class name without "Scraper"
            vendor_name = self.__class__.__name__.replace("Scraper", "")
        if self.db_url and self.db_skip_existing and data:
            try:
                # Build provisional keys from raw listings (ID → URL → Title)
                from utils.db_upsert import compute_dedupe_key, get_existing_keys

                def _provisional_key(raw: dict) -> str:
                    pid = (
                        raw.get("Posting ID")
                        or raw.get("id")
                        or raw.get("jobId")
                        or raw.get("reqId")
                        or raw.get("Requisition ID")
                        or ""
                    )
                    url = (
                        raw.get("Detail URL")
                        or raw.get("url")
                        or raw.get("detail_url")
                        or raw.get("href")
                        or ""
                    )
                    title = (
                        raw.get("Position Title")
                        or raw.get("title")
                        or raw.get("jobTitle")
                        or ""
                    )
                    return compute_dedupe_key(
                        {"Posting ID": pid, "Detail URL": url, "Position Title": title}
                    )

                keys = [k for k in (_provisional_key(r) for r in data) if k]
                have = get_existing_keys(self.db_url, self.db_table, vendor_name, keys)
                if have:
                    before = len(data)
                    data = [r for r in data if _provisional_key(r) not in have]
                    self.log(
                        "incremental:skip_existing",
                        before=before,
                        skipped=(before - len(data)),
                        kept=len(data),
                    )
                    self.metrics.inc("incremental.skipped", before - len(data))
                    self.metrics.set_gauge("incremental.kept", len(data))
            except Exception:
                # Non-fatal: fall back to full parse if existence check fails
                self.logger.exception("incremental:skip:error")

        self.log("parse:start", total=len(data))
        parsed_min: List[Dict[str, Any]] = []
        parsed_full: List[Dict[str, Any]] = []

        # -------------------------
        # Parallelize parse_job()
        # -------------------------
        self.log("parse:pool:start", workers=self.max_workers, total=len(data))
        self.metrics.set_gauge("parse.pool_workers", self.max_workers)

        def _do_parse(raw_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            # Keep parse_job untouched; it may call self.thread_get where needed.
            return self.parse_job(raw_item)

        idx = 0
        with (
            self.metrics.time("parse.pool_seconds"),
            ThreadPoolExecutor(max_workers=self.max_workers) as ex,
        ):
            futures = [ex.submit(_do_parse, r) for r in data]
            for fut in as_completed(futures):
                try:
                    rec = fut.result()
                except Exception:
                    errors += 1
                    self.metrics.inc("parse.errors")
                    self.logger.exception("parse:error")
                    continue
                if not rec:
                    self.metrics.inc("parse.skipped")
                    continue
                idx += 1
                artifacts = rec.pop("artifacts", None)
                full_row = {"Vendor": vendor_name, **rec}
                if artifacts:
                    full_row["_artifacts"] = artifacts
                canon_row = canonicalize_record(vendor=vendor_name, raw=rec)
                parsed_full.append(full_row)
                parsed_min.append({k: canon_row.get(k, "") for k in CANON_COLUMNS})
                if idx == 1 or idx % self.log_every == 0:
                    self.log("parse:progress", idx=idx, total=len(data))
        self.metrics.set_gauge("parse.parsed_min", len(parsed_min))
        self.log("parse:pool:done", parsed=len(parsed_min))

        # De-duplicate both views using the same keys
        before_dedup = len(parsed_min)
        parsed_min = self.dedupe_records(parsed_min)
        self.metrics.inc("dedupe.dropped", before_dedup - len(parsed_min))
        if self.write_full_also:
            parsed_full = self.dedupe_records(parsed_full)
            self.jobs_full = parsed_full

        self.jobs = parsed_min

        self.log("done", count=len(self.jobs))
        self.log("parse:errors", n=errors)
        total_sec = round(time() - start, 3)
        self.metrics.observe("run.seconds", total_sec)
        self.metrics.set_gauge("output.jobs", len(self.jobs))
        self.log("run:duration", seconds=total_sec)

    # -----------------------------
    # Export
    # -----------------------------
    def export(self, filename: str) -> None:
        """
        Export the collected jobs to CSV.

        Args:
            filename: Destination path (e.g., 'scraped_data/vendor_jobs.csv').

        Returns:
            None

        Raises:
            OSError: If the destination path is invalid or not writable.
            ValueError: If the records cannot be converted to a DataFrame (e.g.,
                unexpected record structure).
        """
        df = pd.DataFrame(self.jobs)
        df.to_csv(filename, index=False)
        if self.write_full_also and getattr(self, "jobs_full", None):
            stem, ext = (filename.rsplit(".", 1) + ["csv"])[:2]
            full_path = f"{stem}.full.{ext}"
            pd.DataFrame(self.jobs_full).to_csv(full_path, index=False)
            self.log("export:full", path=full_path, n=len(self.jobs_full))
            self.metrics.set_gauge("output.jobs_full", len(self.jobs_full))

        # ---- DB upsert (optional) ----
        try:
            if self.db_url:
                from utils.db_upsert import upsert_rows

                payload = (
                    self.jobs
                    if (self.db_mode or "min") == "min"
                    else (self.jobs_full or [])
                )
                if payload:
                    n = upsert_rows(
                        self.db_url,
                        self.db_table,
                        payload,
                        extra_indexes=("Post Date",),
                    )
                    self.log(
                        "export:db_upsert",
                        url=self.db_url,
                        table=self.db_table,
                        mode=self.db_mode,
                        n=n,
                    )
                    self.metrics.inc("db.upsert_rows", n)
                else:
                    self.log(
                        "export:db_upsert:skip",
                        reason="empty_payload",
                        mode=self.db_mode,
                    )
        except Exception:
            # Don't fail the run on DB issues; just log it
            self.logger.exception("export:db_upsert:error")
            self.metrics.inc("db.errors")
