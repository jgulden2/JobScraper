"""
Command-line entrypoint to run one or more job scrapers and export results.

This module wires up:
- Argument parsing (which scrapers to run, logging destination, suppression, testing).
- Structured logging configuration with a 'scraper' attribute on each record.
- A simple lifecycle per scraper: instantiate → set testing → run() → export().

The registry of available scrapers is imported from `scrapers.SCRAPER_REGISTRY`.
"""

from __future__ import annotations

import argparse
import logging
import pandas as pd

from pathlib import Path
from time import time
from utils.geocode import geocode_unique
from typing import Mapping, Optional, Protocol, Sequence, Type, List, Dict
from scrapers import SCRAPER_REGISTRY as SCRAPER_MAPPING


class ScraperProtocol(Protocol):
    """
    Protocol describing the minimal scraper interface expected by this CLI.

    Implementations are constructed with no required arguments, expose a
    `.testing` boolean attribute that influences fetch volume, and provide
    a `run()` method to collect and parse jobs followed by an `export()`
    method to save results to disk.
    """

    testing: bool

    def run(self) -> None:
        """Execute the scraper's fetch → parse → dedupe lifecycle."""

    def export(self, filename: str) -> None:
        """
        Export the collected job records to a file.

        Args:
            filename: Destination path (e.g., 'scraped_data/{name}_jobs.csv').
        """

    jobs_full: List[Dict]


class ScraperField(logging.Filter):
    """
    Logging filter that guarantees a 'scraper' attribute on log records.

    This lets the formatter include '%(scraper)s' safely even for log
    messages emitted outside scraper adapters (e.g., third-party libs).
    """

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        """
        Ensure a 'scraper' attribute exists on the record.

        Args:
            record: The log record to filter/enrich.

        Returns:
            True to allow the record to be handled.
        """
        if not hasattr(record, "scraper"):
            record.scraper = ""
        return True


def configure_logging(logfile: Optional[str], suppress_console: bool) -> None:
    """
    Configure root logging with optional file/console handlers and a uniform format.

    Args:
        logfile: Path to a log file. If provided, logs are written here.
        suppress_console: If True, do not attach a console (stdout) handler.

    Returns:
        None

    Raises:
        OSError: If the logfile cannot be opened/created by the FileHandler.
        ValueError: If logging configuration fails due to invalid handler/formatter setup.
    """
    handlers: list[logging.Handler] = []
    if logfile:
        handlers.append(logging.FileHandler(logfile))
    if not suppress_console and not logfile:
        # If a log file is provided, we default to file-only to keep the console quiet.
        handlers.append(logging.StreamHandler())
    if not handlers:
        handlers.append(logging.NullHandler())

    fmt = "%(asctime)s [%(levelname)s] %(scraper)s %(message)s"
    formatter = logging.Formatter(fmt)

    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.INFO)

    filt = ScraperField()
    for h in handlers:
        h.setFormatter(formatter)
        h.addFilter(filt)
        root.addHandler(h)

    # Quiet down verbose third-party libraries unless debugging.
    logging.getLogger("undetected_chromedriver").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)


def run_scraper(
    scraper_name: str,
    testing: bool = False,
    registry: Mapping[str, Type[ScraperProtocol]] = SCRAPER_MAPPING,
) -> ScraperProtocol | None:
    """
    Run a single scraper end-to-end and export its results.

    Args:
        scraper_name: Key in the scraper registry indicating which scraper to run.
        testing: If True, scrapers should run in reduced-scope mode (few jobs).
        registry: Mapping of scraper names to scraper classes.

    Returns:
        ScraperProtocol or None

    Raises:
        KeyError: If the scraper name is not present in the registry.
    """
    logger = logging.getLogger(__name__)
    logger.info("run:start", extra={"scraper": scraper_name})

    scraper_class = registry.get(scraper_name)
    if not scraper_class:
        # Keep user-visible feedback while remaining silent in logs when suppressed.
        print(f"Unknown scraper: {scraper_name}")
        logger.info("run:finish", extra={"scraper": scraper_name})
        return

    print(f"Running {scraper_name} scraper... (testing={testing})")
    scraper: ScraperProtocol = scraper_class()
    scraper.testing = testing

    # Ensure export destination exists.
    out_dir = Path("scraped_data")
    out_dir.mkdir(parents=True, exist_ok=True)

    scraper.run()
    scraper.export(str(out_dir / f"{scraper_name}_jobs.csv"))

    print(f"Finished {scraper_name}.\n")
    logger.info("run:finish", extra={"scraper": scraper_name})
    return scraper


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """
    Parse CLI arguments.

    Args:
        argv: Optional sequence of raw CLI tokens. If None, uses sys.argv.

    Returns:
        Parsed arguments namespace with attributes:
            - scrapers: Optional list of scraper names to run (defaults to all).
            - logfile: Optional path to write structured logs.
            - suppress: Whether to suppress console logging.
            - testing: Whether to run in testing mode (reduced job count).
    """
    parser = argparse.ArgumentParser(description="Run one or more job scrapers.")
    parser.add_argument(
        "--scrapers",
        nargs="*",
        choices=SCRAPER_MAPPING.keys(),
        help="Specify one or more scrapers to run. If omitted, all will run.",
    )
    parser.add_argument(
        "--logfile",
        type=str,
        default="run.log",
        help="Path to log file (default: run.log).",
    )
    parser.add_argument(
        "--suppress",
        action="store_true",
        help="Suppress console logging.",
    )
    parser.add_argument(
        "--testing",
        nargs="?",
        const="true",
        default="false",
        help='Enable testing mode (can optionally specify limit, e.g. "--testing 10")',
    )
    parser.add_argument(
        "--combine-full",
        nargs="?",
        const="scraped_data/all_full.csv",
        default=None,
        metavar="OUT_CSV",
        help=(
            "After running all selected scrapers, write one combined FULL canonical CSV to this path. "
            "If used without a value, defaults to scraped_data/all_full.csv."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Program entrypoint: configure logging, parse args, and run selected scrapers.

    Args:
        argv: Optional sequence of raw CLI tokens. If None, uses sys.argv.

    Returns:
        Process exit code (0 on success).
    """
    args = parse_args(argv)
    configure_logging(args.logfile, args.suppress)

    if args.testing.lower() == "false":
        testing = False
        test_limit = None
    else:
        testing = True
        try:
            test_limit = int(args.testing)
        except ValueError:
            test_limit = None

    # Run selected scrapers (or all if none specified) and keep handles.
    to_run = list(args.scrapers or SCRAPER_MAPPING.keys())
    ran: list[ScraperProtocol] = []
    for scraper_name in to_run:
        s = run_scraper(scraper_name, testing=args.testing)
        s.testing = testing
        s.test_limit = test_limit
        if s is not None:
            ran.append(s)

    # Optionally write one combined FULL canonical CSV across all scrapers.
    if args.combine_full:
        rows: list[dict] = []
        for s in ran:
            jf = getattr(s, "jobs_full", None)
            if isinstance(jf, list) and jf:
                rows.extend(jf)

        if rows:
            # gather all existing Location strings (non-empty)
            loc_strings = [
                r.get("Raw Location") or r.get("Location", "")
                for r in rows
                if r.get("Raw Location") or r.get("Location")
            ]
            logger = logging.getLogger(__name__)
            n_rows = len(rows)
            n_loc = len(loc_strings)
            uniq = sorted(set(loc_strings))
            logger.info(
                f"geocode:rows:start rows={n_rows} locations={n_loc} unique={len(uniq)}"
            )
            start = time()
            lookup = geocode_unique(
                loc_strings,
                cache_path=".cache/geocode.sqlite",
                user_agent="jobscraper-geocoder",
                rate_limit_s=1.1,
            )
            # augment each row if we have a hit
            introduced_cols = (
                set().union(*[set(rec.keys()) for rec in lookup.values()])
                if lookup
                else set()
            )
            matched = 0
            for r in rows:
                q = r.get("Raw Location") or r.get("Location", "")
                rec = lookup.get(q)
                if not rec:
                    continue

                lat = rec.get("Geo Latitude")
                lon = rec.get("Geo Longitude")
                if lat is not None and r.get("Latitude") != lat:
                    r["Latitude"] = lat
                if lon is not None and r.get("Longitude") != lon:
                    r["Longitude"] = lon

                norm_map = {
                    "Country": rec.get("Geo Country"),
                    "State": rec.get("Geo State"),
                    "City": rec.get("Geo City"),
                    "Postal Code": rec.get("Geo Postcode"),
                }
                changed = False
                for col, val in norm_map.items():
                    if val and r.get(col) != val:
                        r[col] = val
                        changed = True
                if changed or lat is not None or lon is not None:
                    matched += 1

            new_geo_cols = len([c for c in introduced_cols if c.startswith("Geo ")])
            logger.info(
                f"geocode:rows:finish matched_rows={matched} unique_lookups={len(lookup)} new_geo_cols={new_geo_cols} time={round(time() - start)} s"
            )

        if rows:
            out_path = Path(args.combine_full)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rows).to_csv(out_path, index=False)
            logging.getLogger(__name__).info(
                "export:combined_full",
                extra={"scraper": "", "path": str(out_path), "n": len(rows)},
            )
        else:
            logging.getLogger(__name__).info(
                "export:combined_full:empty",
                extra={"scraper": "", "reason": "no_full_rows"},
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
