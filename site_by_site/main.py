import argparse
import logging
from scrapers import SCRAPER_REGISTRY as SCRAPER_MAPPING


def run_scraper(scraper_name, suppress_console, testing=False):
    scraper_class = SCRAPER_MAPPING.get(scraper_name)
    if not scraper_class:
        print(f"Unknown scraper: {scraper_name}")
        return

    print(f"Running {scraper_name} scraper... (testing={testing})")
    scraper = scraper_class()
    if hasattr(scraper, "suppress_console"):
        scraper.suppress_console = suppress_console
    scraper.testing = testing

    scraper.run()
    scraper.export(f"{scraper_name}_jobs.csv")
    print(f"Finished {scraper_name}.\n")


if __name__ == "__main__":
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
        default=None,
        help="Optional path to log file.",
    )
    parser.add_argument(
        "--suppress",
        action="store_true",
        help="Suppress console logging.",
    )
    parser.add_argument(
        "--testing",
        type=lambda x: str(x).lower() == "true",
        default=False,
        help="Run in testing mode with a small sample of jobs.",
    )

    args = parser.parse_args()

    log_handlers = []
    if args.logfile:
        log_handlers.append(logging.FileHandler(args.logfile))
    if not args.suppress and not args.logfile:
        log_handlers.append(logging.StreamHandler())
    if not log_handlers:
        log_handlers.append(logging.NullHandler())

    logging.root.handlers = []
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=log_handlers,
    )

    for scraper_name in args.scrapers or SCRAPER_MAPPING.keys():
        run_scraper(scraper_name, args.suppress, testing=args.testing)
