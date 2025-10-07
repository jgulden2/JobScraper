import argparse
import logging
from scrapers import SCRAPER_REGISTRY as SCRAPER_MAPPING


def run_scraper(scraper_name, testing=False):
    scraper_class = SCRAPER_MAPPING.get(scraper_name)
    if not scraper_class:
        print(f"Unknown scraper: {scraper_name}")
        return

    print(f"Running {scraper_name} scraper... (testing={testing})")
    scraper = scraper_class()
    scraper.testing = testing

    scraper.run()
    scraper.export(f"scraped_data/{scraper_name}_jobs.csv")
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

    class ScraperField(logging.Filter):
        def filter(self, record):
            if not hasattr(record, "scraper"):
                record.scraper = ""
            return True

    fmt = "%(asctime)s [%(levelname)s] %(scraper)s %(message)s"
    formatter = logging.Formatter(fmt)

    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.INFO)
    for h in log_handlers:
        h.setFormatter(formatter)
        h.addFilter(ScraperField())
        root.addHandler(h)

    logging.getLogger("undetected_chromedriver").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)

    for scraper_name in args.scrapers or SCRAPER_MAPPING.keys():
        run_scraper(scraper_name, testing=args.testing)
