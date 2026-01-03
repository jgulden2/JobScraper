# scrapers/__init__.py

from typing import Dict, Type
from scrapers.engine import JobScraper

# Phase 1.2+: Config-driven companies are the primary path.
# Legacy per-company scrapers are no longer auto-discovered.
SCRAPER_REGISTRY: Dict[str, Type[JobScraper]] = {}
