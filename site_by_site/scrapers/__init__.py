"""
Scraper package initialization and dynamic registry construction.

This module discovers all modules in the current package (so each scraper
class is imported) and then builds a registry mapping short, CLI-friendly
names (e.g., "baesystems") to their corresponding `JobScraper` subclasses.

The registry is used by the CLI to look up and run selected scrapers.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import Dict, Type

from .base import JobScraper

# Discover and import all modules in this package so subclasses register.
for loader, name, is_pkg in pkgutil.walk_packages(__path__):
    importlib.import_module(f"{__name__}.{name}")

#: Mapping from lowercase scraper key (e.g., "baesystems") to the scraper class.
SCRAPER_REGISTRY: Dict[str, Type[JobScraper]] = {
    cls.__name__.replace("Scraper", "").lower(): cls
    for cls in JobScraper.__subclasses__()
}

__all__ = ["SCRAPER_REGISTRY", "JobScraper"]
