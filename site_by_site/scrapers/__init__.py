# scrapers/__init__.py
import pkgutil
import importlib
from .base import JobScraper

# Discover all modules in the current package
for loader, name, is_pkg in pkgutil.walk_packages(__path__):
    importlib.import_module(f"{__name__}.{name}")

# Register all subclasses
SCRAPER_REGISTRY = {
    cls.__name__.replace("Scraper", "").lower(): cls
    for cls in JobScraper.__subclasses__()
}
