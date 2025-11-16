import json
import sys
import types
import importlib
import importlib.util
from pathlib import Path
import pytest

# ---------- Resolve project roots ----------
HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1]  # .../site_by_site
REPO_ROOT = HERE.parents[2]  # .../JobScraper
for p in (str(PROJECT_ROOT), str(REPO_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)


# ---------- Helper: load a module from a file path ----------
def _load_module_from_file(dotted_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(dotted_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {dotted_name} from {file_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sys.modules[dotted_name] = mod
    return mod


# ---------- Build synthetic 'scrapers' package if needed ----------
try:
    import scrapers  # real package present? great
except ModuleNotFoundError:
    scrapers = types.ModuleType("scrapers")
    scrapers.__path__ = []  # make it a namespace package
    sys.modules["scrapers"] = scrapers

    # Find any *_scraper.py in the project root and alias them
    for py in sorted(PROJECT_ROOT.glob("*_scraper.py")):
        name = py.stem  # e.g., 'bae_scraper'
        dotted = f"scrapers.{name}"
        # Load module by path and register under scrapers.<name>
        mod = _load_module_from_file(dotted, py)
        setattr(scrapers, name, mod)

# ---------- Build synthetic 'utils' package if needed ----------
try:
    import utils  # real package present? great
except ModuleNotFoundError:
    utils = types.ModuleType("utils")
    utils.__path__ = []
    sys.modules["utils"] = utils

    # Map common utility modules by filename if they exist
    util_candidates = [
        "canonicalize",
        "schema",
        "detail_fetchers",
        "extractors",
        "transforms",
        "enrich",
        "db_upsert",
        "http",
        "geocode",
        "base",
        "main",
    ]
    for stem in util_candidates:
        py = PROJECT_ROOT / f"{stem}.py"
        if py.exists():
            dotted = f"utils.{stem}"
            mod = _load_module_from_file(dotted, py)
            setattr(utils, stem, mod)


# ---------- Test fixtures ----------
@pytest.fixture
def fx(request):
    base = Path(request.config.rootpath) / "tests" / "data"

    class _Fx:
        def text(self, name):
            return (base / name).read_text(encoding="utf-8")

        def json(self, name):
            return json.loads((base / name).read_text(encoding="utf-8"))

    return _Fx()


@pytest.fixture
def mock_fetch_artifacts(monkeypatch):
    """
    Replace fetch_detail_artifacts everywhere it's used, so scrapers never hit the network.
    Usage in tests: mock_fetch_artifacts({...}) where the dict contains any of:
      _html, _vendor_blob, _jsonld, _meta, _datalayer, _canonical_url
    """

    def apply(return_dict):
        def _fake(get, log, detail_url, **kwargs):
            # exactly what parse_job() expects back
            return {
                "detail_url": detail_url,
                "_html": return_dict.get("_html", ""),
                "_vendor_blob": return_dict.get("_vendor_blob"),
                "_jsonld": return_dict.get("_jsonld"),
                "_meta": return_dict.get("_meta"),
                "_datalayer": return_dict.get("_datalayer"),
                "_canonical_url": return_dict.get("_canonical_url"),
            }

        # Patch the function where each scraper imported it:
        targets = [
            "utils.detail_fetchers",
            "scrapers.bae_scraper",
            "scrapers.lockheed_scraper",
            "scrapers.rtx_scraper",
            "scrapers.gd_scraper",
            "scrapers.northrop_scraper",
        ]
        for dotted in targets:
            try:
                mod = importlib.import_module(dotted)
                monkeypatch.setattr(mod, "fetch_detail_artifacts", _fake, raising=True)
            except Exception:
                pass

    return apply
