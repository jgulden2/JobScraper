# scrapers/platform_adapters/base.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol


@dataclass(frozen=True)
class JobRef:
    """
    Listing-only job reference returned by adapters.
    Adapters should NOT fetch detail HTML. They return stable IDs + URLs (+ optional metadata).
    """

    detail_url: str
    posting_id: Optional[str] = None
    title: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None  # adapter-specific passthrough


class Adapter(Protocol):
    """
    Phase 0.3 contract:
      - list_jobs() = listing discovery only
      - normalize() = map (raw_job + artifacts) -> raw record dict
      - No detail fetching inside adapters (driver owns fetch_detail_artifacts)
    """

    def list_jobs(self, scraper: Any, cfg: Any) -> List[Dict[str, Any]]: ...

    def normalize(
        self, cfg: Any, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]: ...
