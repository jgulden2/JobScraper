# scrapers/platform_adapters/base.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol


@dataclass(frozen=True)
class JobRef:
    detail_url: str
    posting_id: Optional[str] = None
    title: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


class Adapter(Protocol):
    """
    Phase 1.1 contract:
      - probe(): returns confidence 0. plotting into [0.0, 1.0]
      - list_jobs(): listing discovery only
      - normalize(): map (raw_job + artifacts) -> raw record dict
    """

    def probe(self, cfg: Any) -> float: ...

    def list_jobs(self, scraper: Any, cfg: Any) -> List[Dict[str, Any]]: ...

    def normalize(
        self, cfg: Any, raw_job: Dict[str, Any], artifacts: Dict[str, Any]
    ) -> Dict[str, Any]: ...
