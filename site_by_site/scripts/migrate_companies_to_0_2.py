from __future__ import annotations

import json

from pathlib import Path
from urllib.parse import urlparse


def domain_from_url(u: str) -> str:
    try:
        return urlparse(u).netloc or ""
    except Exception:
        return ""


src = Path("configs/companies.json")  # your old format
dst = Path("configs/companies.0.2.json")

raw = json.loads(src.read_text(encoding="utf-8"))

out = {"version": "0.2", "companies": {}}

for key, cfg in raw.items():
    name = cfg.get("name") or cfg.get("vendor") or key
    careers_home = (
        (cfg.get("entrypoints") or {}).get("careers_home")
        or cfg.get("careers_home")
        or ""
    )
    domain = (
        cfg.get("domain")
        or domain_from_url(careers_home)
        or domain_from_url((cfg.get("entrypoints") or {}).get("search_url") or "")
    )

    entrypoints = cfg.get("entrypoints") or {}
    discovery = cfg.get("discovery") or {}
    access = cfg.get("access") or {}

    out["companies"][key] = {
        "identity": {"company_id": key, "name": name, "domain": domain},
        "entry_points": {
            "careers_home": careers_home,
            "search_url": entrypoints.get("search_url") or cfg.get("search_url"),
            "sitemap_url": entrypoints.get("sitemap_url") or cfg.get("sitemap_url"),
            "sitemap_index_url": entrypoints.get("sitemap_index_url")
            or cfg.get("sitemap_index_url"),
        },
        "platform": {"name": cfg.get("platform")} if cfg.get("platform") else {},
        "access_policy": {
            "headers": (access.get("headers") or {}),
            "requires_browser_bootstrap": bool(
                access.get("requires_browser_bootstrap", False)
            ),
            "max_rps": float(access.get("max_rps", 2.0)),
            "cooldown_minutes": float(access.get("cooldown_minutes", 0)),
        },
        "discovery_hints": {
            "type": discovery.get("type"),
            "job_url_contains": discovery.get("job_url_contains"),
            "allowed_prefixes": discovery.get("allowed_prefixes") or [],
        },
    }

dst.parent.mkdir(parents=True, exist_ok=True)
dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Wrote {dst}")
