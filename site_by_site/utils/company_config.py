from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from jsonschema import Draft202012Validator


@dataclass
class CompanyConfig:
    company_id: str
    name: str
    domain: str

    careers_home: str
    search_url: Optional[str]
    sitemap_url: Optional[str]
    sitemap_index_url: Optional[str]

    platform_name: Optional[str]
    platform_tenant: Optional[str]
    platform_slug: Optional[str]

    requires_browser_bootstrap: bool
    max_rps: float
    cooldown_minutes: float
    headers: Dict[str, str]

    discovery_type: Optional[str]
    job_url_contains: Optional[str]
    allowed_prefixes: list[str]
    locale: Optional[str]
    pagination: Dict[str, Any]

    disabled: bool
    status: Dict[str, Any]


def _load_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_schema(path: str | Path) -> Draft202012Validator:
    schema = _load_json(path)
    v = Draft202012Validator(schema)
    return v


def _merge_status(companies_doc: dict, status_doc: Optional[dict]) -> dict:
    if not status_doc:
        return companies_doc

    merged = json.loads(json.dumps(companies_doc))  # deep copy
    status_map = (
        (status_doc.get("status") or {}) if isinstance(status_doc, dict) else {}
    )
    companies = merged.get("companies") or {}

    for cid, rec in companies.items():
        if cid in status_map:
            rec["status"] = status_map[cid]
    return merged


def load_companies_0_2(
    companies_path: str | Path,
    schema_path: str | Path = "configs/company.schema.json",
    status_path: str | Path | None = "configs/company_status.json",
) -> Dict[str, CompanyConfig]:
    doc = _load_json(companies_path)

    status_doc = None
    if status_path and Path(status_path).exists():
        status_doc = _load_json(status_path)

    doc = _merge_status(doc, status_doc)

    validator = _load_schema(schema_path)
    errors = sorted(validator.iter_errors(doc), key=lambda e: e.path)

    if errors:
        msg = ["Company config schema validation failed:"]
        for e in errors[:50]:
            loc = ".".join(str(p) for p in e.path)
            msg.append(f" - {loc}: {e.message}")
        raise ValueError("\n".join(msg))

    out: Dict[str, CompanyConfig] = {}
    companies = doc["companies"]

    for cid, rec in companies.items():
        ident = rec["identity"]
        ep = rec["entry_points"]
        plat = rec.get("platform") or {}
        ap = rec.get("access_policy") or {}
        dh = rec.get("discovery_hints") or {}

        out[cid] = CompanyConfig(
            company_id=ident["company_id"],
            name=ident["name"],
            domain=ident["domain"],
            careers_home=ep["careers_home"],
            search_url=ep.get("search_url"),
            sitemap_url=ep.get("sitemap_url"),
            sitemap_index_url=ep.get("sitemap_index_url"),
            platform_name=plat.get("name"),
            platform_tenant=plat.get("tenant"),
            platform_slug=plat.get("slug"),
            requires_browser_bootstrap=bool(
                ap.get("requires_browser_bootstrap", False)
            ),
            max_rps=float(ap.get("max_rps", 2.0)),
            cooldown_minutes=float(ap.get("cooldown_minutes", 0)),
            headers=dict(ap.get("headers") or {}),
            discovery_type=dh.get("type"),
            job_url_contains=dh.get("job_url_contains"),
            allowed_prefixes=list(dh.get("allowed_prefixes") or []),
            locale=dh.get("locale"),
            pagination=dict(dh.get("pagination") or {}),
            disabled=bool(rec.get("disabled", False)),
            status=dict(rec.get("status") or {}),
        )

    return out


def update_company_status(
    company_id: str,
    ok: bool,
    failure_type: str | None = None,
    status_path: str | Path = "configs/company_status.json",
) -> None:
    p = Path(status_path)
    doc = _load_json(p) if p.exists() else {"version": "0.2", "status": {}}
    st = doc.setdefault("status", {}).setdefault(
        company_id,
        {
            "last_success": None,
            "last_failure": None,
            "last_failure_type": None,
            "failure_count": 0,
        },
    )

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if ok:
        st["last_success"] = now
        st["last_failure"] = None
        st["last_failure_type"] = None
        st["failure_count"] = 0
    else:
        st["last_failure"] = now
        st["last_failure_type"] = failure_type or "unknown"
        st["failure_count"] = int(st.get("failure_count") or 0) + 1

    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
