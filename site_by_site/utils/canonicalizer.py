from __future__ import annotations

import re
import json
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

CANON_FIELDS: Tuple[str, ...] = (
    "Vendor",
    "Position Title",
    "Location",
    "Job Category",
    "Posting ID",
    "Detail URL",
    "Post Date",
    "US Person Required",
    "Clearance Needed",
    "Clearance Obtainable",
    "Relocation Available",
    "Hybrid/Online Status",
    "Salary Min (USD/yr)",
    "Salary Max (USD/yr)",
    "Salary Raw",
    "schema_version",
    "source_notes",
)

CLEARANCE_TOKENS = (
    ("ts/sci", "TS/SCI"),
    ("top secret", "Top Secret"),
    ("secret", "Secret"),
    ("public trust", "Public Trust"),
    ("confidential", "Confidential"),
    ("sci", "SCI"),
    ("ts", "Top Secret"),
)


class Canonicalizer:
    def __init__(
        self, schema_version: str = datetime.today().strftime("%Y-%m-%d")
    ) -> None:
        self.schema_version = schema_version
        self.adapters: dict[str, VendorAdapter] = {}

    def register_adapter(self, vendor_key: str, adapter: "VendorAdapter") -> None:
        self.adapters[vendor_key.lower()] = adapter

    def adapter_for(self, vendor: str) -> Optional["VendorAdapter"]:
        return self.adapters.get((vendor or "").lower())

    def canonicalize(
        self,
        *,
        vendor: str,
        record: Optional[Dict[str, Any]] = None,
        artifacts: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Produce a canonical record from normalized artifacts.

        Args:
            vendor: Short vendor key ("BAE", "RTX", etc.).
            record: Optional summary listing record.
            artifacts: Optional pre-extracted bundle from fetch_detail_artifacts()
                containing keys:
                    _vendor_blob, _jsonld, _meta, _datalayer, _canonical_url

        Returns:
            Canonicalized flat dict with CANON_FIELDS + dotted namespaces.
        """
        base: Dict[str, Any] = {k: "" for k in CANON_FIELDS}
        base["Vendor"] = vendor
        base["schema_version"] = self.schema_version

        namespaces: Dict[str, Any] = {}
        if record:
            namespaces["rec"] = {
                k: v for k, v in record.items() if not k.startswith("_")
            }

        if artifacts:
            # Flatten each available artifact into its own namespace
            if artifacts.get("_vendor_blob"):
                ns_key = self.guess_vendor_namespace(vendor)
                namespaces[ns_key] = artifacts["_vendor_blob"]
            if artifacts.get("_jsonld"):
                namespaces["ld"] = artifacts["_jsonld"]
            if artifacts.get("_meta"):
                namespaces["meta"] = artifacts["_meta"]
            if artifacts.get("_datalayer"):
                namespaces["datalayer"] = artifacts["_datalayer"]
            if artifacts.get("_canonical_url"):
                base["Detail URL"] = artifacts["_canonical_url"]

        self.fill_canonical_from_namespaces(base, namespaces)
        self.fill_canonical_from_record(base, record or {})

        html_text = artifacts.get("_html") if artifacts else None

        text = self.join_text_surfaces(record, html_text, namespaces)
        text_lc = text.lower()

        if not base.get("US Person Required"):
            base["US Person Required"] = self.detect_us_person(text)

        if not base.get("Clearance Needed"):
            cl = self.detect_clearance(text)
            if cl:
                base["Clearance Needed"] = cl
                if not base.get("Clearance Obtainable"):
                    base["Clearance Obtainable"] = cl

        if not base.get("Relocation Available"):
            if "relocation" in text_lc:
                base["Relocation Available"] = "Yes"

        if not base.get("Hybrid/Online Status"):
            if any(
                s in text_lc
                for s in (
                    "remote",
                    "hybrid",
                    "on-site",
                    "onsite",
                    "on site",
                    "work from home",
                    "telework",
                    "telecommute",
                    "wfh",
                    "w.f.h",
                    "flexible location",
                )
            ):
                base["Hybrid/Online Status"] = "Mentioned in text"

        if not base.get("Salary Raw"):
            lo, hi, span = self.parse_salary_range(text)
            if lo is not None:
                base["Salary Min (USD/yr)"] = lo
            if hi is not None:
                base["Salary Max (USD/yr)"] = hi
            if span:
                base["Salary Raw"] = span

        if not base.get("Post Date"):
            base["Post Date"] = self.best_effort_date(text) or ""

        adapter = self.adapter_for(vendor)
        if adapter:
            base, namespaces = adapter.postprocess(base, namespaces)

        out = dict(base)
        for ns, payload in namespaces.items():
            for k, v in payload.items():
                out[f"{ns}.{k}"] = v
        return out

    def fill_canonical_from_namespaces(
        self, base: Dict[str, Any], ns: Dict[str, Dict[str, Any]]
    ) -> None:
        ld = ns.get("ld", {})
        if not base.get("Position Title"):
            base["Position Title"] = str(ld.get("title", "")) if ld else ""
        if not base.get("Posting ID"):
            ident = ld.get("identifier") if isinstance(ld, dict) else None
            if isinstance(ident, dict):
                base["Posting ID"] = str(ident.get("value") or ident.get("name") or "")
        if not base.get("Post Date"):
            base["Post Date"] = self.norm_date(ld.get("datePosted")) if ld else ""
        if not base.get("Location") and ld:
            base["Location"] = self.locations_from_jsonld(ld.get("jobLocation"))
        if not base.get("Detail URL"):
            meta = ns.get("meta", {})
            if isinstance(meta, dict):
                ogurl = meta.get("og:url")
                if ogurl:
                    base["Detail URL"] = ogurl

        desc = ""
        if ld:
            d = ld.get("description")
            if isinstance(d, str):
                desc = d
        if desc and not base.get("Salary Raw"):
            lo, hi, span = self.parse_salary_range(desc)
            if lo is not None:
                base["Salary Min (USD/yr)"] = lo
            if hi is not None:
                base["Salary Max (USD/yr)"] = hi
            if span:
                base["Salary Raw"] = span

    def fill_canonical_from_record(
        self, base: Dict[str, Any], rec: Dict[str, Any]
    ) -> None:
        if not rec:
            return
        base["Position Title"] = base["Position Title"] or str(
            rec.get("Position Title") or rec.get("title") or ""
        )
        base["Posting ID"] = base["Posting ID"] or str(
            rec.get("Posting ID") or rec.get("posting_id") or ""
        )
        base["Detail URL"] = base["Detail URL"] or str(
            rec.get("Detail URL") or rec.get("detail_url") or ""
        )
        base["Location"] = base["Location"] or str(
            rec.get("Location") or rec.get("location") or ""
        )
        base["Job Category"] = base["Job Category"] or str(
            rec.get("Job Category") or rec.get("category") or ""
        )
        base["Post Date"] = base["Post Date"] or self.norm_date(
            rec.get("Post Date") or rec.get("post_date") or None
        )
        if not base.get("Salary Raw"):
            lo, hi, span = self.parse_salary_range(str(rec))
            if lo is not None:
                base["Salary Min (USD/yr)"] = lo
            if hi is not None:
                base["Salary Max (USD/yr)"] = hi
            if span:
                base["Salary Raw"] = span

    def locations_from_jsonld(self, loc: Any) -> str:
        def one(addr) -> Optional[str]:
            if not isinstance(addr, dict):
                return None
            city = addr.get("addressLocality") or ""
            region = addr.get("addressRegion") or ""
            parts = [p for p in (city, region) if p]
            return ", ".join(parts) if parts else None

        if isinstance(loc, list):
            vals = []
            for item in loc:
                a = item.get("address") if isinstance(item, dict) else None
                v = one(a) if a else None
                if v:
                    vals.append(v)
            return "; ".join(sorted(set(vals)))
        if isinstance(loc, dict):
            a = loc.get("address")
            v = one(a) if a else None
            return v or ""
        return ""

    def parse_salary_range(self, text: str):
        m = re.search(r"([\d,]{2,})\s*USD\s*-\s*([\d,]{2,})\s*USD", text, re.I)
        if m:
            lo = int(m.group(1).replace(",", ""))
            hi = int(m.group(2).replace(",", ""))
            return lo, hi, m.group(0)
        m = re.search(r"\$([\d,]{2,})\s*-\s*\$([\d,]{2,})", text)
        if m:
            lo = int(m.group(1).replace(",", ""))
            hi = int(m.group(2).replace(",", ""))
            return lo, hi, m.group(0)
        m = re.search(r"\$([\d,]{2,})", text)
        if m:
            val = int(m.group(1).replace(",", ""))
            return val, val, m.group(0)
        return None, None, None

    def norm_date(self, s: Optional[str]) -> str:
        if not s:
            return ""
        s = s.strip()
        m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return datetime(y, mo, d).strftime("%Y-%m-%d")
            except Exception:
                return ""
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
        if m:
            mo, d, y = map(int, m.groups())
            try:
                return datetime(y, mo, d).strftime("%Y-%m-%d")
            except Exception:
                return ""
        m = re.search(r"(\d{4}-\d{1,2}-\d{1,2})", s)
        if m:
            return self.norm_date(m.group(1))
        return ""

    def best_effort_date(self, text: str) -> Optional[str]:
        m = re.search(
            r"(?:date posted|posted date)\s*[:\-]?\s*([A-Za-z]{3,}\.?\s+\d{1,2},\s*\d{4}|\d{4}\-\d{1,2}\-\d{1,2}|\d{1,2}/\d{1,2}/\d{4})",
            text,
            re.I,
        )
        if m:
            return self.norm_date(m.group(1))
        return None

    def detect_us_person(self, text: str) -> str:
        t = text.lower().replace(".", "")
        markers_true = (
            "us citizenship is required",
            "us citizen",
            "us persons only",
            "us persons per itar",
            "us citizenship status is required",
            "us citizen or permanent resident",
        )
        markers_false = ("no citizenship required",)
        if any(m in t for m in markers_true):
            return "Yes"
        if any(m in t for m in markers_false):
            return "No"
        return ""

    def detect_clearance(self, text: str) -> Optional[str]:
        t = text.lower()
        for token, norm in CLEARANCE_TOKENS:
            if token in t:
                return norm
        return None

    def guess_vendor_namespace(self, vendor: str) -> str:
        v = (vendor or "").lower()
        if "rtx" in v or "raytheon" in v:
            return "phapp"
        if "bae" in v:
            return "phapp"
        if "northrop" in v:
            return "smartapply"
        return "vendor"

    def merge(self, dst: Dict[str, Any], src: Dict[str, Any]) -> None:
        for k, v in src.items():
            if k not in dst:
                dst[k] = v
            else:
                dst[k].update(v)

    def join_text_surfaces(
        self,
        rec: Optional[Dict[str, Any]],
        html_text: Optional[str],
        ns: Dict[str, Dict[str, Any]],
    ) -> str:
        parts = []
        if rec:
            parts.append(
                json.dumps(
                    {k: v for k, v in rec.items() if not k.startswith("_")},
                    ensure_ascii=False,
                )
            )
        if html_text:
            parts.append(html_text)
        for payload in ns.values():
            parts.append(json.dumps(payload, ensure_ascii=False))
        return "\n".join(parts)


class VendorAdapter:
    def postprocess(
        self, base: Dict[str, Any], namespaces: Dict[str, Dict[str, Any]]
    ) -> tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        return base, namespaces
