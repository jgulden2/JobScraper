from __future__ import annotations

from typing import Dict, Any
from utils.schema import CANON_COLUMNS, validate_row
from utils.transforms import (
    normalize_url,
    parse_salary,
    normalize_location,
    parse_date,
    sanitize_description,
)
from utils.enrich import extract_education_and_skills


def canonicalize_record(vendor: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: raw.get(k) for k in CANON_COLUMNS}
    out["Vendor"] = vendor

    # Normalize URL
    if out.get("Detail URL"):
        out["Detail URL"] = normalize_url(out.get("Detail URL"))

    # Normalize Post Date format (best-effort ISO yyyy-mm-dd)
    if out.get("Post Date"):
        out["Post Date"] = parse_date(out.get("Post Date"))

    # Parse/annualize salary if only raw is present
    smin_existing = out.get("Salary Min (USD/yr)")
    smax_existing = out.get("Salary Max (USD/yr)")
    if (smin_existing is None and smax_existing is None) and out.get("Salary Raw"):
        smin, smax, rawsal = parse_salary(out.get("Salary Raw"))
        out["Salary Min (USD/yr)"] = smin
        out["Salary Max (USD/yr)"] = smax
        out["Salary Raw"] = rawsal or out.get("Salary Raw")

    # Normalize location fields from Raw Location, but don't overwrite explicit fields
    loc = normalize_location(out.get("Raw Location"))
    for k in ["Raw Location", "Country", "State", "City", "Postal Code"]:
        out[k] = out.get(k) or loc.get(k)

    desc = out.get("Description")
    if desc:
        out["Description"] = sanitize_description(desc)

    if out.get("Bonus") in (None, "", "null"):
        out["Bonus"] = 0

    if any(
        out.get(k) in (None, "", [])
        for k in (
            "Required Education",
            "Preferred Education",
            "Required Skills",
            "Preferred Skills",
        )
    ):
        enrich = extract_education_and_skills(out.get("Description") or "")
        for k, v in enrich.items():
            out[k] = out.get(k) or v

    return out


def validate_records(rows):
    errs = 0
    problems = []
    for i, r in enumerate(rows):
        e = validate_row(r)
        if e:
            errs += 1
            problems.append((i, e))
    return errs, problems
