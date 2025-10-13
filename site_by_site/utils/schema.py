from __future__ import annotations

from typing import Dict, Any
from enum import Enum


class HybridStatus(str, Enum):
    Remote = "Remote"
    Hybrid = "Hybrid"
    Onsite = "Onsite"
    Multiple = "Multiple"
    Unspecified = "Unspecified"


class FullTimeStatus(str, Enum):
    FullTime = "Full-time"
    PartTime = "Part-time"
    Contract = "Contract"
    Intern = "Intern"
    Temporary = "Temporary"
    Other = "Other"
    Unspecified = "Unspecified"


class ClearanceLevel(str, Enum):
    NoneLevel = "None"
    PublicTrust = "PublicTrust"
    Confidential = "Confidential"
    Secret = "Secret"
    TS = "TS"
    TSSCI = "TS/SCI"
    TSSCI_CI = "TS/SCI w/ CI Poly"
    TSSCI_FS = "TS/SCI w/ FS Poly"


class EducationLevel(str, Enum):
    HS = "HS"
    AA_AS = "AA/AS"
    BA_BS = "BA/BS"
    MS = "MS"
    PhD = "PhD"
    Other = "Other"
    Unspecified = "Unspecified"


CANON_COLUMNS = [
    "Vendor",
    "Position Title",
    "Detail URL",
    "Description",
    "Post Date",
    "Posting ID",
    "US Person Required",
    "Clearance Level Must Possess",
    "Clearance Level Must Obtain",
    "Relocation Available",
    "Salary Raw",
    "Salary Min (USD/yr)",
    "Salary Max (USD/yr)",
    "Bonus",
    "Remote Status",
    "Full Time Status",
    "Hours Per Week",
    "Travel Percentage",
    "Job Category",
    "Business Sector",
    "Business Area",
    "Industry",
    "Shift",
    "Required Education",
    "Preferred Education",
    "Career Level",
    "Required Skills",
    "Preferred Skills",
    "Raw Location",
    "Country",
    "State",
    "City",
    "Postal Code",
    "Latitude",
    "Longitude",
]

REQUIRED_COLUMNS = ["Vendor", "Position Title", "Detail URL"]


def validate_row(row: Dict[str, Any]):
    errors = []
    for k in REQUIRED_COLUMNS:
        if not row.get(k):
            errors.append(f"missing_required:{k}")
    v = row.get("Detail URL")
    if v and not str(v).startswith(("http://", "https://")):
        errors.append("bad_detail_url")
    d = row.get("Post Date")
    if d:
        import re

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(d)):
            errors.append("bad_post_date_format")
    smin = row.get("Salary Min (USD/yr)")
    smax = row.get("Salary Max (USD/yr)")
    if smin is not None and smax is not None:
        try:
            if float(smin) > float(smax):
                errors.append("salary_min_gt_max")
        except Exception:
            errors.append("salary_non_numeric")
    return errors
