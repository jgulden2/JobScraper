from __future__ import annotations

import re

from bs4 import BeautifulSoup as BS
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta


def normalize_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    try:
        p = urlparse(u.strip())
        if p.scheme in ("http", "https") and p.netloc:
            return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
        return None
    except Exception:
        return None


def to_bool(x: Any) -> Optional[bool]:
    if x is None:
        return None
    s = str(x).strip().lower()
    if s in {"true", "yes", "y", "1"}:
        return True
    if s in {"false", "no", "n", "0"}:
        return False
    return None


def parse_date(s: Optional[str], anchor_dt: Optional[datetime] = None) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "").replace("T", " ").split()[0])
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    m = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        y, mo, da = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(da):02d}"
    if anchor_dt is None:
        anchor_dt = datetime.utcnow()
    m = re.match(r"^(\d+)\s*(day|days|d)\s*ago$", s, re.I)
    if m:
        n = int(m.group(1))
        dt = anchor_dt - timedelta(days=n)
        return dt.strftime("%Y-%m-%d")
    if s.lower() in {"yesterday"}:
        dt = anchor_dt - timedelta(days=1)
        return dt.strftime("%Y-%m-%d")
    return None


def parse_money_span(
    s: Optional[str],
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    if not s:
        return None, None, None
    t = s.replace(",", "").strip()
    hr = re.findall(
        r"(\$?\d+(?:\.\d+)?)(?:\s*/\s*(hour|hr|day|mo|month|yr|year))?", t, re.I
    )
    if not hr:
        rng = re.findall(r"(\$?\d+(?:\.\d+)?)[^\d]+(\$?\d+(?:\.\d+)?)", t)
        if rng:
            a, b = rng[0]
            try:
                return float(a.replace("$", "")), float(b.replace("$", "")), "unknown"
            except Exception:
                return None, None, None
        try:
            v = float(re.findall(r"\d+(?:\.\d+)?", t)[0])
            return v, v, "unknown"
        except Exception:
            return None, None, None
    vals = []
    unit = None
    for v, u in hr:
        unit = (u or unit or "").lower()
        vals.append(float(v.replace("$", "")))
    if not vals:
        return None, None, None
    a = min(vals)
    b = max(vals)
    return a, b, unit or "yr"


def to_annual(
    min_v: Optional[float], max_v: Optional[float], unit: Optional[str]
) -> Tuple[Optional[float], Optional[float]]:
    if min_v is None or max_v is None:
        return min_v, max_v
    u = (unit or "yr").lower()
    f = 1.0
    if u in {"hour", "hr"}:
        f = 2080.0
    elif u in {"day"}:
        f = 260.0
    elif u in {"mo", "month"}:
        f = 12.0
    elif u in {"yr", "year"}:
        f = 1.0
    return round(min_v * f, 2), round(max_v * f, 2)


def parse_salary(
    raw: Optional[str],
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    a, b, u = parse_money_span(raw)
    a, b = to_annual(a, b, u)
    return a, b, raw


def parse_clearance_level(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    s = text.lower()
    if "fs poly" in s:
        return "TS/SCI w/ FS Poly"
    if "ci poly" in s:
        return "TS/SCI w/ CI Poly"
    if "ts/sci" in s or "ts sci" in s or "tsci" in s:
        return "TS/SCI"
    if re.search(r"\btop\s*secret\b", s):
        return "TS"
    if "secret" in s:
        return "Secret"
    if "public trust" in s:
        return "PublicTrust"
    if "confidential" in s:
        return "Confidential"
    return "None"


def parse_remote_status(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    s = text.lower()
    if "remote" in s and "hybrid" in s:
        return "Hybrid"
    if "remote" in s:
        return "Remote"
    if "hybrid" in s:
        return "Hybrid"
    if "on-site" in s or "onsite" in s:
        return "Onsite"
    return "Unspecified"


def parse_fulltime_status(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    s = text.lower()
    if "full" in s:
        return "Full-time"
    if "part" in s:
        return "Part-time"
    if "contract" in s or "contingent" in s or "1099" in s:
        return "Contract"
    if "intern" in s:
        return "Intern"
    if "temporary" in s or "temp" in s:
        return "Temporary"
    return "Unspecified"


def parse_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(float(str(x).replace("%", "").strip()))
    except Exception:
        return None


def parse_skills(text: Optional[str]) -> List[str]:
    if not text:
        return []
    s = re.split(r"[;,\n]", text)
    return [t.strip() for t in s if t.strip()]


def normalize_location(raw: Optional[str]) -> Dict[str, Optional[str]]:
    if not raw:
        return {
            "Raw Location": None,
            "Country": None,
            "State": None,
            "City": None,
            "Postal Code": None,
        }
    t = raw.strip()
    parts = [p.strip() for p in t.split(",")]
    city = None
    state = None
    postal = None
    country = None
    if len(parts) == 1:
        city = parts[0] or None
    elif len(parts) == 2:
        city, state = parts
    elif len(parts) >= 3:
        city, state, rest = parts[0], parts[1], ",".join(parts[2:])
        m = re.search(r"(\d{5}(?:-\d{4})?)", rest)
        postal = m.group(1) if m else None
        country = rest.replace(postal or "", "").strip(" ,") or None
    return {
        "Raw Location": raw,
        "Country": country,
        "State": state,
        "City": city,
        "Postal Code": postal,
    }


def sanitize_description(raw: Optional[str]) -> str:
    if not raw:
        return ""
    s = str(raw)
    s = s.replace("</br>", "<br>").replace("<br/>", "<br>")
    soup = BS(s, "html.parser")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for li in soup.find_all("li"):
        text = li.get_text(" ", strip=True)
        li.clear()
        li.append(text + "\n")
    txt = soup.get_text("\n", strip=True)
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    txt = txt.replace("\xa0", " ").strip()
    return txt
