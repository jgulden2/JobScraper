from __future__ import annotations

import re

from bs4 import BeautifulSoup as BS
from typing import Optional, Tuple
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


def sanitize_description(raw: Optional[str]) -> str:
    if not raw:
        return ""
    s = str(raw)
    # normalize common line-break variants early
    s = (
        s.replace("</br>", "<br>")
        .replace("<br/>", "<br>")
        .replace("<BR/>", "<br>")
        .replace("<BR>", "<br>")
    )
    soup = BS(s, "html.parser")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    # turn structural blocks into line breaks so content stays readable
    for p in soup.find_all(["p", "div"]):
        if p.find("br") is None:
            p.append(soup.new_string("\n"))
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
