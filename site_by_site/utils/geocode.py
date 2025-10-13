# utils/geocode.py
from __future__ import annotations

import json
import os
import sqlite3

from time import time, sleep
from typing import Dict, Iterable, Optional
from geopy.geocoders import Nominatim


def ensure_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geo_cache (
            query TEXT PRIMARY KEY,
            json  TEXT NOT NULL,
            ts    INTEGER NOT NULL
        )
    """)
    return conn


def cache_get(conn: sqlite3.Connection, q: str) -> Optional[Dict]:
    cur = conn.execute("SELECT json FROM geo_cache WHERE query=?", (q,))
    row = cur.fetchone()
    if not row:
        return None
    return json.loads(row[0])


def cache_put(conn: sqlite3.Connection, rec: Dict) -> None:
    payload = json.dumps(rec)
    conn.execute(
        "INSERT OR REPLACE INTO geo_cache(query,json,ts) VALUES(?,?,?)",
        (rec.get("Geo Query", ""), payload, int(time())),
    )
    conn.commit()


def titleize(key: str) -> str:
    return key.replace("_", " ").title()


def parse_osm(raw: dict, query: str) -> Dict:
    addr = raw.get("address") or {}
    rec: Dict = {
        "Geo Query": query,
        "Geo Address": raw.get("display_name", ""),
        "Geo Latitude": float(raw["lat"])
        if "lat" in raw and raw["lat"] is not None
        else None,
        "Geo Longitude": float(raw["lon"])
        if "lon" in raw and raw["lon"] is not None
        else None,
    }
    # Flatten EVERY address component dynamically → "Geo <Title Cased Key>"
    for k, v in addr.items():
        rec["Geo " + titleize(k)] = v
    # Optional: include a few useful top-level OSM fields
    for k in ("addresstype", "class", "type", "place_rank", "importance", "name"):
        if k in raw:
            rec["Geo Meta " + titleize(k)] = raw[k]
    # Optional: normalized bounding box
    bb = raw.get("boundingbox")
    if isinstance(bb, (list, tuple)) and len(bb) == 4:
        (
            rec["Geo Meta Bbox South"],
            rec["Geo Meta Bbox North"],
            rec["Geo Meta Bbox West"],
            rec["Geo Meta Bbox East"],
        ) = bb
    return rec


class NominatimClient:
    def __init__(self, user_agent: str, rate_limit_s: float = 1.1, timeout: int = 10):
        self.rate_limit_s = rate_limit_s
        self.last = 0.0
        self.geocoder = Nominatim(user_agent=user_agent, timeout=timeout)

    def geocode_one(self, q: str) -> Optional[Dict]:
        if not q or not q.strip():
            return None
        # polite throttling
        now = time()
        wait = self.rate_limit_s - (now - self.last)
        if wait > 0:
            sleep(wait)
        try:
            loc = self.geocoder.geocode(q, addressdetails=True)
            self.last = time()
        except Exception:
            return None
        if not loc:
            return None
        # geopy's Location has .raw with OSM fields
        raw = getattr(loc, "raw", {}) or {}
        # ensure lat/lon exist in raw for parser
        raw.setdefault("lat", loc.latitude if hasattr(loc, "latitude") else None)
        raw.setdefault("lon", loc.longitude if hasattr(loc, "longitude") else None)
        raw.setdefault("display_name", getattr(loc, "address", "") or "")
        return parse_osm(raw, q)


def geocode_unique(
    queries: Iterable[str],
    cache_path: str = ".cache/geocode.sqlite",
    user_agent: str = "jobscraper-geocoder",
    rate_limit_s: float = 1.1,
) -> Dict[str, Dict]:
    """
    Geocode a set of location strings with on-disk caching and polite rate limiting.
    Returns a mapping query -> flat dict of "Geo …" columns (only for non-empty queries).
    """
    conn = ensure_db(cache_path)
    client = NominatimClient(user_agent=user_agent, rate_limit_s=rate_limit_s)

    out: Dict[str, Dict] = {}
    for q in {x.strip() for x in queries if x and x.strip()}:
        cached = cache_get(conn, q)
        if cached:
            out[q] = cached
            continue
        rec = client.geocode_one(q)
        if rec:
            cache_put(conn, rec)
            out[q] = rec
        else:
            # cache negative result minimally to avoid repeated failures (optional)
            cache_put(conn, {"Geo Query": q})
    return out
