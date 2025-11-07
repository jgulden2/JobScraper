from __future__ import annotations
import json
import threading
import time
from typing import Dict, Any


class Metrics:
    """
    Tiny in-process metrics sink: counters, gauges, timers, and histograms.
    Thread-safe; emits a simple JSON snapshot.
    """

    def __init__(self, namespace: str = "") -> None:
        self.ns = (namespace + ".") if namespace else ""
        self._lock = threading.Lock()
        self._c: Dict[str, float] = {}
        self._g: Dict[str, float] = {}
        self._h: Dict[
            str, Dict[str, Any]
        ] = {}  # {name: {"count":n,"sum":s,"min":m,"max":M}}

    def _key(self, name: str) -> str:
        return f"{self.ns}{name}" if self.ns else name

    def inc(self, name: str, value: float = 1.0) -> None:
        with self._lock:
            k = self._key(name)
            self._c[k] = self._c.get(k, 0.0) + value

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._g[self._key(name)] = float(value)

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            k = self._key(name)
            h = self._h.setdefault(
                k, {"count": 0, "sum": 0.0, "min": None, "max": None}
            )
            h["count"] += 1
            h["sum"] += float(value)
            h["min"] = float(value) if h["min"] is None else min(h["min"], float(value))
            h["max"] = float(value) if h["max"] is None else max(h["max"], float(value))

    def time(self, name: str):
        start = time.time()

        def _done():
            self.observe(name, time.time() - start)

        return _Timer(_done)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            snap = {
                "counters": dict(self._c),
                "gauges": dict(self._g),
                "histograms": dict(self._h),
            }
            for h in snap["histograms"].values():
                if h.get("count", 0):
                    h["avg"] = h["sum"] / h["count"]
            return snap

    def to_json(self) -> str:
        return json.dumps(self.snapshot(), separators=(",", ":"), ensure_ascii=False)


class _Timer:
    def __init__(self, cb):
        self._cb = cb

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._cb()
