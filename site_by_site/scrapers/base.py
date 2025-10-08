import logging
import pandas as pd
from time import time
from bs4 import BeautifulSoup


class JobScraper:
    def __init__(self, base_url, headers=None, params=None):
        self.base_url = base_url
        self.headers = headers or {}
        self.params = params or {}
        self.jobs = []
        self.testing = False
        self.test_limit = 15
        self.logger = logging.LoggerAdapter(
            logging.getLogger(self.__class__.__name__),
            {"scraper": self.__class__.__name__.replace("Scraper", "").lower()},
        )
        self.log_every = 25

    def fmt_pairs(self, **kv):
        if not kv:
            return ""
        parts = [f"{k}={v}" for k, v in kv.items()]
        return " " + " ".join(parts)

    def log(self, event, level="info", **kv):
        msg = f"{event}{self.fmt_pairs(**kv)}"
        getattr(self.logger, level)(msg)

    def fetch_data(self):
        raise NotImplementedError

    def parse_job(self, raw_job):
        raise NotImplementedError

    def raw_id(self, raw_job):
        return None

    def clean_html(self, raw_html):
        if not raw_html:
            return ""
        return BeautifulSoup(raw_html, "html.parser").get_text(
            separator=" ", strip=True
        )

    def dedupe_raw(self, data):
        seen = set()
        out = []
        for r in data:
            rid = self.raw_id(r)
            if not rid:
                out.append(r)
                continue
            if rid in seen:
                continue
            seen.add(rid)
            out.append(r)
        return out

    def dedupe_records(self, records):
        seen = set()
        out = []
        for r in records:
            k = r.get("Posting ID") or r.get("Detail URL") or r.get("Position Title")
            if not k:
                out.append(r)
                continue
            if k in seen:
                continue
            seen.add(k)
            out.append(r)
        return out

    def run(self):
        start = time()
        errors = 0
        self.log("fetch:start")
        data = self.fetch_data()
        if self.testing:
            # Central, consistent testing enforcement + message
            self.log("testing:limit", limit=self.test_limit)
            data = data[: self.test_limit]
        self.log("fetch:done", n=len(data))
        data = self.dedupe_raw(data)
        self.log("dedupe_raw:unique", n=len(data))
        self.log("parse:start", total=len(data))
        parsed = []
        for idx, raw in enumerate(data, 1):
            try:
                rec = self.parse_job(raw)
                if rec:
                    parsed.append(rec)
                    if idx == 1 or idx % self.log_every == 0:
                        self.log("parse:progress", idx=idx, total=len(data))
            except Exception:
                errors += 1
                self.logger.exception("parse:error")
        parsed = self.dedupe_records(parsed)
        self.jobs = parsed
        self.log("dedupe_records:unique", n=len(self.jobs))
        self.log("done", count=len(self.jobs))
        self.log("parse:errors", n=errors)
        self.log("run:duration", seconds=round(time() - start, 3))

    def export(self, filename):
        df = pd.DataFrame(self.jobs)
        df.to_csv(filename, index=False)
