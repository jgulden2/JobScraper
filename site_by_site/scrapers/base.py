import logging
import pandas as pd
from bs4 import BeautifulSoup


class JobScraper:
    def __init__(self, base_url, headers=None, params=None):
        self.base_url = base_url
        self.headers = headers or {}
        self.params = params or {}
        self.jobs = []
        self.testing = False
        self.test_limit = 15
        self.suppress_console = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self.log_every = 25

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
        self.logger.info("fetch:start")
        data = self.fetch_data()
        if self.testing:
            data = data[: self.test_limit]
        self.logger.info(f"fetch:done n={len(data)}")
        data = self.dedupe_raw(data)
        self.logger.info(f"dedupe_raw:unique={len(data)}")
        self.logger.info(f"parse:start total={len(data)}")
        parsed = []
        for idx, raw in enumerate(data, 1):
            try:
                rec = self.parse_job(raw)
                if rec:
                    parsed.append(rec)
                    if idx == 1 or idx % self.log_every == 0:
                        self.logger.info(f"parse:progress idx={idx}/{len(data)}")
            except Exception:
                self.logger.exception("parse:error")
        parsed = self.dedupe_records(parsed)
        self.jobs = parsed
        self.logger.info(f"dedupe_records:unique={len(self.jobs)}")
        self.logger.info(f"done count={len(self.jobs)}")

    def export(self, filename):
        df = pd.DataFrame(self.jobs)
        df.to_csv(filename, index=False)
