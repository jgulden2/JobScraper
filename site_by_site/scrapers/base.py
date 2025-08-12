import pandas as pd
from bs4 import BeautifulSoup


class JobScraper:
    def __init__(self, base_url, headers=None, params=None):
        self.base_url = base_url
        self.headers = headers or {}
        self.params = params or {}
        self.jobs = []

    def fetch_data(self):
        raise NotImplementedError(
            "Subclasses must implement fetch_data to retrieve API data."
        )

    def parse_job(self, raw_job):
        raise NotImplementedError(
            "Subclasses must implement parse_job to process raw job data."
        )

    def clean_html(self, raw_html):
        if not raw_html:
            return ""
        return BeautifulSoup(raw_html, "html.parser").get_text(
            separator=" ", strip=True
        )

    def scrape_jobs(self):
        data = self.fetch_data()
        for raw_job in data:
            job_info = self.parse_job(raw_job)
            if job_info:
                self.jobs.append(job_info)

    def export(self, filename):
        df = pd.DataFrame(self.jobs)
        df.to_csv(filename, index=False)
