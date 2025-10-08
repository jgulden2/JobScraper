import json
import re
import requests
from scrapers.base import JobScraper
from traceback import format_exc


class BAESystemsScraper(JobScraper):
    def __init__(self):
        self.suppress_console = False

        super().__init__(
            base_url="https://jobs.baesystems.com/global/en/search-results",
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def raw_id(self, raw_job):
        return raw_job.get("jobId")

    def extract_total_results(self, phapp_data):
        return int(phapp_data.get("eagerLoadRefineSearch", {}).get("totalHits", 0))

    def extract_phapp_ddo(self, html):
        pattern = re.compile(r"phApp\.ddo\s*=\s*(\{.*?\});", re.DOTALL)
        match = pattern.search(html)
        if not match:
            raise ValueError("phApp.ddo object not found in HTML")
        phapp_ddo_str = match.group(1)
        data = json.loads(phapp_ddo_str)
        return data

    def fetch_job_detail(self, job_id):
        url = f"https://jobs.baesystems.com/global/en/job/{job_id}/"
        self.log("detail:fetch", url=url)
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
        except requests.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            self.log(
                "detail:http_error",
                level="warning",
                url=url,
                status=status,
                error=format_exc(),
            )
            return {}
        html = response.text
        try:
            phapp_data = self.extract_phapp_ddo(html)
        except Exception:
            self.log("detail:parse_error", level="warning", url=url, error=format_exc())
            return {}
        return phapp_data.get("jobDetail", {}).get("data", {}).get("job", {})

    def fetch_data(self):
        all_jobs = []
        offset = 0
        page_size = 10
        job_limit = 15 if getattr(self, "testing", False) else float("inf")

        first_page_url = f"{self.base_url}?from={offset}&s=1"
        response = requests.get(first_page_url, headers=self.headers)
        response.raise_for_status()
        html = response.text
        phapp_data = self.extract_phapp_ddo(html)
        total_results = self.extract_total_results(phapp_data)

        # Logging handled centrally; keep info messages here
        self.log("source:total", total=total_results)

        while offset < total_results and len(all_jobs) < job_limit:
            page_url = f"{self.base_url}?from={offset}&s=1"
            response = requests.get(page_url, headers=self.headers)
            response.raise_for_status()
            html = response.text
            phapp_data = self.extract_phapp_ddo(html)
            self.log("list:page", offset=offset, requested=page_size)

            jobs = (
                phapp_data.get("eagerLoadRefineSearch", {})
                .get("data", {})
                .get("jobs", [])
            )

            if not jobs:
                self.log("list:done", reason="empty")
                break

            all_jobs.extend(jobs)

            self.log("list:fetched", count=len(jobs), offset=offset)

            offset += page_size

        self.log("list:done", reason="end")
        return all_jobs

    def parse_job(self, job):
        job_id = job.get("jobId")
        detail = self.fetch_job_detail(job_id)

        if not detail:
            self.log("parse:errors_detail", n=1, reason="detail_empty", job_id=job_id)

        return {
            "Position Title": job.get("title"),
            "Location": job.get("cityStateCountry"),
            "Job Category": ", ".join(job.get("multi_category", [])),
            "Posting ID": job_id,
            "Post Date": job.get("postedDate"),
            "Clearance Obtainable": detail.get("clearenceLevel", ""),
            "Clearance Needed": detail.get("isSecurityClearanceRequired", ""),
            "Relocation Available": job.get("isRelocationAvailable"),
            "US Person Required": "Yes" if detail.get("itar") == "Yes" else "No",
            "Salary Min": detail.get("salaryMin", ""),
            "Salary Max": detail.get("salaryMax", ""),
            "Reward Bonus": detail.get("reward", ""),
            "Hybrid/Online Status": detail.get("physicalLocation", ""),
            "Required Skills": self.clean_html(
                detail.get("requiredSkillsEducation", "")
            ),
            "Preferred Skills": self.clean_html(
                detail.get("preferredSkillsEducation", "")
            ),
            "Job Description": self.clean_html(detail.get("description", "")),
        }
