import logging
import json
import time
import re
import requests
from scrapers.base import JobScraper


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
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        html = response.text
        phapp_data = self.extract_phapp_ddo(html)
        job_detail = phapp_data.get("jobDetail", {}).get("data", {}).get("job", {})
        return job_detail

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

        if getattr(self, "testing", False) and not self.suppress_console:
            print(f"Running in testing mode — limiting to {job_limit} job postings.")

        if not self.suppress_console:
            print(f"Total job postings: {total_results}")

        while offset < total_results and len(all_jobs) < job_limit:
            page_url = f"{self.base_url}?from={offset}&s=1"
            response = requests.get(page_url, headers=self.headers)
            response.raise_for_status()
            html = response.text
            phapp_data = self.extract_phapp_ddo(html)

            jobs = (
                phapp_data.get("eagerLoadRefineSearch", {})
                .get("data", {})
                .get("jobs", [])
            )

            if not jobs:
                if not self.suppress_console:
                    print("No jobs returned, stopping.")
                break

            all_jobs.extend(jobs)

            if not self.suppress_console:
                print(f"Fetched {len(jobs)} jobs from offset {offset}")

            offset += page_size

        return all_jobs

    def parse_job(self, job):
        job_id = job.get("jobId")
        detail = self.fetch_job_detail(job_id)

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

    def scrape(self):
        start_time = time.time()
        message = "Starting scrape process"
        if self.suppress_console:
            print(message)
        else:
            logging.info(message)

        jobs_data = self.fetch_data()

        unique_jobs = {}
        for job in jobs_data:
            job_id = job.get("jobId")
            if job_id and job_id not in unique_jobs:
                unique_jobs[job_id] = job

        message = f"Initial scrape complete: {len(unique_jobs)} unique jobs found."
        if self.suppress_console:
            print(message)
        else:
            logging.info(message)

        for job in unique_jobs.values():
            try:
                job_info = self.parse_job(job)
                if job_info:
                    self.jobs.append(job_info)
                    message = f"Parsed job: {job_info['Position Title']} at {job_info['Location']}"
                    if not self.suppress_console and logging.getLogger().hasHandlers():
                        logging.info(message)
            except Exception as e:
                error_message = f"Failed to parse job ID {job.get('jobId')}: {e}"
                if not self.suppress_console and logging.getLogger().hasHandlers():
                    logging.error(error_message)

        total_duration = time.time() - start_time

        final_message = (
            f"{len(self.jobs)} job postings collected in {total_duration:.2f} seconds."
        )
        print(final_message) if self.suppress_console else logging.info(final_message)
