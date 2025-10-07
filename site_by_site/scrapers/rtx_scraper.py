import json
import time
import re
import os
import requests
import undetected_chromedriver as uc
from scrapers.base import JobScraper
from selenium.webdriver.support.ui import WebDriverWait


# Silence UC's noisy destructor on Windows (enabled by default; set env var to 0 to disable)
if os.name == "nt" and os.environ.get("RTX_SILENCE_UC_DEL", "1") == "1":
    try:

        def noop(self):
            return None

        uc.Chrome.__del__ = noop
    except Exception:
        pass


class RTXScraper(JobScraper):
    def __init__(self):
        super().__init__(
            base_url="https://careers.rtx.com/global/en/search-results",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Connection": "keep-alive",
            },
        )
        self.search_url_template = (
            "https://careers.rtx.com/global/en/search-results?from={offset}&s=1"
        )
        self.job_detail_url_template = "https://careers.rtx.com/global/en/job/{job_id}/"
        self.page_size = 10
        self.suppress_console = False

    def raw_id(self, raw_job):
        return raw_job.get("jobId")

    def extract_phapp_ddo(self, html):
        pattern = re.compile(r"phApp\.ddo\s*=\s*(\{.*?\});", re.DOTALL)
        match = pattern.search(html)
        if not match:
            raise ValueError("phApp.ddo object not found in HTML")
        return json.loads(match.group(1))

    def extract_total_results(self, phapp_data):
        return int(phapp_data.get("eagerLoadRefineSearch", {}).get("totalHits", 0))

    def fetch_data(self):
        job_limit = 15 if getattr(self, "testing", False) else float("inf")

        all_jobs = []
        offset = 0

        options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1200")
        options.add_argument("--no-sandbox")

        driver = uc.Chrome(options=options)

        try:
            # First page
            first_page_url = "https://careers.rtx.com/global/en/search-results"
            driver.get(first_page_url)

            # Wait until phApp.ddo is present in the page's JS context
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script("return window.phApp && window.phApp.ddo;")
            )
            phapp_data = driver.execute_script("return window.phApp.ddo;")

            total_results = self.extract_total_results(phapp_data)

            if getattr(self, "testing", False) and not self.suppress_console:
                print(
                    f"Running in testing mode — limiting to {job_limit} job postings."
                )

            if not self.suppress_console:
                print(f"RTX: Total job postings: {total_results}")

            jobs = (
                phapp_data.get("eagerLoadRefineSearch", {})
                .get("data", {})
                .get("jobs", [])
            )
            for job in jobs:
                if len(all_jobs) >= job_limit:
                    break
                all_jobs.append(job)

            if len(all_jobs) >= job_limit:
                if not self.suppress_console:
                    print("RTX: Reached test job limit after initial page.")
                return all_jobs

            if not self.suppress_console:
                print(f"RTX: Fetched {len(jobs)} jobs from initial page")

            offset += self.page_size

            while offset < total_results:
                if len(all_jobs) >= job_limit:
                    break
                page_url = f"https://careers.rtx.com/global/en/search-results?from={offset}&s=1"
                driver.get(page_url)
                html = driver.page_source
                phapp_data = self.extract_phapp_ddo(html)

                jobs = (
                    phapp_data.get("eagerLoadRefineSearch", {})
                    .get("data", {})
                    .get("jobs", [])
                )

                if not jobs:
                    if not self.suppress_console:
                        print("RTX: No jobs returned, stopping.")
                    break

                for job in jobs:
                    if len(all_jobs) >= job_limit:
                        break
                    all_jobs.append(job)

                if not self.suppress_console:
                    print(f"RTX: Fetched {len(jobs)} jobs from offset {offset}")

                offset += self.page_size
                time.sleep(1)

        finally:
            driver.quit()

        return all_jobs

    def fetch_job_detail(self, job_id):
        url = self.job_detail_url_template.format(job_id=job_id)
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        html = response.text
        phapp_data = self.extract_phapp_ddo(html)
        return phapp_data.get("jobDetail", {}).get("data", {}).get("job", {})

    def extract_salary_range(self, text):
        match = re.search(r"([\d,]+)\s*USD\s*-\s*([\d,]+)\s*USD", text)
        if match:
            min_salary = match.group(1).replace(",", "")
            max_salary = match.group(2).replace(",", "")
            return min_salary, max_salary
        return "", ""

    def extract_section(self, text, start_markers, end_markers):
        for start_marker in start_markers:
            start_idx = text.find(start_marker)
            if start_idx != -1:
                for end_marker in end_markers:
                    end_idx = text.find(end_marker, start_idx)
                    if end_idx != -1:
                        section = text[start_idx + len(start_marker) : end_idx].strip()
                        return section.lstrip(": ").strip()
                # If no end_marker found, grab till end
                section = text[start_idx + len(start_marker) :].strip()
                return section.lstrip(": ").strip()
        return ""

    def parse_job(self, raw_job):
        job_id = raw_job.get("jobId")
        detail = self.fetch_job_detail(job_id)
        description_html = detail.get("description", "")
        clean_desc = self.clean_html(description_html)

        us_person_required = (
            "Yes" if "U.S. citizenship is required" in clean_desc else "No"
        )
        salary_min, salary_max = self.extract_salary_range(clean_desc)
        required_skills = self.extract_section(
            clean_desc,
            start_markers=[
                "Qualifications You Must Have",
                "Qualifications, Experience and Skills",
                "Basic Qualifications",
                "Skills and Experience",
                "Experience and Qualifications",
            ],
            end_markers=[
                "Qualifications We Prefer",
                "Qualifications We Value",
                "Preferred Qualifications",
                "Highly Desirable",
                "Desirable",
                "What We Offer",
                "Benefits",
                "Privacy Policy and Terms",
            ],
        )

        preferred_skills = self.extract_section(
            clean_desc,
            start_markers=[
                "Qualifications We Prefer",
                "Qualifications We Value",
                "Highly Desirable",
                "Preferred Qualifications",
                "Desirable",
            ],
            end_markers=["What We Offer", "Benefits", "Privacy Policy and Terms"],
        )

        return {
            "Position Title": raw_job.get("title"),
            "Location": raw_job.get("cityStateCountry"),
            "Job Category": detail.get("category", ""),
            "Posting ID": job_id,
            "Post Date": detail.get("dateCreated"),
            "Clearance Obtainable": detail.get("clearenceLevel", ""),
            "Clearance Needed": detail.get("clearanceType", ""),
            "Relocation Available": detail.get("relocationEligible"),
            "US Person Required": us_person_required,
            "Salary Min": salary_min,
            "Salary Max": salary_max,
            "Reward Bonus": detail.get("reward", ""),
            "Hybrid/Online Status": detail.get("locationType", ""),
            "Required Skills": required_skills,
            "Preferred Skills": preferred_skills,
            "Job Description": clean_desc,
        }
