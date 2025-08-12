import json
import re
import requests
import logging
import time
import argparse
import pandas as pd
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


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


class BAESystemsScraper(JobScraper):
    def __init__(self):
        self.suppress_console = False

        super().__init__(
            base_url="https://jobs.baesystems.com/global/en/search-results",
            headers={"User-Agent": "Mozilla/5.0"},
        )

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

        first_page_url = f"{self.base_url}?from={offset}&s=1"
        response = requests.get(first_page_url, headers=self.headers)
        response.raise_for_status()
        html = response.text
        phapp_data = self.extract_phapp_ddo(html)
        total_results = self.extract_total_results(phapp_data)

        if not self.suppress_console:
            print(f"Total job postings: {total_results}")

        while offset < total_results:
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


class LockheedMartinScraper(JobScraper):
    BASE_URL = "https://www.lockheedmartinjobs.com"
    SEARCH_URL = f"{BASE_URL}/search-jobs"

    def __init__(self, max_pages=None, delay=0.5):
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.visited_job_ids = set()
        self.max_pages = max_pages
        self.delay = delay
        self.suppress_console = False  # For compatibility

    def fetch_data(self):
        total_pages = self.get_total_pages()
        if self.max_pages:
            total_pages = min(total_pages, self.max_pages)

        all_job_links = []
        for page_num in range(1, total_pages + 1):
            if not self.suppress_console:
                print(f"Scraping search page {page_num}/{total_pages}")
            page_links = self.get_job_links(page_num)
            all_job_links.extend(page_links)
            time.sleep(self.delay)

        return all_job_links

    def parse_job(self, job_entry):
        return self.scrape_job_detail(job_entry["url"], job_entry["job_id"])

    def scrape(self):
        start_time = time.time()
        jobs_data = self.fetch_data()

        for job in jobs_data:
            try:
                details = self.parse_job(job)
                if details:
                    self.jobs.append(details)
                    if not self.suppress_console:
                        print(
                            f"Parsed job: {details['Position Title']} at {details['Location']}"
                        )
            except Exception as e:
                if not self.suppress_console:
                    print(f"Failed to parse job ID {job['job_id']}: {e}")

        total_duration = time.time() - start_time
        print(
            f"{len(self.jobs)} job postings collected in {total_duration:.2f} seconds."
        )

    def get_total_pages(self):
        response = requests.get(self.SEARCH_URL, headers=self.headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        pagination = soup.select_one("section#search-results")
        return int(pagination["data-total-pages"]) if pagination else 1

    def get_job_links(self, page_num):
        page_url = f"{self.SEARCH_URL}?p={page_num}"
        response = requests.get(page_url, headers=self.headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        job_links = []

        for link in soup.select("section#search-results-list a[data-job-id]"):
            job_id = link["data-job-id"]
            href = link["href"]
            if job_id not in self.visited_job_ids:
                self.visited_job_ids.add(job_id)
                full_url = f"{self.BASE_URL}{href}"
                job_links.append({"job_id": job_id, "url": full_url})

        return job_links

    def scrape_job_detail(self, url, job_id):
        if not self.suppress_console:
            print(f"Scraping job detail: {url}")
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        json_ld = soup.find("script", type="application/ld+json")
        job_data = {}
        if json_ld:
            try:
                clean_json = re.sub(r"[\x00-\x1F\x7F]", "", json_ld.string)
                job_data = json.loads(clean_json)
            except json.JSONDecodeError as e:
                if not self.suppress_console:
                    print(f"Warning: JSON-LD parse failed for {url}: {e}")

        def extract_or_empty(field, default=""):
            return job_data.get(field, default)

        locations = []
        for loc in job_data.get("jobLocation", []):
            address = loc.get("address", {})
            city = address.get("addressLocality", "")
            region = address.get("addressRegion", "")
            full_location = ", ".join(filter(None, [city, region]))
            if full_location:
                locations.append(full_location)
        location = "; ".join(locations)

        salary_min, salary_max = self.extract_salary_range(soup)

        return {
            "Position Title": extract_or_empty("title"),
            "Location": location,
            "Job Category": self.extract_job_category(
                soup, extract_or_empty("industry")
            ),
            "Posting ID": job_id,
            "Post Date": extract_or_empty("datePosted"),
            "Clearance Obtainable": extract_or_empty("employmentType"),
            "Clearance Needed": self.extract_tagged_value(soup, "Clearance Level"),
            "Relocation Available": self.extract_tagged_value(
                soup, "Relocation Available"
            ),
            "US Person Required": "Yes" if "US Citizen" in soup.text else "No",
            "Salary Min": salary_min,
            "Salary Max": salary_max,
            "Reward Bonus": "",
            "Hybrid/Online Status": self.extract_tagged_value(
                soup, "Ability to Work Remotely"
            ),
            "Required Skills": self.clean_html(extract_or_empty("qualifications")),
            "Preferred Skills": self.clean_html(
                extract_or_empty("educationRequirements")
            ),
            "Job Description": self.clean_html(extract_or_empty("description")),
        }

    def extract_salary_range(self, soup):
        pay_rate_tag = soup.find("strong", string=lambda s: s and "Pay Rate:" in s)
        if not pay_rate_tag or not pay_rate_tag.next_sibling:
            return "", ""

        pay_rate_text = str(pay_rate_tag.next_sibling)
        match = re.search(r"\$([\d,]+)\s*-\s*\$([\d,]+)", pay_rate_text)
        if match:
            min_salary = match.group(1).replace(",", "")
            max_salary = match.group(2).replace(",", "")
            return min_salary, max_salary

        return "", ""

    def extract_job_category(self, soup, json_ld_industry):
        value = self.extract_tagged_value(soup, "Career Area")
        if value:
            return value.strip()

        if json_ld_industry:
            return re.sub(r"^\d+:\s*", "", json_ld_industry).strip()

        return ""

    def extract_tagged_value(self, soup, label):
        tag = soup.find("b", string=lambda text: text and label in text)
        if tag and tag.next_sibling:
            return str(tag.next_sibling).strip(": ").strip()
        return ""


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

    def extract_phapp_ddo(self, html):
        pattern = re.compile(r"phApp\.ddo\s*=\s*(\{.*?\});", re.DOTALL)
        match = pattern.search(html)
        if not match:
            raise ValueError("phApp.ddo object not found in HTML")
        return json.loads(match.group(1))

    def extract_total_results(self, phapp_data):
        return int(phapp_data.get("eagerLoadRefineSearch", {}).get("totalHits", 0))

    def fetch_data(self):
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

            if not self.suppress_console:
                print(f"RTX: Total job postings: {total_results}")

            jobs = (
                phapp_data.get("eagerLoadRefineSearch", {})
                .get("data", {})
                .get("jobs", [])
            )
            all_jobs.extend(jobs)

            if not self.suppress_console:
                print(f"RTX: Fetched {len(jobs)} jobs from initial page")

            offset += self.page_size

            while offset < total_results:
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

                all_jobs.extend(jobs)
                if not self.suppress_console:
                    print(f"RTX: Fetched {len(jobs)} jobs from offset {offset}")

                offset += self.page_size
                time.sleep(1)  # Politeness delay

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

    def parse_job(self, job):
        job_id = job.get("jobId")
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
            "Position Title": job.get("title"),
            "Location": job.get("cityStateCountry"),
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

    def scrape(self):
        start_time = time.time()
        message = "Starting RTX scrape process"
        logging.info(message) if not self.suppress_console else print(message)

        jobs_data = self.fetch_data()

        unique_jobs = {}
        for job in jobs_data:
            job_id = job.get("jobId")
            if job_id and job_id not in unique_jobs:
                unique_jobs[job_id] = job

        message = f"Initial RTX scrape complete: {len(unique_jobs)} unique jobs found."
        logging.info(message) if not self.suppress_console else print(message)

        for job in unique_jobs.values():
            try:
                job_info = self.parse_job(job)
                if job_info:
                    self.jobs.append(job_info)
                    msg = f"Parsed RTX job: {job_info['Position Title']} at {job_info['Location']}"
                    if not self.suppress_console and logging.getLogger().hasHandlers():
                        logging.info(msg)
            except Exception as e:
                error_msg = f"Failed to parse RTX job ID {job.get('jobId')}: {e}"
                if not self.suppress_console and logging.getLogger().hasHandlers():
                    logging.error(error_msg)

        total_duration = time.time() - start_time
        final_msg = f"{len(self.jobs)} RTX job postings collected in {total_duration:.2f} seconds."
        logging.info(final_msg) if not self.suppress_console else print(final_msg)


class NorthropGrummanScraper(JobScraper):
    START_URL = "https://jobs.northropgrumman.com/careers"
    CAREER_DETAIL_URL_TEMPLATE = "https://jobs.northropgrumman.com/careers?pid={pid}&domain=ngc.com&sort_by=recent"

    def __init__(self):
        super().__init__(base_url=self.START_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.suppress_console = False
        options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        self.driver = uc.Chrome(options=options)

    def extract_total_job_count(self):
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            message_container = soup.select_one("div.message-top-container strong")
            if message_container:
                text = message_container.get_text(strip=True)
                match = re.search(r"(\d+)\s+open jobs", text)
                if match:
                    total = int(match.group(1))
                    if not self.suppress_console:
                        print(f"Expected total jobs: {total}")
                    return total
        except Exception as e:
            if not self.suppress_console:
                print(f"Failed to extract total job count: {e}")
        return None

    def bypass_skip_button(self):
        try:
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[normalize-space()='Skip']")
                )
            ).click()
            if not self.suppress_console:
                print("Clicked 'Skip' button.")
            time.sleep(10)  # Let UI settle
        except Exception:
            if not self.suppress_console:
                print("No 'Skip' button found or click failed. Proceeding anyway.")

    def click_show_more(self, expected_total=None):
        while True:
            if expected_total:
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                current_count = len(soup.select("li.ph-job-card"))
                if current_count >= expected_total:
                    if not self.suppress_console:
                        print(f"Reached expected total of {expected_total} jobs.")
                    break

            try:
                show_more_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "button.show-more-positions")
                    )
                )
                show_more_button.click()
                if not self.suppress_console:
                    print("Clicked 'Show More Positions'.")
                time.sleep(1.5)
            except Exception:
                if not self.suppress_console:
                    print(
                        "No more 'Show More Positions' button found or timeout occurred."
                    )
                break

    def fetch_data(self):
        self.driver.get(self.START_URL)
        time.sleep(3)
        self.bypass_skip_button()
        expected_total = self.extract_total_job_count()
        self.click_show_more(expected_total)

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        job_elements = soup.select("li.ph-job-card")
        jobs = []
        for elem in job_elements:
            pid = elem.get("data-ph-job-id")
            title = elem.select_one("h3.ph-job-card-title")
            location = elem.select_one("div.ph-job-card-location")
            category = elem.select_one("div.ph-job-card-category")

            if pid:
                jobs.append(
                    {
                        "pid": pid,
                        "title": title.get_text(strip=True) if title else "",
                        "location": location.get_text(strip=True) if location else "",
                        "category": category.get_text(strip=True) if category else "",
                    }
                )

        if not self.suppress_console:
            print(f"NorthropGrumman: Fetched {len(jobs)} jobs")
        self.driver.quit()
        return jobs

    def parse_job(self, job):
        pid = job.get("pid")
        if not pid:
            return None

        url = self.CAREER_DETAIL_URL_TEMPLATE.format(pid=pid)
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        description = soup.select_one(".ph-job-description")
        qualifications = soup.select_one(".ph-job-qualifications")
        preferred = soup.select_one(".ph-job-preferred-qualifications")

        clearance = self.extract_clearance(soup)

        return {
            "Position Title": job.get("title", ""),
            "Location": job.get("location", ""),
            "Job Category": job.get("category", ""),
            "Posting ID": pid,
            "Post Date": "",  # Can be parsed if visible
            "Clearance Obtainable": clearance,
            "Clearance Needed": clearance,
            "Relocation Available": "",
            "US Person Required": (
                "Yes"
                if "US citizen" in (description.text.lower() if description else "")
                else "No"
            ),
            "Salary Min": "",
            "Salary Max": "",
            "Reward Bonus": "",
            "Hybrid/Online Status": "",
            "Required Skills": self.clean_html(
                qualifications.text if qualifications else ""
            ),
            "Preferred Skills": self.clean_html(preferred.text if preferred else ""),
            "Job Description": self.clean_html(description.text if description else ""),
        }

    def extract_clearance(self, soup):
        text = soup.get_text().lower()
        match = re.search(r"(secret|top secret|ts/sci|public trust)", text)
        return match.group(0).title() if match else ""

    def scrape(self):
        start_time = time.time()
        jobs_data = self.fetch_data()

        for job in jobs_data:
            try:
                job_info = self.parse_job(job)
                if job_info:
                    self.jobs.append(job_info)
                    if not self.suppress_console:
                        print(
                            f"Parsed job: {job_info['Position Title']} at {job_info['Location']}"
                        )
            except Exception as e:
                if not self.suppress_console:
                    print(f"Failed to parse job PID {job.get('pid')}: {e}")

        total_duration = time.time() - start_time
        print(
            f"{len(self.jobs)} Northrop Grumman job postings collected in {total_duration:.2f} seconds."
        )


SCRAPER_MAPPING = {
    "bae": BAESystemsScraper,
    "lockheed": LockheedMartinScraper,
    "rtx": RTXScraper,
    "northrop": NorthropGrummanScraper,
}


def run_scraper(scraper_name, suppress_console):
    scraper_class = SCRAPER_MAPPING.get(scraper_name)
    if not scraper_class:
        print(f"Unknown scraper: {scraper_name}")
        return

    print(f"Running {scraper_name} scraper...")
    scraper = scraper_class()
    if hasattr(scraper, "suppress_console"):
        scraper.suppress_console = suppress_console

    scraper.scrape()
    output_file = f"{scraper_name}_jobs.csv"
    scraper.export(output_file)
    print(f"Finished {scraper_name}. Results saved to {output_file}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one or more job scrapers.")
    parser.add_argument(
        "--scrapers",
        nargs="*",
        choices=SCRAPER_MAPPING.keys(),
        help="Specify one or more scrapers to run (choices: %(choices)s). If omitted, all will run.",
    )
    parser.add_argument(
        "--logfile",
        type=str,
        default=None,
        help="Optional path to log file. If set, all logs are saved to this file.",
    )
    parser.add_argument(
        "--suppress",
        action="store_true",
        help="Suppress console logging except key progress messages.",
    )
    args = parser.parse_args()

    log_handlers = []
    if args.logfile:
        log_handlers.append(logging.FileHandler(args.logfile))
    if not args.suppress and not args.logfile:
        log_handlers.append(logging.StreamHandler())
    if not log_handlers:
        log_handlers.append(logging.NullHandler())

    logging.root.handlers = []
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=log_handlers,
    )

    selected_scrapers = args.scrapers or SCRAPER_MAPPING.keys()
    for scraper_name in selected_scrapers:
        run_scraper(scraper_name, args.suppress)
