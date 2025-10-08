import json
import time
import re
import requests
from scrapers.base import JobScraper
from bs4 import BeautifulSoup
from traceback import format_exc


class LockheedMartinScraper(JobScraper):
    BASE_URL = "https://www.lockheedmartinjobs.com"
    SEARCH_URL = f"{BASE_URL}/search-jobs"

    def __init__(self, max_pages=None, delay=0.5):
        super().__init__(self.SEARCH_URL, headers={"User-Agent": "Mozilla/5.0"})
        self.visited_job_ids = set()
        self.max_pages = max_pages
        self.delay = delay
        self.suppress_console = False

    def raw_id(self, raw_job):
        return raw_job.get("job_id")

    def fetch_data(self):
        total_pages = self.get_total_pages()
        self.log("list:pages", total_pages=total_pages)
        job_limit = 15 if getattr(self, "testing", False) else float("inf")

        if getattr(self, "testing", False):
            total_pages = 1
        elif self.max_pages:
            total_pages = min(total_pages, self.max_pages)

        all_job_links = []
        for page_num in range(1, total_pages + 1):
            page_links = self.get_job_links(page_num)
            self.log("list:fetched", page=page_num, count=len(page_links))
            for link in page_links:
                if len(all_job_links) >= job_limit:
                    break
                all_job_links.append(link)

            time.sleep(self.delay)

        self.log("list:done", reason="end")
        return all_job_links

    def parse_job(self, job_entry):
        return self.scrape_job_detail(job_entry["url"], job_entry["job_id"])

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
        self.log("detail:fetch", url=url)
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        json_ld = soup.find("script", type="application/ld+json")
        job_data = {}
        if json_ld:
            try:
                clean_json = re.sub(r"[\x00-\x1F\x7F]", "", json_ld.string)
                job_data = json.loads(clean_json)
            except json.JSONDecodeError:
                self.log(
                    "detail:jsonld_error", level="warning", url=url, error=format_exc()
                )

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
