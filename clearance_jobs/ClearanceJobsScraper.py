import requests
import json
import re
import math
from bs4 import BeautifulSoup
from time import sleep
import random
import csv
import os

BASE_URL = "https://www.clearancejobs.com/jobs"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.clearancejobs.com/",
}

LIMIT = 100  # max items per page


def get_total_jobs():
    response = requests.get(f"{BASE_URL}?PAGE=1&limit={LIMIT}", headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    jobs_text_div = soup.find("div", class_="jobs-text")
    match = re.search(r"of ([\d,]+)", jobs_text_div.text)
    return int(match.group(1).replace(",", "")) if match else 0


def extract_json_blob(html):
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", id="vike_pageContext", type="application/json")
    if not script_tag:
        return {}
    try:
        raw_json = script_tag.contents[0]
        return json.loads(raw_json)
    except Exception:
        return {}


def extract_job_urls_from_blob(blob):
    try:
        items = blob["data"]["data"]["data"]
        return [job.get("jobUrl") for job in items if job.get("jobUrl")]
    except Exception:
        return []


def collect_all_job_urls():
    total_jobs = get_total_jobs()
    total_pages = math.ceil(total_jobs / LIMIT)
    print(f"Discovered {total_jobs} jobs across {total_pages} pages...")

    all_urls = []
    for page in range(1, total_pages + 1):
        attempt = 1
        max_attempts = 3
        success = False

        while attempt <= max_attempts:
            print(f"Fetching PAGE={page} (Attempt {attempt})")
            url = f"{BASE_URL}?PAGE={page}&limit={LIMIT}"
            response = requests.get(url, headers=HEADERS)
            blob = extract_json_blob(response.text)
            page_urls = extract_job_urls_from_blob(blob)

            if page_urls:
                print(f"Found {len(page_urls)} job URLs on page {page}")
                all_urls.extend(page_urls)
                success = True
                break
            else:
                print(f"No job URLs found on page {page}. Retrying...")
                attempt += 1

        if not success:
            print(
                f"Failed to get job URLs on page {page} after {max_attempts} attempts."
            )

    return all_urls


def save_urls_to_file(urls, filename="job_urls.txt"):
    with open(filename, "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")


def flatten_json(y, parent_key="", sep="."):
    items = []
    if isinstance(y, list):
        for i, v in enumerate(y):
            items.extend(flatten_json(v, f"{parent_key}[{i}]", sep=sep).items())
    elif isinstance(y, dict):
        for k, v in y.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_json(v, new_key, sep=sep).items())
    else:
        items.append((parent_key, y))
    return dict(items)


def scrape_job_details(urls):
    all_jobs = []

    for idx, url in enumerate(urls, start=1):
        print(f"Scraping job {idx}/{len(urls)}: {url}")
        attempt = 1
        max_attempts = 3
        success = False

        while attempt <= max_attempts:
            try:
                response = requests.get(url, headers=HEADERS)
                soup = BeautifulSoup(response.text, "html.parser")
                script_tag = soup.find(
                    "script", id="vike_pageContext", type="application/json"
                )
                if not script_tag or not script_tag.contents:
                    raise ValueError("No JSON blob found.")

                raw_json = script_tag.contents[0]
                blob = json.loads(raw_json)
                job_data = blob["data"]["data"]

                # Flatten everything
                flattened = flatten_json(job_data)
                flattened["jobUrl"] = url

                # Also add cleaned plain-text description (separately, if available)
                if "description" in job_data:
                    flattened["description_text"] = (
                        BeautifulSoup(job_data["description"], "html.parser")
                        .get_text(separator="\n")
                        .strip()
                    )

                all_jobs.append(flattened)
                success = True
                break
            except Exception as e:
                print(f"Error on attempt {attempt}: {e}")
                attempt += 1

        if not success:
            print(f"Failed to scrape job at {url}")

    return all_jobs


def save_jobs_to_csv(jobs, filename="detailed_jobs.csv"):
    if not jobs:
        print("No jobs to save.")
        return

    # Dynamically collect all unique keys across all jobs
    all_keys = set()
    for job in jobs:
        all_keys.update(job.keys())
    fieldnames = sorted(all_keys)

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for job in jobs:
            writer.writerow(job)


def fetch_jobs_via_api(limit=100, max_pages=2):
    API_URL = "https://www.clearancejobs.com/api/v1/jobs/search"
    HEADERS_API = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://www.clearancejobs.com",
        "Referer": "https://www.clearancejobs.com/jobs",
    }

    all_jobs = []
    for page in range(1, max_pages + 1):
        print(f"Fetching API page {page}...")
        body = {"limit": limit, "page": page}
        try:
            response = requests.post(API_URL, headers=HEADERS_API, json=body)
            response.raise_for_status()
            data = response.json().get("data", [])
            print(f"  -> Retrieved {len(data)} jobs")
            for job in data:
                flattened = flatten_json(job)
                all_jobs.append(flattened)
        except Exception as e:
            print(f"Error fetching page {page}: {e}")

    return all_jobs


if __name__ == "__main__":
    if os.path.exists("job_urls.txt"):
        print("Found existing job_urls.txt. Using saved URLs.")
        with open("job_urls.txt", "r", encoding="utf-8") as f:
            job_urls = [line.strip() for line in f if line.strip()]
    else:
        job_urls = collect_all_job_urls()
        print(f"Collected {len(job_urls)} job URLs.")
        save_urls_to_file(job_urls)

    jobs = scrape_job_details(job_urls)
    save_jobs_to_csv(jobs)
