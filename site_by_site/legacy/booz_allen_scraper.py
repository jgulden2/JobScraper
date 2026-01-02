from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from scrapers.engine import JobScraper
from utils.detail_fetchers import fetch_detail_artifacts
from utils.sitemap import parse_sitemap_xml


class BoozAllenScraper(JobScraper):
    VENDOR = "Booz Allen Hamilton"

    BASE_URL = "https://careers.boozallen.com"
    START_URL = (
        f"{BASE_URL}/careers/SearchJobs"  # informational; we enumerate via sitemap
    )
    SITEMAP_URL = f"{BASE_URL}/jobs/sitemap.xml"

    def __init__(self) -> None:
        super().__init__(base_url=self.START_URL, headers={"User-Agent": "Mozilla/5.0"})

    # ---------------------------------------------------------------------
    # Listing
    # ---------------------------------------------------------------------
    def fetch_data(self) -> List[Dict[str, str]]:
        resp = self.get(self.SITEMAP_URL)
        resp.raise_for_status()

        allowed_prefixes = (
            f"{self.BASE_URL}/careers/JobDetail/",
            f"{self.BASE_URL}/jobs/JobDetail/",
        )

        entries = parse_sitemap_xml(
            resp.content,
            url_filter=lambda loc: any(loc.startswith(p) for p in allowed_prefixes),
        )

        urls = [e["loc"] for e in entries]

        # Testing mode limit (matches other scrapers' pattern)
        if getattr(self, "testing", False):
            try:
                job_limit = int(getattr(self, "test_limit", 15)) or 0
            except Exception:
                job_limit = 15
        else:
            job_limit = float("inf")

        if job_limit != float("inf"):
            urls = urls[: int(job_limit)]

        seen: set[str] = set()
        jobs: List[Dict[str, str]] = []
        for u in urls:
            if u in seen:
                continue
            seen.add(u)
            # Posting ID comes from JobPosting.identifier.value on the detail page.
            jobs.append({"Detail URL": u})

        self.log("list:sitemap", total_urls=len(urls), unique=len(jobs))
        self.log("list:done", total=len(jobs))
        return jobs

    # ---------------------------------------------------------------------
    # Detail parsing (STRICT: only from embedded JobPosting JSON)
    # ---------------------------------------------------------------------
    def parse_job(self, job_entry: Dict[str, str]) -> Dict[str, Any]:
        url = (job_entry.get("Detail URL") or "").strip()
        if not url:
            raise ValueError("Missing Detail URL in job_entry")

        artifacts = fetch_detail_artifacts(
            self.thread_get,
            self.log,
            url,
            get_vendor_blob=False,
            get_jsonld=False,
            get_meta=False,
            get_datalayer=False,
        )

        html = artifacts.get("_html") or ""
        canonical_url = artifacts.get("_canonical_url") or url

        jobposting = self._extract_embedded_jobposting(html)

        title = self._req_str(jobposting, "title")
        description = self._req_str(jobposting, "description")
        date_posted = self._req_str(jobposting, "datePosted")

        identifier = jobposting.get("identifier")
        if not isinstance(identifier, dict):
            raise ValueError("JobPosting.identifier missing or not an object")
        posting_id = self._req_str(identifier, "value")

        city = ""
        state = ""
        country = ""
        postal_code = ""

        job_loc = jobposting.get("jobLocation")
        if isinstance(job_loc, dict):
            addr = job_loc.get("address")
            if isinstance(addr, dict):
                city = (addr.get("addressLocality") or "").strip()
                state = (addr.get("addressRegion") or "").strip()
                country = (addr.get("addressCountry") or "").strip()
                postal_code = (addr.get("postalCode") or "").strip()

        raw_location = ", ".join([x for x in [city, state, country] if x])

        org_name = ""
        hiring_org = jobposting.get("hiringOrganization")
        if isinstance(hiring_org, dict):
            org_name = (hiring_org.get("name") or "").strip()

        valid_through = (jobposting.get("validThrough") or "").strip()
        employment_type = (jobposting.get("employmentType") or "").strip()

        return {
            "Posting ID": posting_id,
            "Position Title": title,
            "Detail URL": canonical_url,
            "Description": description,
            "Post Date": date_posted,
            "Valid Through": valid_through,
            "Full Time Status": employment_type,
            "Business Area": org_name,
            "Raw Location": raw_location,
            "City": city,
            "State": state,
            "Country": country,
            "Postal Code": postal_code,
        }

    # ---------------------------------------------------------------------
    # Strict embedded JobPosting extractor
    # ---------------------------------------------------------------------
    @staticmethod
    def _req_str(obj: Dict[str, Any], key: str) -> str:
        v = obj.get(key)
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"JobPosting.{key} missing or empty")
        return v.strip()

    @staticmethod
    def _extract_embedded_jobposting(html: str) -> Dict[str, Any]:
        """
        Extract the object literal inside:
            <script type='text/javascript'> { ... "@type": "JobPosting", ... } </script>

        Implementation is deterministic:
          1) Locate script tags of type text/javascript (single or double quotes).
          2) Within each tag body, find the first occurrence of '"@type": "JobPosting"' or '"@type":"JobPosting"'.
          3) From the first '{' before that marker, brace-balance to the matching '}' to get the JSON text.
          4) json.loads() into a dict, require @type == JobPosting.
        """
        # Find candidate script blocks (we only care about the contents).
        script_re = re.compile(
            r"<script\b[^>]*\btype\s*=\s*(['\"])text/javascript\1[^>]*>(.*?)</script>",
            re.IGNORECASE | re.DOTALL,
        )

        type_marker_re = re.compile(r'"@type"\s*:\s*"JobPosting"', re.DOTALL)

        for m in script_re.finditer(html):
            body = m.group(2) or ""
            tm = type_marker_re.search(body)
            if not tm:
                continue

            start = body.find("{")
            if start == -1:
                raise ValueError("JobPosting script found but no '{' to start object")

            json_text = BoozAllenScraper._brace_balanced_object(body, start)
            try:
                data = json.loads(json_text, strict=False)
            except Exception as e:
                raise ValueError(
                    f"Failed to parse embedded JobPosting JSON: {e}"
                ) from e

            if not isinstance(data, dict):
                raise ValueError("Embedded JobPosting JSON parsed to non-object")

            if data.get("@type") != "JobPosting":
                raise ValueError("Embedded JSON object is not @type == JobPosting")

            return data

        raise ValueError(
            "No embedded JobPosting object found in text/javascript scripts"
        )

    @staticmethod
    def _brace_balanced_object(s: str, start_idx: int) -> str:
        """
        Return the substring s[start_idx: end_idx+1] where braces are balanced.
        Assumes s[start_idx] == '{'.
        Handles strings so braces inside quotes don't count.
        """
        if start_idx < 0 or start_idx >= len(s) or s[start_idx] != "{":
            raise ValueError("brace balancing called with invalid start_idx")

        depth = 0
        in_str = False
        esc = False

        for i in range(start_idx, len(s)):
            ch = s[i]

            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue

            # not in string
            if ch == '"':
                in_str = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return s[start_idx : i + 1]

        raise ValueError("Unbalanced braces while extracting embedded JobPosting JSON")
