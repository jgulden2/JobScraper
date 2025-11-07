from scrapers.northrop_scraper import NorthropGrummanScraper


def test_northrop_parse_job_html_only(monkeypatch, mock_fetch_artifacts):
    # Force the detail API path to do nothing / not 200
    s = NorthropGrummanScraper()

    class FakeResp:
        status_code = 404
        url = "https://jobs.northropgrumman.com/careers/job/abc"
        reason = "Not Found"

        def json(self):
            return {}

        def raise_for_status(self):
            # mimic requests' behavior
            import requests

            if 400 <= self.status_code:
                raise requests.HTTPError(
                    f"{self.status_code} Client Error: {self.reason} for url: {self.url}"
                )

    monkeypatch.setattr(s.session, "get", lambda *a, **k: FakeResp())

    mock_fetch_artifacts(
        {
            "_jsonld": {"employmentType": "Full-time", "datePosted": "2025-09-20"},
            "_html": "<html></html>",
            "_canonical_url": "https://jobs.northropgrumman.com/careers/job/abc",
        }
    )

    raw = {
        "pid": "abc",
        "ats_job_id": "abc",
        "title": "Avionics Eng",
        "detail_url": "https://jobs.northropgrumman.com/careers/job/abc",
    }
    out = s.parse_job(raw)
    assert out["Posting ID"] == "abc"
    assert out["Full Time Status"] == "Full-time"
    assert out["Post Date"] == "2025-09-20"
