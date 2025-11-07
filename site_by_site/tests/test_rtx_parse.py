from scrapers.rtx_scraper import RTXScraper


def test_rtx_parse_job(mock_fetch_artifacts, fx):
    jsonld = {
        "description": "Work on guidance systems",
        "jobLocation.0.address.postalCode": "92121",
        "workHours": "40",
    }
    vendor_blob = {
        "businessUnit": "Collins",
        "relocationEligible": True,
        "clearanceType": "Secret",
    }

    mock_fetch_artifacts(
        {
            "_jsonld": jsonld,
            "_vendor_blob": vendor_blob,
            "_canonical_url": "https://careers.rtx.com/global/en/job/999999/",
            "_html": "<html></html>",
        }
    )

    s = RTXScraper()
    raw = {
        "jobId": "999999",
        "title": "GNC Engineer",
        "address": "San Diego, CA",
        "type": "Full-time",
        "locationType": "Onsite",
        "state": "CA",
        "city": "San Diego",
        "latitude": 32.9,
        "longitude": -117.2,
        "experienceLevel": "Mid",
        "country": "US",
        "postedDate": "2025-11-01",
        "category": "Engineering",
    }
    out = s.parse_job(raw)

    assert out["Posting ID"] == "999999"
    assert out["Position Title"] == "GNC Engineer"
    assert out["Detail URL"].endswith("/999999/")
    assert out["Business Area"] == "Collins"
    assert out["Clearance Level Must Possess"] == "Secret"
    assert out["Post Date"] == "2025-11-01"
