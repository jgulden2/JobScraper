from scrapers.lockheed_scraper import LockheedMartinScraper


def test_lockheed_parse_job(mock_fetch_artifacts, fx):
    html = fx.text("lockheed_detail_jsonld.html")
    # Minimal flattened JSON-LD keys Lockheed parse_job reads
    jsonld = {
        "title": "Senior Systems Engineer",
        "datePosted": "2025-10-31",
        "jobLocation.0.address.addressLocality": "Orlando",
        "jobLocation.0.address.addressRegion": "FL",
        "jobLocation.0.address.addressCountry": "US",
        "jobLocation.0.address.postalCode": "32801",
        "industry": "Aerospace",
        "employmentType": "Full-time",  # parsed into "Clearance Level Must Possess" in LM scraper
        "qualifications": "DoD Secret; MATLAB",
        "educationRequirements": "BS in Engineering",
    }
    meta = {"gtm_tbcn_division": "MFC", "gtm_tbcn_location": "Orlando, FL"}

    mock_fetch_artifacts(
        {
            "_jsonld": jsonld,
            "_meta": meta,
            "_canonical_url": "https://www.lockheedmartinjobs.com/job/12345",
            "_html": html,
        }
    )

    s = LockheedMartinScraper()
    raw = {"Posting ID": "12345", "Detail URL": "https://example"}
    out = s.parse_job(raw)

    assert out["Posting ID"] == "12345"
    assert out["Position Title"] == "Senior Systems Engineer"
    assert out["Post Date"] == "2025-10-31"
    assert out["City"] == "Orlando"
    assert out["State"] == "FL"
    assert out["Business Sector"] == "MFC"
    assert out["Raw Location"] == "Orlando, FL"
    assert out["Detail URL"] == "https://www.lockheedmartinjobs.com/job/12345"
