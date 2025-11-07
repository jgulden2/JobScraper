from scrapers.gd_scraper import GeneralDynamicsScraper


def test_gd_parse_job(mock_fetch_artifacts, fx):
    html = fx.text("gd_detail_bold_block.html")
    mock_fetch_artifacts(
        {"_html": html, "_canonical_url": "https://www.gd.com/careers/job/some-path"}
    )
    s = GeneralDynamicsScraper()
    raw = {
        "Detail URL": "/careers/job/some-path",
        "ReferenceCode": "REF-123",
        "Position Title": "Software Engineer",
        "Date": "2025-10-01",
        "Company": "GDMS",
    }
    # parse_job copies/updates fields, then uses bold-block HTML for Description
    out = s.parse_job(raw)
    assert out["Detail URL"].startswith("https://www.gd.com/")
    assert out["Position Title"] == "Software Engineer"
    assert isinstance(out["Description"], str) and out["Description"]
