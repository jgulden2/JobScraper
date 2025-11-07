from scrapers.bae_scraper import BAESystemsScraper


def test_bae_parse_job(mock_fetch_artifacts, fx):
    # Frozen vendor blob that parse_job expects (phApp detail JSON)
    vendor_blob = fx.json("bae_detail_vendor_blob.json")

    mock_fetch_artifacts(
        {
            "_vendor_blob": vendor_blob,
            "_canonical_url": f"https://jobs.baesystems.com/global/en/job/{vendor_blob['jobId']}/",
            "_html": "<html></html>",
        }
    )

    s = BAESystemsScraper()
    raw_listing = {"jobId": vendor_blob["jobId"]}
    rec = s.parse_job(raw_listing)

    # Key fields come from vendor blob per implementation
    assert rec["Posting ID"] == vendor_blob["jobId"]
    assert rec["Position Title"] == vendor_blob["title"]
    assert rec["Detail URL"].endswith(f"/{vendor_blob['jobId']}/")
    assert isinstance(rec["Description"], str)
    # Salary normalization is left to canonicalizer; raw should be present
    assert "Salary Raw" in rec
