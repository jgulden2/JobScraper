from utils.canonicalize import canonicalize_record, validate_records
from utils.schema import CANON_COLUMNS


def test_canonicalize_and_validate_date_and_url():
    raw = {
        "Position Title": "Systems Engineer",
        "Detail URL": "https://example.com/path?utm=foo",
        "Post Date": "2025/11/01",  # loose → ISO in canonicalizer
        "Salary Raw": "$50/hr - $80/hr",  # → annualized min/max
        "Description": "<p>Hello<br/>World</p>",
    }
    out = canonicalize_record("VendorX", raw)

    # URL stripped of query, date normalized, salary annualized
    assert out["Detail URL"] == "https://example.com/path"
    assert out["Post Date"] == "2025-11-01"
    assert out["Salary Min (USD/yr)"] and out["Salary Max (USD/yr)"]
    assert all(k in out for k in CANON_COLUMNS)

    # Validate requireds and formats
    errs, problems = validate_records([out])
    assert errs == 0, problems


def test_required_columns_enforced():
    bad = {"Vendor": "X", "Position Title": "Y", "Detail URL": "not-a-url"}
    errs, problems = validate_records([bad])
    assert errs == 1
    assert any("bad_detail_url" in p[1] for p in problems)
