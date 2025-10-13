import sys
import json

import pandas as pd

from utils.schema import CANON_COLUMNS, validate_row


def main(path):
    df = pd.read_csv(path)
    missing = [c for c in CANON_COLUMNS if c not in df.columns]
    report = {"file": path, "missing_columns": missing, "row_errors": 0, "errors": []}
    for i, row in df.iterrows():
        r = {k: row.get(k) if k in df.columns else None for k in CANON_COLUMNS}
        errs = validate_row(r)
        if errs:
            report["row_errors"] += 1
            if report["row_errors"] <= 1000:
                report["errors"].append({"row_index": int(i), "reasons": errs})
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main(sys.argv[1])
