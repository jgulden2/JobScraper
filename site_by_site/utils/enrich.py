# utils/enrich.py
from __future__ import annotations

import re

from typing import Dict, List

HEADINGS = {
    "required_edu": [
        "Required Education",
        "Basic Qualifications",
        "Minimum Qualifications",
        "Required Qualifications",
        "Required Education, Experience, & Skills",
    ],
    "preferred_edu": [
        "Preferred Education",
        "Preferred Qualifications",
        "Desired Qualifications",
        "Preferred Education, Experience, & Skills",
    ],
    "required_skills": [
        "Required Skills",
        "Required Experience",
        "What You Will Need",
        "Must Have",
        "Responsibilities",
        "Required Education, Experience, & Skills",
    ],
    "preferred_skills": [
        "Preferred Skills",
        "Nice to Have",
        "Desired Skills",
        "Preferred Education, Experience, & Skills",
    ],
}
STOP_HEADINGS = [
    "Responsibilities",
    "Duties",
    "Benefits",
    "Pay",
    "Pay Information",
    "Salary",
    "Equal Opportunity",
    "EEO",
    "About Us",
    "About BAE Systems",
    "About BAE Systems Intelligence & Security",
    "Travel",
    "Relocation",
]


def _section(text: str, starts: List[str]) -> str:
    s = text or ""
    # normalize whitespace
    s = re.sub(r"\r", "\n", s)
    # greedy: pick first start marker hit, stop at next heading-ish line
    for start in starts:
        m = re.search(rf"(?im)^\s*{re.escape(start)}\s*:?\s*$", s)
        if not m:
            continue
        start_idx = m.end()
        tail = s[start_idx:]
        # find next all-capsy/bold-like heading or a known stop heading
        stop = re.search(
            rf"(?im)^\s*({'|'.join([re.escape(h) for h in STOP_HEADINGS])})\s*:?\s*$",
            tail,
        )
        chunk = tail[: stop.start()] if stop else tail
        return chunk.strip()
    return ""


def bullets_to_list(text: str) -> List[str]:
    if not text:
        return []
    # bullets or line items
    items = re.split(r"(?m)^\s*[-*•]\s+|[\n;]+", text)
    items = [re.sub(r"\s+", " ", x).strip(" \t•-*;") for x in items]
    return [x for x in items if x]


def extract_education_and_skills(description: str) -> Dict[str, str]:
    req_edu = _section(description, HEADINGS["required_edu"])
    pref_edu = _section(description, HEADINGS["preferred_edu"])
    req_sk = _section(description, HEADINGS["required_skills"])
    pref_sk = _section(description, HEADINGS["preferred_skills"])

    req_sk_list = bullets_to_list(req_sk)
    pref_sk_list = bullets_to_list(pref_sk)

    return {
        "Required Education": req_edu.strip() or None,
        "Preferred Education": pref_edu.strip() or None,
        "Required Skills": "\n".join(req_sk_list) if req_sk_list else (req_sk or None),
        "Preferred Skills": "\n".join(pref_sk_list)
        if pref_sk_list
        else (pref_sk or None),
    }
