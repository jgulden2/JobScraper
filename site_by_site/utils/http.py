from __future__ import annotations

import json
import base64


def b64url_decode(s: str) -> str:
    """
    Decode a base64url string to UTF-8 text (with missing padding tolerated).

    Args:
        s: Base64url text (padding optional).

    Returns:
        Decoded UTF-8 string.

    Raises:
        binascii.Error: If the input contains invalid base64url characters.
        UnicodeDecodeError: If the decoded bytes are not valid UTF-8.
    """
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s.encode("utf-8")).decode("utf-8")


def b64url_encode(obj: str) -> str:
    """
    Encode a Python object to compact JSON, then base64url without padding.

    Args:
        obj: JSON-serializable object.

    Returns:
        Base64url-encoded string with trailing '=' padding removed.

    Raises:
        TypeError: If `obj` is not JSON-serializable.
    """
    raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")
