"""Public stop IDs and URL slugs (name, platform number and a 5-letter id: `dworzec-glowny-zachod09-xqoww`)."""

from __future__ import annotations

import re
import secrets
import string
import unicodedata

ID_RE = re.compile(r"^[a-z]{5}$")
_EXTRA = str.maketrans({"ł": "l", "Ł": "L", "đ": "d", "Đ": "D", "ß": "ss", "æ": "ae", "ø": "o"})


def slugify(text: str) -> str:
    """Lowercase ASCII slug: Polish letters to ASCII, anything else to single dashes."""
    s = unicodedata.normalize("NFKD", text.translate(_EXTRA))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "stop"


def platform_slug(name: str, platform: str | None) -> str:
    """Group slug + platform number without separator: `rondo-mogilskie01`."""
    suffix = re.sub(r"[^a-z0-9]", "", slugify(platform)) if platform else ""
    return slugify(name) + suffix


def normalize_name(name: str | None) -> str:
    return slugify(name) if name else ""


def new_public_id() -> str:
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(5))


def url_key(slug: str, public_id: str) -> str:
    return f"{slug}-{public_id}"


def parse_url_key(key: str) -> str | None:
    """Return the public ID from `{slug}-{id}` (or a bare id). The slug part is ignored."""
    candidate = key.rsplit("-", 1)[-1]
    return candidate if ID_RE.match(candidate) else None
