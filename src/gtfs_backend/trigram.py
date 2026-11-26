"""pg_trgm's similarity and word_similarity in Python, value for value, so stop search can run in memory and rank as the
SQL it replaces did (`q <% name`, `word_similarity(q, name)`, `similarity(q, name)`).

A port of contrib/pg_trgm (trgm_op.c) for already lowercased, unaccented text: words are runs of letters and digits,
each padded with two spaces in front and one behind, and cut into trigrams. Scores are float4 as in Postgres
(float32 here), so ties and the 0.6 threshold come out the same. tests/shared/test_trigram.py checks it against the
database.
"""

from __future__ import annotations

import re
import unicodedata

import numpy as np

WORD_SIMILARITY_THRESHOLD = np.float32(0.6)  # pg_trgm.word_similarity_threshold: `<%`
_WORD = re.compile(r"[^\W_]+")  # pg_trgm's ISWORDCHR: alphanumeric
# what unicodedata's decomposition leaves alone but unaccent.rules maps (Polish ł first)
_EXTRA = str.maketrans(
    {
        **{"ł": "l", "Ł": "L", "đ": "d", "Đ": "D", "ø": "o", "Ø": "O", "ß": "ss", "æ": "ae", "Æ": "AE"},
        # punctuation unaccent.rules maps too (KM's stop names use en dashes)
        **{"–": "-", "—": "-", "‐": "-", "‑": "-", "‒": "-", "‘": "'", "’": "'", "‚": ",", "“": '"', "”": '"', "„": ",,"},
    }
)


def unaccent_lower(text: str) -> str:
    """core.immutable_unaccent(lower(text)) for the letters stop names use: Polish ones, accents, ß."""
    decomposed = unicodedata.normalize("NFKD", text.lower().translate(_EXTRA))
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def trigrams(text: str) -> list[str]:
    """Trigrams in order of occurrence (repeats kept), as pg_trgm generates them."""
    out = []
    for word in _WORD.findall(text):
        padded = f"  {word} "
        out += [padded[i : i + 3] for i in range(len(padded) - 2)]
    return out


def _calcsml(count: int, len1: int, len2: int) -> np.float32:
    return np.float32(count) / np.float32(len1 + len2 - count)


def similarity(a: str, b: str) -> np.float32:
    """pg_trgm similarity(a, b): shared unique trigrams over all unique trigrams."""
    ta, tb = set(trigrams(a)), set(trigrams(b))
    if not ta or not tb:
        return np.float32(0)
    common = len(ta & tb)
    return _calcsml(common, len(ta), len(tb))


def word_similarity(a: str, b: str) -> np.float32:
    """pg_trgm word_similarity(a, b): how well `a` matches some run of `b`'s trigrams (iterate_word_similarity)."""
    t1, t2 = trigrams(a), trigrams(b)
    found = set(t1)
    ulen1 = len(found)
    if ulen1 == 0 or not t2:
        return np.float32(0)
    lastpos: dict[str, int] = {}
    ulen2 = count = 0
    lower = -1
    smlr_max = np.float32(0)
    for i, trg in enumerate(t2):
        if lower >= 0 or trg in found:
            if lastpos.get(trg, -1) < 0:
                ulen2 += 1
                if trg in found:
                    count += 1
            lastpos[trg] = i
        if trg in found:
            upper = i
            if lower == -1:
                lower = i
                ulen2 = 1
            smlr_cur = _calcsml(count, ulen1, ulen2)
            # also try to move the lower bound for a greater similarity
            tmp_count, tmp_ulen2, prev_lower = count, ulen2, lower
            for tmp_lower in range(lower, upper + 1):
                smlr_tmp = _calcsml(tmp_count, ulen1, tmp_ulen2)
                if smlr_tmp > smlr_cur:
                    smlr_cur, ulen2, lower, count = smlr_tmp, tmp_ulen2, tmp_lower, tmp_count
                tmp_trg = t2[tmp_lower]
                if lastpos.get(tmp_trg, -1) == tmp_lower:
                    tmp_ulen2 -= 1
                    if tmp_trg in found:
                        tmp_count -= 1
            smlr_max = max(smlr_max, smlr_cur)
            for tmp_lower in range(prev_lower, lower):
                tmp_trg = t2[tmp_lower]
                if lastpos.get(tmp_trg, -1) == tmp_lower:
                    lastpos[tmp_trg] = -1
    return smlr_max


def collation_key(text: str) -> tuple:
    """A sort key ordering names as Postgres' en_US.utf8 collation (glibc) does, as far as stop names go: letters and
    digits first, with case, accents, spaces and punctuation ignored; then accents (none first), then case (lower first),
    then the rest."""
    letters = [c for c in text if c.isalnum()]
    base = "".join(unaccent_lower(c) for c in letters)
    accents = tuple(unaccent_lower(c) != c.lower() for c in letters)
    case = tuple(c.isupper() for c in letters)
    return base, accents, case, text
