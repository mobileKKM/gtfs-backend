"""Comparing stop names across feeds (abbreviations, typos, a 'Kraków' prefix, platform numbers)."""

from __future__ import annotations

from difflib import SequenceMatcher

from gtfs_backend.ids import slugify

# common abbreviations in Kraków-area stop names ("bp. Prandoty" == "Biskupa Prandoty"); None = filler word
ABBREVIATIONS = {
    "bp": "biskupa",
    "ks": "ksiedza",
    "os": "osiedle",
    "al": "aleja",
    "pl": "plac",
    "sw": "swietego",
    "gen": "generala",
    "ul": "ulica",
    "im": None,
    "nr": None,
}


def name_tokens(name: str) -> tuple[set[str], str | None]:
    """Tokens of the slugged name without a leading 'krakow' and without a trailing platform number."""
    tokens = slugify(name).split("-")
    number = tokens[-1] if len(tokens) > 1 and tokens[-1].isdigit() and len(tokens[-1]) <= 2 else None
    if number:
        tokens = tokens[:-1]
    out = set()
    for t in tokens:
        if t == "krakow":
            continue
        t = ABBREVIATIONS.get(t, t)
        if t:
            out.add(t)
    return out, number


def names_match(a: str, b: str) -> str | None:
    """'exact', 'subset' (one name extends the other, e.g. 'Pleszów Pętla'), 'similar' (typos) or None."""
    ta, tb = name_tokens(a)[0], name_tokens(b)[0]
    if not ta or not tb:
        return None
    if ta == tb:
        return "exact"
    if ta <= tb or tb <= ta:
        return "subset"
    # a typo in one word ('Rutki'/'Rudki'); the shared words (often the town) don't count
    da, db = ta - tb, tb - ta
    if len(da) == len(db) == 1 and SequenceMatcher(None, da.pop(), db.pop()).ratio() >= 0.8:
        return "similar"
    return None
