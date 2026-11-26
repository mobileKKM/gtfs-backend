"""gtfs_backend/trigram.py gives pg_trgm's values exactly (float4), checked against Postgres on real stop names."""

import csv
import io
import random
import zipfile

import numpy as np
import psycopg
import pytest
from conftest import FIXTURES

from gtfs_backend import trigram


def stop_names() -> list[str]:
    names = set()
    for zip_name in ("GTFS_KRK_A.zip", "GTFS_KRK_M.zip", "GTFS_KRK_T.zip"):
        with zipfile.ZipFile(FIXTURES / "static" / zip_name) as zf:
            rows = csv.DictReader(io.TextIOWrapper(zf.open("stops.txt"), encoding="utf-8-sig"))
            names |= {r["stop_name"] for r in rows if r["stop_name"]}
    return sorted(names)


def queries(names: list[str], rng: random.Random) -> list[tuple[str, str]]:
    """(query, the name it was made from): what people type: prefixes, a word, a typo, words swapped, a piece."""
    out = []
    for n in rng.sample(names, min(300, len(names))):
        words = n.split()
        made = [n[: rng.randint(1, max(1, len(n)))], words[-1], n[: max(1, len(n) - 1)]]
        if len(n) > 4:
            i = rng.randrange(len(n) - 1)
            made += [n[:i] + n[i + 1 :], n[:i] + n[i + 1] + n[i] + n[i + 2 :]]  # a letter missing, two swapped
        if len(words) > 1:
            made.append(" ".join(reversed(words)))
        a = rng.randrange(len(n))
        made.append(n[a : a + rng.randint(2, 8)])
        out += [(q, n) for q in made if q.strip()]
    return out


@pytest.fixture(scope="module")
def pg(database_url):
    with psycopg.connect(database_url) as conn:
        yield conn


def test_unaccent_matches_the_database(pg):
    names = stop_names() + ["Łódź Kaliska", "ŻÓŁTA Ćma", "Słoneczna Ń", "Grünwald", "Straße", "Tarnów Krakowska – Planty"]
    names += ["a — b", "„Bagatela”", "‚x‘", "‘x’ “y”", "a‐b‑c‒d"]
    rows = pg.execute("SELECT n, core.immutable_unaccent(lower(n)) FROM unnest(%s::text[]) AS n", (names,)).fetchall()
    assert [(n, trigram.unaccent_lower(n)) for n, _ in rows] == rows


def test_similarity_and_word_similarity_match_pg_trgm(pg):
    rng = random.Random(7)
    names = [trigram.unaccent_lower(n) for n in stop_names()]
    made = queries(names, rng)
    pairs = made + [(q, rng.choice(names)) for q, _ in made]  # with the name it was made from (high scores), and any
    pairs += [("word", "two words"), ("word", "xx wor ord"), ("ab cd", "ab xx cd"), ("abc", "xabcx"), ("a", "a"), ("", "x")]
    qs, ns = [q for q, _ in pairs], [n for _, n in pairs]
    rows = pg.execute(
        "SELECT word_similarity(q, n), similarity(q, n), q <%% n FROM unnest(%s::text[], %s::text[]) AS t (q, n)", (qs, ns)
    ).fetchall()
    mismatches = []
    for (q, n), (ws, sim, within) in zip(pairs, rows, strict=True):
        mine_ws, mine_sim = trigram.word_similarity(q, n), trigram.similarity(q, n)
        if mine_ws != np.float32(ws) or mine_sim != np.float32(sim) or (mine_ws >= trigram.WORD_SIMILARITY_THRESHOLD) != within:
            mismatches.append((q, n, ws, float(mine_ws), sim, float(mine_sim)))
    assert len(pairs) > 2000
    assert mismatches == []


def test_names_sort_as_the_databases_collation(pg):
    names = stop_names() + ["Kurdwanów P+R", "Kurdwanów Południe", "Kurdwanow Pol", "kurdwanów a", "Kurdwanów-B", "Kurdwanów 2"]
    theirs = [n for (n,) in pg.execute('SELECT n FROM unnest(%s::text[]) AS n ORDER BY n COLLATE "en_US.utf8"', (names,))]
    assert sorted(names, key=trigram.collation_key) == theirs
