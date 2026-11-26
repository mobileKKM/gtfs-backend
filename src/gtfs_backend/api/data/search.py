"""Stop search in memory, ranked as the SQL it replaces (services/stops.py history):

    WHERE (q <% name_search OR name_search LIKE '%q%')
    ORDER BY name_search LIKE 'q%' DESC, name_search LIKE '%q%' DESC, word_similarity(q, name_search) DESC,
             similarity(q, name_search) DESC, name

with pg_trgm's scores reproduced exactly (gtfs_backend/trigram.py). Only stop groups of the searchable namespaces that
are active and have a served stop are found. Candidates for `<%` come from an inverted trigram index (a group needs at
least ceil(0.6 x the query's trigrams) of them in common to reach the threshold), and scores are computed tier by
tier (prefix, substring, the rest) only until the page is full.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict

from gtfs_backend.trigram import (
    WORD_SIMILARITY_THRESHOLD,
    collation_key,
    similarity,
    trigrams,
    unaccent_lower,
    word_similarity,
)


class StopSearch:
    def __init__(self, groups: list[tuple[str, str]]):
        """`groups`: (group id, name) of the searchable stop groups."""
        self._ids = [gid for gid, _ in groups]
        self._names = [collation_key(name) for _, name in groups]
        self._search = [unaccent_lower(name) for _, name in groups]
        self._postings: dict[str, list[int]] = defaultdict(list)
        for i, text in enumerate(self._search):
            for trg in set(trigrams(text)):
                self._postings[trg].append(i)

    def __len__(self) -> int:
        return len(self._ids)

    def search(self, query: str, limit: int) -> list[str]:
        q = unaccent_lower(query.strip())
        if not q:
            return []
        prefix, substring = [], []
        for i, text in enumerate(self._search):
            if q in text:
                (prefix if text.startswith(q) else substring).append(i)
        fuzzy = []
        q_trigrams = set(trigrams(q))
        if q_trigrams:
            need = math.ceil(float(WORD_SIMILARITY_THRESHOLD) * len(q_trigrams) - 1e-6)
            shared = Counter(i for trg in q_trigrams for i in self._postings.get(trg, ()))
            matched = set(prefix) | set(substring)
            fuzzy = [
                i
                for i, n in shared.items()
                if n >= need and i not in matched and word_similarity(q, self._search[i]) >= WORD_SIMILARITY_THRESHOLD
            ]
        out: list[str] = []
        for tier in (prefix, substring, fuzzy):
            if len(out) >= limit:
                break
            ranked = sorted(
                tier,
                key=lambda i: (
                    -word_similarity(q, self._search[i]),
                    -similarity(q, self._search[i]),
                    self._names[i],  # ORDER BY g.name, in the database's collation
                    self._ids[i],
                ),
            )
            out += [self._ids[i] for i in ranked[: limit - len(out)]]
        return out
