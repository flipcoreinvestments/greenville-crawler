"""
NAME ORDER (added 2026-10-13, T Dawg's ask): every first name in a skip-trace
upload must be a real first name and every last name a real last name, no
matter which source wrote the owner's name or in what order.

Which source writes names in which order:

    source (script)                               owner name order
    --------------------------------------------  -----------------------------
    County parcel map (absentee_owner_ingest)      LAST FIRST MIDDLE   "HOOD LINDA M"
    County tax sale list (tax_sale_ingest,
      redemption_period_ingest)                    LAST FIRST MIDDLE   (mostly --
                                                   some entries are typed
                                                   FIRST LAST: "LINDA HOOD")
    PropStream export (propstream_import)          converted to LAST FIRST from
                                                   its own First/Last columns
    Probate court (probate_ingest)                 LAST , FIRST MIDDLE "HOOD , LINDA M"
    Foreclosure auction notices
      (foreclosure_mie_ingest), building permits,
      code enforcement                             as typed -- either order

The county parcel map overwrites the owner name on every lead it covers, so
LAST FIRST is the default everywhere. Names with a comma ("LAST , FIRST") are
read as written. For everything else, decide_order() double-checks the order
with how common each word is as a US first name vs. a US last name
(data/us_name_ranks.json, from the open-source names-dataset; license in
data/us_name_ranks.LICENSE), plus the
middle-initial position:

    "HOOD LINDA"      Hood = surname, Linda = first name  -> keep (LAST FIRST)
    "LINDA HOOD"      Linda = first name, Hood = surname  -> flip (FIRST LAST)
    "JOHN A SMITH"    initial in the middle               -> FIRST M LAST
    "SMITH JOHN A"    initial at the end                  -> LAST FIRST M
    "JAMES HENRY"     both work as first names            -> keep the county order

A name is only flipped when the evidence is strong both ways (the first word
is clearly a first name AND the last word is clearly a surname), so
surname-first county names like "TAYLOR COLUMBUS" or "GRANT KEVIN" stay put.
"""
import json
import math
import os

_DATA = os.path.join(os.path.dirname(__file__), "data", "us_name_ranks.json")
_UNKNOWN = 25000          # rank used for a word that isn't in the list
_FLIP_MARGIN = 3.0        # log-odds gap needed to flip (about 20x)

_ranks = None


def _load():
    global _ranks
    if _ranks is None:
        try:
            with open(_DATA) as f:
                d = json.load(f)
            _ranks = (d["first"], d["last"])
        except (OSError, ValueError, KeyError):
            _ranks = ({}, {})
    return _ranks


def firstness(word):
    """> 0: more common as a first name; < 0: more common as a surname."""
    first, last = _load()
    w = (word or "").upper().replace("'", "").replace("-", "")
    f, l = first.get(w, _UNKNOWN), last.get(w, _UNKNOWN)
    return math.log(l / f)


def is_initial(word):
    return len((word or "").strip(".")) == 1


def decide_order(tokens):
    """tokens: a person's name words in the order the source wrote them,
    suffixes (JR/SR/III) already removed. Returns (first, last)."""
    toks = [t for t in tokens if t]
    if not toks:
        return "", ""
    if len(toks) == 1:
        return "", toks[0]
    # middle-initial position is the strongest signal there is
    if len(toks) == 3 and is_initial(toks[1]) and not is_initial(toks[2]):
        return toks[0], toks[2]                  # FIRST M LAST
    if is_initial(toks[-1]):
        return toks[1], toks[0]                  # LAST FIRST M
    a, z = toks[0], toks[-1]
    fa, fz = firstness(a), firstness(z)
    if fa > 1.0 and fz < 0 and (fa - fz) >= _FLIP_MARGIN:
        return a, z                              # written FIRST ... LAST
    return toks[1], toks[0]                      # county order LAST FIRST ...
