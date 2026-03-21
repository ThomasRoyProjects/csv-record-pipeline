import re

DIRECTION_MAP = {
    "n": "north",
    "s": "south",
    "e": "east",
    "w": "west",
    "ne": "northeast",
    "nw": "northwest",
    "se": "southeast",
    "sw": "southwest",
}

def _clean(val):
    if val is None:
        return ""
    return str(val).strip()

def _norm(val):
    v = _clean(val).lower()
    v = re.sub(r"[,\./]", " ", v)
    v = re.sub(r"\s+", " ", v)
    return v

def normalize_address_parts(*parts):
    """
    Generic, lossless address normalization.
    Accepts arbitrary address components and returns
    a single comparable string.
    """
    tokens = []

    for part in parts:
        if not part:
            continue

        v = _norm(part)

        if v in DIRECTION_MAP:
            v = DIRECTION_MAP[v]

        if v:
            tokens.append(v)

    return " ".join(tokens)
