import re

PREFIX_UNIT_PATTERNS = [
    re.compile(r"^\s*(\d+)\s*[-–]\s*(\d+\s+.+)$"),
    re.compile(r"^\s*([A-Za-z]\w*)\s*[-–]\s*(\d+.+)$"),
    re.compile(r"^\s*(\w+)\s*/\s*(\d+.+)$"),
    re.compile(
        r"^(?:apt|unit|suite|#)\s*(\w+)[,\s]+(\d+.+)$",
        re.IGNORECASE,
    ),
]

SUFFIX_UNIT_PATTERNS = [
    re.compile(r"^(\d+.+?)[,\s]+\s*#\s*(\w+(?:[-–]\w+)?)$"),
    re.compile(
        r"^(\d+.+?)[,\s]+\s*(?:apt|unit|suite)\.?\s*(\w+(?:[-–]\w+)?)$",
        re.IGNORECASE,
    ),
]
COMPLEX_PATTERN = re.compile(
    r"\w+\s*[-–]\s*\w+\s*[-–]\s*\d+",
    re.IGNORECASE,
)

NAMED_UNIT_PATTERN = re.compile(
    r"^(dock|shop|unit|lot)\s+\w+[-–]\d+",
    re.IGNORECASE,
)


def split_unit_and_street(address):
    """Split a free-form address into (street, unit, status)."""
    if not address:
        return "", "", "UNSPLIT"

    text = str(address).strip()

    if COMPLEX_PATTERN.search(text):
        return text, "", "COMPLEX_MULTI_LEVEL"

    if NAMED_UNIT_PATTERN.match(text):
        return text, "", "NAMED_UNIT"

    for pattern in PREFIX_UNIT_PATTERNS:
        match = pattern.match(text)
        if match:
            unit, street = match.groups()
            return street.strip(), unit.strip(), "OK_UNIT_STREET"

    for pattern in SUFFIX_UNIT_PATTERNS:
        match = pattern.match(text)
        if match:
            street, unit = match.groups()
            return street.strip(), unit.strip(), "OK_UNIT_STREET"

    return text, "", "UNSPLIT"
