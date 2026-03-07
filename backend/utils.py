"""Shared utilities."""

# Registration prefix → country.  Two-char prefixes checked before one-char.
# Ordered: longer prefixes first so we try VH before V, etc.
_REG_PREFIXES: dict[str, str] = {
    # 2-char
    'VH': 'Australia',      'ZK': 'New Zealand',    'ZS': 'South Africa',
    'HB': 'Switzerland',    'OE': 'Austria',        'PH': 'Netherlands',
    'SE': 'Sweden',         'OH': 'Finland',        'LN': 'Norway',
    'OY': 'Denmark',        'SP': 'Poland',         'OK': 'Czech Republic',
    'HA': 'Hungary',        'YR': 'Romania',        'LZ': 'Bulgaria',
    'SX': 'Greece',         'CS': 'Portugal',       'EC': 'Spain',
    'EI': 'Ireland',        'EW': 'Belarus',        'ES': 'Estonia',
    'YL': 'Latvia',         'LY': 'Lithuania',      'LX': 'Luxembourg',
    'OO': 'Belgium',        'TC': 'Turkey',         'UR': 'Ukraine',
    'RA': 'Russia',         'UN': 'Kazakhstan',     'EK': 'Armenia',
    'HS': 'Thailand',       'VT': 'India',          'JA': 'Japan',
    'HL': 'South Korea',    'A6': 'United Arab Emirates',
    'A7': 'Qatar',          'HZ': 'Saudi Arabia',   'SU': 'Egypt',
    'PP': 'Brazil',         'PT': 'Brazil',         'PR': 'Brazil',
    'PS': 'Brazil',         'LV': 'Argentina',      'CC': 'Chile',
    'YV': 'Venezuela',      'CP': 'Bolivia',        'HC': 'Ecuador',
    'OB': 'Peru',           'ZP': 'Paraguay',       'CX': 'Uruguay',
    'HP': 'Panama',         'TI': 'Costa Rica',     'HR': 'Honduras',
    'TG': 'Guatemala',      'YS': 'El Salvador',    'TI': 'Costa Rica',
    'XA': 'Mexico',         'XB': 'Mexico',         'XC': 'Mexico',
    '4X': 'Israel',         '5B': 'Cyprus',         '9H': 'Malta',
    'MM': 'Mexico',         'VP': None,             'VQ': None,
    # 1-char (checked only when 2-char not matched)
    'G':  'United Kingdom', 'D':  'Germany',        'F':  'France',
    'I':  'Italy',          'B':  'China',          'C':  'Canada',
    'M':  'Isle of Man',
}


def country_from_registration(reg: str | None) -> str | None:
    """Derive country from registration prefix (more reliable than ICAO block for GA).
    E.g. G-CIMB → United Kingdom, N12345 → United States."""
    if not reg:
        return None
    reg = reg.upper().strip()
    # US: all N-prefix registrations
    if reg.startswith('N') and len(reg) > 1 and reg[1:2].isalnum():
        return 'United States'
    if '-' in reg:
        prefix, suffix = reg.split('-', 1)
        # Single-letter prefix countries (G, D, F, I, etc.) require a purely alphabetic
        # suffix — e.g. G-CIMB is valid UK, but G-781 is not a real British registration.
        if len(prefix) == 1 and not suffix.isalpha():
            return None
        return _REG_PREFIXES.get(prefix) or _REG_PREFIXES.get(prefix[:1])
    # No hyphen — try first 2 chars, then 1 char
    prefix = reg[:2]
    return _REG_PREFIXES.get(prefix) or _REG_PREFIXES.get(prefix[:1])

_ACRONYMS = frozenset({
    # Military
    'RAF', 'USAF', 'USMC', 'USN', 'RAAF', 'RNZAF', 'RCAF', 'IAF', 'FAA',
    'NATO', 'AAC', 'RN', 'RM',
    # Legal suffixes
    'PLC', 'LLC', 'LTD', 'INC', 'GMBH', 'AG', 'SA', 'SAS', 'NV', 'BV', 'AB',
    # Country codes
    'UK', 'US', 'USA', 'GB', 'EU',
    # Airlines/orgs
    'KLM', 'LOT', 'TAM', 'TAP', 'LAM',
})


def format_operator(name: str | None) -> str | None:
    """Normalise ALL-CAPS operator names to title case, preserving known acronyms."""
    if not name:
        return name
    if name != name.upper():
        return name  # already mixed case — leave as-is
    return ' '.join(
        word if word in _ACRONYMS else word[0] + word[1:].lower()
        for word in name.split()
    )
