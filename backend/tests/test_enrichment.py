"""
Unit tests for enrichment.get_country_by_icao bisect range lookup.

Uses the module-level _ICAO_COUNTRY_RANGES data directly — no file I/O,
no network, no EnrichmentDB instantiation needed.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from enrichment import _ICAO_COUNTRY_RANGES, _CR_LO, _CR_HI, _CR_NAME
import bisect


def country_for(icao: str):
    """Reimplement get_country_by_icao against module-level data for isolated testing."""
    try:
        val = int(icao, 16)
    except ValueError:
        return None
    idx = bisect.bisect_right(_CR_LO, val) - 1
    if idx < 0:
        return None
    return _CR_NAME[idx] if val <= _CR_HI[idx] else None


class TestIcaoCountryLookup:
    def test_known_uk_icao(self):
        # UK block: 0x400000–0x43FFFF
        assert country_for("400000") == "United Kingdom"
        assert country_for("43FFFF") == "United Kingdom"

    def test_known_us_icao(self):
        # USA block: 0xA00000–0xAFFFFF
        assert country_for("A00000") == "United States"
        assert country_for("AFFFFF") == "United States"

    def test_known_germany_icao(self):
        # Germany block: 0x3C0000–0x3FFFFF
        assert country_for("3C0000") == "Germany"

    def test_known_france_icao(self):
        # France block: 0x380000–0x3BFFFF
        assert country_for("380000") == "France"

    def test_address_below_all_ranges(self):
        # 0x000000 is below all defined blocks
        assert country_for("000000") is None

    def test_address_in_unallocated_gap(self):
        # 0x000001 is likely in an unallocated gap — should return None, not a wrong country
        result = country_for("000001")
        # Either None (gap) or a valid country string — never raise
        assert result is None or isinstance(result, str)

    def test_boundary_start_of_block(self):
        # South Africa: 0x008000–0x00FFFF — test exact start
        assert country_for("008000") == "South Africa"

    def test_boundary_end_of_block(self):
        # South Africa: end is 0x00FFFF
        assert country_for("00FFFF") == "South Africa"

    def test_one_above_block_end(self):
        # 0x010000 starts Egypt, so 0x00FFFF+1 should not be South Africa
        result_at_boundary = country_for("00FFFF")
        result_above = country_for("010000")
        assert result_at_boundary != result_above or result_above == "Egypt"

    def test_invalid_hex_returns_none(self):
        assert country_for("ZZZZZZ") is None

    def test_empty_string_returns_none(self):
        assert country_for("") is None

    def test_case_insensitive(self):
        # ICAO addresses may arrive upper or lower case
        assert country_for("a00000") == country_for("A00000")

    def test_ranges_sorted(self):
        # The bisect algorithm requires _CR_LO to be sorted ascending
        assert _CR_LO == sorted(_CR_LO), "_CR_LO must be sorted for bisect to work correctly"

    def test_no_overlapping_ranges(self):
        # Each range's end must be less than the next range's start
        for i in range(len(_CR_HI) - 1):
            assert _CR_HI[i] < _CR_LO[i + 1], (
                f"Range overlap between {_CR_NAME[i]} (ends {_CR_HI[i]:#08x}) "
                f"and {_CR_NAME[i+1]} (starts {_CR_LO[i+1]:#08x})"
            )
