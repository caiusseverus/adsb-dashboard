"""
Unit tests for config._parse_mlat_servers.

Tests all format branches and error handling without touching env vars.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import logging
import pytest
from config import _parse_mlat_servers


DEFAULT_HOST = "myreceiver"


class TestParseMlatServers:
    def test_empty_string(self):
        assert _parse_mlat_servers("", DEFAULT_HOST) == []

    def test_whitespace_only(self):
        assert _parse_mlat_servers("   ", DEFAULT_HOST) == []

    def test_explicit_host_format(self):
        # Name@host:port
        result = _parse_mlat_servers("ADSBx@adsbpi:30158", DEFAULT_HOST)
        assert result == [("ADSBx", "adsbpi", 30158)]

    def test_default_host_format(self):
        # Name:port — host defaults to DEFAULT_HOST
        result = _parse_mlat_servers("FlightAware:30105", DEFAULT_HOST)
        assert result == [("FlightAware", DEFAULT_HOST, 30105)]

    def test_multiple_entries_mixed(self):
        val = "ADSBx@adsbpi:30158,FlightAware:30105,Airplanes:30157"
        result = _parse_mlat_servers(val, DEFAULT_HOST)
        assert result == [
            ("ADSBx", "adsbpi", 30158),
            ("FlightAware", DEFAULT_HOST, 30105),
            ("Airplanes", DEFAULT_HOST, 30157),
        ]

    def test_whitespace_around_entries(self):
        result = _parse_mlat_servers("  MyFeed : 30200  ", DEFAULT_HOST)
        assert result == [("MyFeed", DEFAULT_HOST, 30200)]

    def test_malformed_no_port_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = _parse_mlat_servers("BadEntry", DEFAULT_HOST)
        assert result == []
        assert "malformed" in caplog.text.lower() or "skipping" in caplog.text.lower()

    def test_malformed_non_integer_port_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = _parse_mlat_servers("Feed:notaport", DEFAULT_HOST)
        assert result == []

    def test_malformed_entry_skipped_valid_entry_kept(self, caplog):
        # One bad entry should not prevent a good one from being parsed
        with caplog.at_level(logging.WARNING):
            result = _parse_mlat_servers("BadEntry,Good:30105", DEFAULT_HOST)
        assert len(result) == 1
        assert result[0] == ("Good", DEFAULT_HOST, 30105)

    def test_trailing_comma_ignored(self):
        result = _parse_mlat_servers("Feed:30105,", DEFAULT_HOST)
        assert result == [("Feed", DEFAULT_HOST, 30105)]

    def test_name_with_spaces_trimmed(self):
        result = _parse_mlat_servers("  My Feed  :30105", DEFAULT_HOST)
        assert result[0][0] == "My Feed"

    def test_explicit_host_with_ip_address(self):
        result = _parse_mlat_servers("Local@192.168.1.10:30158", DEFAULT_HOST)
        assert result == [("Local", "192.168.1.10", 30158)]
