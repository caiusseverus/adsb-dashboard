import sys
import os
import types
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import cast


class TestCastRuleMatches:
    def test_watchlist_rule_uses_watchlist_cache_lookup(self, monkeypatch):
        monkeypatch.setattr(cast, "_watchlist_cache_ts", 0.0)
        monkeypatch.setattr(
            cast,
            "_get_watchlist",
            lambda: {"ABC123", "DEF456"},
        )
        rule = {"enabled": 1, "match_type": "watchlist"}

        assert cast._rule_matches(rule, {"icao": "ABC123"}) is True
        assert cast._rule_matches(rule, {"icao": "ZZZ999"}) is False


class TestTokenStore:
    def setup_method(self):
        cast._tokens.clear()

    def teardown_method(self):
        cast._tokens.clear()

    def test_consume_token_removes_entry(self):
        token = cast._store_token({"icao": "ABC123"})

        first = cast.consume_token(token)
        second = cast.consume_token(token)

        assert first == {"icao": "ABC123"}
        assert second is None

    def test_expire_tokens_prunes_expired_entries(self, monkeypatch):
        cast._tokens["expired"] = ({"icao": "OLD111"}, 10.0)
        cast._tokens["fresh"] = ({"icao": "NEW222"}, 200.0)
        monkeypatch.setattr(cast.time, "monotonic", lambda: 100.0)

        cast._expire_tokens()

        assert "expired" not in cast._tokens
        assert "fresh" in cast._tokens


class TestChromecastCacheLifecycle:
    def teardown_method(self):
        cast._cc_cache = {"device_name": None, "cc": None, "browser": None, "ts": 0.0}

    def test_reset_config_cache_disconnects_before_stopping_discovery(self, monkeypatch):
        events = []

        class FakeChromecast:
            def disconnect(self):
                events.append("disconnect")

        fake_browser = object()
        fake_pychromecast = types.SimpleNamespace(
            discovery=types.SimpleNamespace(
                stop_discovery=lambda browser: events.append(("stop", browser))
            )
        )
        monkeypatch.setitem(sys.modules, "pychromecast", fake_pychromecast)
        cast._cc_cache = {
            "device_name": "Kitchen display",
            "cc": FakeChromecast(),
            "browser": fake_browser,
            "ts": 123.0,
        }

        cast.reset_config_cache()

        assert events == ["disconnect", ("stop", fake_browser)]
        assert cast._cc_cache == {"device_name": None, "cc": None, "browser": None, "ts": 0.0}

    def test_get_chromecast_clears_stale_cache_before_rediscovery(self, monkeypatch):
        events = []

        class OldChromecast:
            def disconnect(self):
                events.append("disconnect-old")

        class NewChromecast:
            def wait(self):
                events.append("wait-new")

        fake_browser_old = object()
        fake_browser_new = object()

        def stop_discovery(browser):
            events.append(("stop", browser))

        def get_listed_chromecasts(*, friendly_names, timeout):
            events.append(("discover", tuple(friendly_names), timeout))
            return [NewChromecast()], fake_browser_new

        fake_pychromecast = types.SimpleNamespace(
            discovery=types.SimpleNamespace(stop_discovery=stop_discovery),
            get_listed_chromecasts=get_listed_chromecasts,
        )
        monkeypatch.setitem(sys.modules, "pychromecast", fake_pychromecast)
        monkeypatch.setattr(cast.time, "monotonic", lambda: 500.0)
        cast._cc_cache = {
            "device_name": "Old display",
            "cc": OldChromecast(),
            "browser": fake_browser_old,
            "ts": 0.0,
        }

        cc = cast._get_chromecast("Kitchen display")

        assert isinstance(cc, NewChromecast)
        assert events == [
            "disconnect-old",
            ("stop", fake_browser_old),
            ("discover", ("Kitchen display",), 10),
            "wait-new",
        ]
        assert cast._cc_cache == {
            "device_name": "Kitchen display",
            "cc": cc,
            "browser": fake_browser_new,
            "ts": 500.0,
        }
