import sys
import os
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
