"""
test_polymarket_client.py — lógica pura do polymarket_client.

Cobre:
- parse_end_time (ISO string → unix ms)
- parse_clob_token_ids (string JSON e list)
- build_polymarket_market com mercados realistas
- process_clob_msg com book/price_change/best_bid_ask
- normalize_polymarket_timestamp
"""
from __future__ import annotations

import json
import sys
import time
import traceback

# Stubs
try:
    import httpx  # noqa
except ImportError:
    sys.modules["httpx"] = type(sys)("httpx")
try:
    import websockets  # noqa
    import websockets.exceptions  # noqa
except ImportError:
    mod = type(sys)("websockets")
    exc = type(sys)("websockets.exceptions")
    class _CC(Exception): ...
    exc.ConnectionClosed = _CC
    mod.exceptions = exc
    sys.modules["websockets"] = mod
    sys.modules["websockets.exceptions"] = exc

from polymarket_client import (
    BookLevel,
    MarketType,
    OutcomeBook,
    build_polymarket_market,
    handle_best_bid_ask_msg,
    handle_book_msg,
    handle_price_change_msg,
    normalize_polymarket_timestamp,
    parse_book_levels,
    parse_clob_token_ids,
    parse_end_time,
    parse_float_safe,
    process_clob_msg,
)


def approx(a, b, tol=1e-6):
    return abs(a - b) <= tol


# ─────────────────────────────────────────────────────
# PARSE END TIME
# ─────────────────────────────────────────────────────

def test_parse_end_time_iso_z() -> None:
    ms = parse_end_time("2026-04-20T21:00:00Z")
    assert ms is not None
    # 2026-04-20 21:00 UTC = 1_776_718_800_000 ms (verificado via datetime.timestamp)
    assert ms == 1_776_718_800_000

def test_parse_end_time_with_offset() -> None:
    ms = parse_end_time("2026-04-20T21:00:00+00:00")
    assert ms == 1_776_718_800_000

# ─────────────────────────────────────────────────────
# FILTRO CRYPTO CANDIDATE (nova discovery via /markets)
# ─────────────────────────────────────────────────────

def test_market_is_crypto_candidate_accepts() -> None:
    from polymarket_client import _market_is_crypto_candidate
    assert _market_is_crypto_candidate({"question": "Will Bitcoin reach $150,000 in April?"})
    assert _market_is_crypto_candidate({"question": "Will the price of Bitcoin be above $66,000 on April 21?"})
    assert _market_is_crypto_candidate({"question": "Will Ethereum fall to $3000?"})
    assert _market_is_crypto_candidate({"question": "Will Solana dip to $100 in April?"})

def test_market_is_crypto_candidate_rejects_no_dollar() -> None:
    from polymarket_client import _market_is_crypto_candidate
    assert not _market_is_crypto_candidate({"question": "Will Bitcoin go up?"})
    assert not _market_is_crypto_candidate({"question": "Bitcoin Up or Down - April 21"})

def test_market_is_crypto_candidate_rejects_non_crypto() -> None:
    from polymarket_client import _market_is_crypto_candidate
    assert not _market_is_crypto_candidate({"question": "Will TSLA be above $300 on April 21?"})
    assert not _market_is_crypto_candidate({"question": "Trump wins $100M lawsuit?"})

def test_market_is_crypto_candidate_empty() -> None:
    from polymarket_client import _market_is_crypto_candidate
    assert not _market_is_crypto_candidate({"question": ""})
    assert not _market_is_crypto_candidate({})


# ─────────────────────────────────────────────────────
# BUILD MARKET VIA /markets (sem event)
# ─────────────────────────────────────────────────────

def test_build_market_from_markets_endpoint() -> None:
    """Chamada com event=None simula /markets direto."""
    m = _make_market_dict(
        question="Will Bitcoin reach $150,000 in April?",
        end_date="2026-04-30T23:59:00Z",
    )
    spots = {"BTCUSDT": 90_000}
    pm = build_polymarket_market(event=None, market=m, spot_by_symbol=spots)
    assert pm is not None
    assert pm.parsed.market_type == MarketType.STRIKE_TOUCH
    assert pm.parsed.strike_primary == 150_000

def test_build_market_prefers_volume24hr() -> None:
    """Se volume24hr existe, usa ele em vez de volume total."""
    m = _make_market_dict(question="Will Bitcoin reach $100k in April?")
    m["volume24hr"] = 5000.0
    m["volume"] = 999999.0
    spots = {"BTCUSDT": 90_000}
    pm = build_polymarket_market(event=None, market=m, spot_by_symbol=spots)
    assert pm is not None
    assert pm.volume_usd == 5000.0


def test_parse_end_time_invalid() -> None:
    assert parse_end_time(None) is None
    assert parse_end_time("") is None
    assert parse_end_time("garbage") is None
    assert parse_end_time(12345) is None


# ─────────────────────────────────────────────────────
# PARSE CLOB TOKEN IDS
# ─────────────────────────────────────────────────────

def test_parse_token_ids_json_string() -> None:
    """Gamma às vezes retorna como string JSON."""
    raw = '["12345", "67890"]'
    out = parse_clob_token_ids(raw)
    assert out == ["12345", "67890"]

def test_parse_token_ids_list() -> None:
    raw = ["111", "222"]
    out = parse_clob_token_ids(raw)
    assert out == ["111", "222"]

def test_parse_token_ids_ints_coerced() -> None:
    """Mesmo com ints, retorna strings."""
    raw = [111, 222]
    out = parse_clob_token_ids(raw)
    assert out == ["111", "222"]

def test_parse_token_ids_invalid() -> None:
    assert parse_clob_token_ids(None) is None
    assert parse_clob_token_ids("not json") is None
    assert parse_clob_token_ids(42) is None


# ─────────────────────────────────────────────────────
# BUILD POLYMARKET MARKET
# ─────────────────────────────────────────────────────

def _make_market_dict(
    mid="mkt1",
    cond_id="0xABC",
    question="Will BTC be above $100k at 5:00 PM ET?",
    tokens=["tok_yes", "tok_no"],
    volume=5000.0,
    end_date="2026-04-20T21:00:00Z",
    enable_ob=True,
    accepting=True,
    closed=False,
    active=True,
) -> dict:
    return {
        "id": mid,
        "conditionId": cond_id,
        "slug": "btc-above-100k",
        "question": question,
        "clobTokenIds": tokens,
        "volume": volume,
        "endDate": end_date,
        "enableOrderBook": enable_ob,
        "acceptingOrders": accepting,
        "closed": closed,
        "active": active,
    }

def test_build_market_happy_path() -> None:
    event = {"id": "ev1", "title": "BTC events", "endDate": "2026-04-20T21:00:00Z"}
    market = _make_market_dict()
    pm = build_polymarket_market(event, market, spot_for_sanity=95_000)
    assert pm is not None
    assert pm.parsed.market_type == MarketType.STRIKE_AT_TIME
    assert pm.parsed.strike_primary == 100_000
    assert pm.parsed.direction_above is True
    assert pm.yes_token_id == "tok_yes"
    assert pm.no_token_id == "tok_no"
    assert pm.volume_usd == 5000.0

def test_build_market_rejects_not_accepting() -> None:
    event = {"id": "ev1"}
    m = _make_market_dict(accepting=False)
    assert build_polymarket_market(event, m, spot_for_sanity=95_000) is None

def test_build_market_rejects_closed() -> None:
    event = {"id": "ev1"}
    m = _make_market_dict(closed=True)
    assert build_polymarket_market(event, m, spot_for_sanity=95_000) is None

def test_build_market_rejects_up_down_implicit() -> None:
    """MVP exclui up/down implícitos."""
    event = {"id": "ev1"}
    m = _make_market_dict(question="Bitcoin Up or Down - April 20, 5PM ET")
    assert build_polymarket_market(event, m, spot_for_sanity=95_000) is None

def test_build_market_accepts_two_barrier() -> None:
    event = {"id": "ev1"}
    m = _make_market_dict(question="Will Bitcoin hit $80k or $100k first?")
    pm = build_polymarket_market(event, m, spot_for_sanity=90_000)
    assert pm is not None
    assert pm.parsed.market_type == MarketType.TWO_BARRIER
    assert pm.parsed.strike_primary == 80_000
    assert pm.parsed.strike_secondary == 100_000

def test_build_market_rejects_parse_low_conf() -> None:
    """Strike absurdo → parse_confidence=0 → rejeita."""
    event = {"id": "ev1"}
    m = _make_market_dict(question="Will BTC be above $2 at 5PM")
    assert build_polymarket_market(event, m, spot_for_sanity=95_000) is None

def test_build_market_rejects_no_tokens() -> None:
    event = {"id": "ev1"}
    m = _make_market_dict(tokens=None)
    assert build_polymarket_market(event, m, spot_for_sanity=95_000) is None

def test_build_market_rejects_no_enddate() -> None:
    event = {"id": "ev1"}
    m = _make_market_dict(end_date=None)
    assert build_polymarket_market(event, m, spot_for_sanity=95_000) is None


# ─────────────────────────────────────────────────────
# NORMALIZE TIMESTAMP
# ─────────────────────────────────────────────────────

def test_normalize_ts_seconds_to_ms() -> None:
    """Polymarket manda em segundos."""
    assert normalize_polymarket_timestamp(1_700_000_000) == 1_700_000_000_000
    assert normalize_polymarket_timestamp("1700000000") == 1_700_000_000_000

def test_normalize_ts_ms_preserved() -> None:
    assert normalize_polymarket_timestamp(1_700_000_000_000) == 1_700_000_000_000

def test_normalize_ts_invalid_falls_back() -> None:
    now = int(time.time() * 1000)
    v = normalize_polymarket_timestamp(None)
    assert abs(v - now) < 1000
    v2 = normalize_polymarket_timestamp("garbage")
    assert abs(v2 - now) < 1000
    v3 = normalize_polymarket_timestamp(0)
    assert abs(v3 - now) < 1000


# ─────────────────────────────────────────────────────
# PARSE BOOK LEVELS
# ─────────────────────────────────────────────────────

def test_parse_book_levels_basic() -> None:
    raw = [{"price": "0.50", "size": "100"}, {"price": "0.48", "size": "200"}]
    out = parse_book_levels(raw)
    assert len(out) == 2
    assert out[0].price == 0.50
    assert out[1].size == 200

def test_parse_book_levels_rejects_zero() -> None:
    raw = [{"price": "0", "size": "100"}, {"price": "0.50", "size": "0"}]
    out = parse_book_levels(raw)
    assert len(out) == 0

def test_parse_book_levels_empty() -> None:
    assert parse_book_levels([]) == []
    assert parse_book_levels(None) == []


# ─────────────────────────────────────────────────────
# HANDLE WS MESSAGES
# ─────────────────────────────────────────────────────

def test_handle_book_msg_replaces() -> None:
    book = OutcomeBook(token_id="tok1")
    # Seed com levels velhos
    book.bids = [BookLevel(0.40, 1.0)]
    book.asks = [BookLevel(0.50, 1.0)]
    msg = {
        "timestamp": "1700000000",
        "bids": [{"price": "0.45", "size": "100"}, {"price": "0.43", "size": "200"}],
        "asks": [{"price": "0.46", "size": "150"}, {"price": "0.48", "size": "50"}],
    }
    handle_book_msg(book, msg)
    assert len(book.bids) == 2
    assert book.bids[0].price == 0.45  # desc
    assert book.asks[0].price == 0.46  # asc
    assert book.last_update_ms == 1_700_000_000_000

def test_handle_price_change_delta() -> None:
    book = OutcomeBook(token_id="tok1")
    book.bids = [BookLevel(0.45, 100.0)]
    book.asks = [BookLevel(0.50, 100.0)]
    msg = {
        "timestamp": "1700000000",
        "changes": [
            {"price": "0.46", "side": "BUY", "size": "50"},      # add novo bid
            {"price": "0.50", "side": "SELL", "size": "0"},      # remove ask
        ],
    }
    handle_price_change_msg(book, msg)
    assert len(book.bids) == 2
    assert book.bids[0].price == 0.46  # novo topo
    assert len(book.asks) == 0

def test_handle_best_bid_ask() -> None:
    book = OutcomeBook(token_id="tok1")
    msg = {"timestamp": "1700000000", "best_bid": "0.45", "best_ask": "0.48"}
    handle_best_bid_ask_msg(book, msg)
    assert book.best_bid == 0.45
    assert book.best_ask == 0.48

def test_process_clob_routes_correctly() -> None:
    book = OutcomeBook(token_id="tok1")
    books = {"tok1": book}
    msg = {
        "event_type": "book",
        "asset_id": "tok1",
        "timestamp": "1700000000",
        "bids": [{"price": "0.45", "size": "100"}],
        "asks": [{"price": "0.50", "size": "100"}],
    }
    process_clob_msg(books, msg)
    assert book.best_bid == 0.45

def test_process_clob_unknown_token_ignored() -> None:
    book = OutcomeBook(token_id="tok1")
    books = {"tok1": book}
    msg = {"event_type": "book", "asset_id": "unknown", "bids": [], "asks": []}
    # Não deve crashar nem afetar book existente
    process_clob_msg(books, msg)
    assert book.best_bid is None


# ─────────────────────────────────────────────────────
# OUTCOME BOOK FRESHNESS
# ─────────────────────────────────────────────────────

def test_book_freshness_check() -> None:
    book = OutcomeBook(token_id="t", last_update_ms=1000)
    assert book.is_fresh(now_ms=10_000)      # 9s atrás, ok (< 30s)
    assert not book.is_fresh(now_ms=50_000)  # 49s atrás, stale


# ─────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────

def run_all() -> int:
    tests = [
        test_market_is_crypto_candidate_accepts,
        test_market_is_crypto_candidate_rejects_no_dollar,
        test_market_is_crypto_candidate_rejects_non_crypto,
        test_market_is_crypto_candidate_empty,
        test_build_market_from_markets_endpoint,
        test_build_market_prefers_volume24hr,
        test_parse_end_time_iso_z,
        test_parse_end_time_with_offset,
        test_parse_end_time_invalid,
        test_parse_token_ids_json_string,
        test_parse_token_ids_list,
        test_parse_token_ids_ints_coerced,
        test_parse_token_ids_invalid,
        test_build_market_happy_path,
        test_build_market_rejects_not_accepting,
        test_build_market_rejects_closed,
        test_build_market_rejects_up_down_implicit,
        test_build_market_accepts_two_barrier,
        test_build_market_rejects_parse_low_conf,
        test_build_market_rejects_no_tokens,
        test_build_market_rejects_no_enddate,
        test_normalize_ts_seconds_to_ms,
        test_normalize_ts_ms_preserved,
        test_normalize_ts_invalid_falls_back,
        test_parse_book_levels_basic,
        test_parse_book_levels_rejects_zero,
        test_parse_book_levels_empty,
        test_handle_book_msg_replaces,
        test_handle_price_change_delta,
        test_handle_best_bid_ask,
        test_process_clob_routes_correctly,
        test_process_clob_unknown_token_ignored,
        test_book_freshness_check,
    ]
    passed = 0
    failed = []
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failed.append(t.__name__)
            print(f"  FAIL  {t.__name__}: {e}")
        except Exception as e:
            failed.append(t.__name__)
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print(f"\n{passed}/{len(tests)} passou, {len(failed)} falhou")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(run_all())
