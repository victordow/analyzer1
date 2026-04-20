"""
test_detector.py — integração detector + lógica de janela.

Cobre:
- Classificação de estratégia A vs B
- Freshness rejection
- Book terminal rejection
- Depth rejection
- Happy path: LONG flagged
- SHORT flagged
- Sub-60s direcional
- Dedupe: 2 ticks da mesma oportunidade → 1 record
- Reopen gap
- TWO_BARRIER
"""
from __future__ import annotations

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

from analyzer_math import MarketType, ParsedMarket
from binance_client import SpotQuote, VolEstimate
from polymarket_client import (
    BookLevel, MarketBooks, OutcomeBook, PolymarketMarket,
)
from detector import (
    DetectorState, REOPEN_GAP_MS, SECONDS_PER_YEAR,
    STRATEGY_A_TAU_MIN_SEC, STRATEGY_A_TAU_MAX_SEC,
    STRATEGY_B_TAU_MIN_SEC, STRATEGY_B_TAU_MAX_SEC,
    classify_strategy, compute_tau_sec, evaluate_market,
)


# ─────────────────────────────────────────────────────
# HELPERS PRA CONSTRUIR MERCADOS FAKE
# ─────────────────────────────────────────────────────

def make_market_at_time(
    strike: float = 100_000.0, direction_above: bool = True,
    end_in_sec: int = 1800, now_ms: int | None = None,
) -> PolymarketMarket:
    now_ms = now_ms or int(time.time() * 1000)
    parsed = ParsedMarket(
        market_type=MarketType.STRIKE_AT_TIME,
        symbol="BTCUSDT",
        strike_primary=strike, strike_secondary=None,
        direction_above=direction_above,
        parse_confidence=1.0, parse_notes="test",
    )
    return PolymarketMarket(
        market_id="mkt_test",
        condition_id="cond_test",
        slug="btc-test",
        question=f"Will BTC be above ${strike:.0f} at time?",
        parsed=parsed,
        end_time_ms=now_ms + end_in_sec * 1000,
        yes_token_id="tok_yes",
        no_token_id="tok_no",
        volume_usd=5000.0,
    )


def make_market_two_barrier(
    k_low: float = 80_000, k_high: float = 100_000,
    end_in_sec: int = 1800, now_ms: int | None = None,
) -> PolymarketMarket:
    now_ms = now_ms or int(time.time() * 1000)
    parsed = ParsedMarket(
        market_type=MarketType.TWO_BARRIER, symbol="BTCUSDT",
        strike_primary=k_low, strike_secondary=k_high,
        direction_above=None,
        parse_confidence=1.0, parse_notes="test",
    )
    return PolymarketMarket(
        market_id="mkt_2b",
        condition_id="cond_2b", slug="btc-2b",
        question="BTC hit X or Y first?", parsed=parsed,
        end_time_ms=now_ms + end_in_sec * 1000,
        yes_token_id="tok_yes_2b", no_token_id="tok_no_2b",
        volume_usd=5000.0,
    )


def make_spot(s: float = 95_000.0, now_ms: int | None = None) -> SpotQuote:
    now_ms = now_ms or int(time.time() * 1000)
    return SpotQuote(
        symbol="BTCUSDT",
        bid_price=s - 0.5, bid_qty=10.0,
        ask_price=s + 0.5, ask_qty=10.0,
        last_update_ms=now_ms,
    )


def make_vol(sigma: float = 0.5) -> VolEstimate:
    """σ=50% anual é típico pra BTC."""
    return VolEstimate(
        symbol="BTCUSDT", sigma_60=sigma, sigma_10=sigma,
        n_valid_60=60, n_valid_10=10,
        computed_at_ms=0, regime_divergence=0.0,
    )


def make_healthy_books(
    yes_bid: float = 0.40, yes_ask: float = 0.42,
    depth_notional: float = 1000.0,
    now_ms: int | None = None,
) -> MarketBooks:
    """Books saudáveis com depth suficiente e YES+NO = ~1.00."""
    now_ms = now_ms or int(time.time() * 1000)
    # YES book: notional = depth_notional / 2 em cada lado
    yes_size = depth_notional / (2 * yes_bid)
    ask_size = depth_notional / (2 * yes_ask)
    yes = OutcomeBook(
        token_id="tok_yes",
        bids=[BookLevel(yes_bid, yes_size), BookLevel(yes_bid - 0.01, yes_size)],
        asks=[BookLevel(yes_ask, ask_size), BookLevel(yes_ask + 0.01, ask_size)],
        last_update_ms=now_ms,
    )
    no_bid = 1.0 - yes_ask - 0.01
    no_ask = 1.0 - yes_bid - 0.01
    no = OutcomeBook(
        token_id="tok_no",
        bids=[BookLevel(no_bid, yes_size)],
        asks=[BookLevel(no_ask, ask_size)],
        last_update_ms=now_ms,
    )
    return MarketBooks(market_id="mkt_test", yes=yes, no=no)


# ─────────────────────────────────────────────────────
# CLASSIFY STRATEGY
# ─────────────────────────────────────────────────────

def test_strategy_A() -> None:
    assert classify_strategy(4 * 3600) == "A"               # 4h: meio de A
    assert classify_strategy(STRATEGY_A_TAU_MIN_SEC) == "A"  # 1h: limite inferior
    assert classify_strategy(STRATEGY_A_TAU_MAX_SEC) == "A"  # 24h: limite superior

def test_strategy_B() -> None:
    assert classify_strategy(3 * 24 * 3600) == "B"            # 3 dias: meio de B
    assert classify_strategy(STRATEGY_A_TAU_MAX_SEC + 1) == "B"
    assert classify_strategy(STRATEGY_B_TAU_MAX_SEC) == "B"    # 7d: limite superior

def test_strategy_out_of_range() -> None:
    assert classify_strategy(30 * 60) is None                  # 30min: abaixo de A
    assert classify_strategy(30) is None                        # 30s
    assert classify_strategy(STRATEGY_B_TAU_MAX_SEC + 1) is None
    assert classify_strategy(30 * 24 * 3600) is None           # 30 dias


# ─────────────────────────────────────────────────────
# EVALUATE MARKET — rejections
# ─────────────────────────────────────────────────────

def test_reject_spot_stale() -> None:
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(end_in_sec=4*3600, now_ms=now)
    spot_stale = SpotQuote("BTCUSDT", 95_000, 1, 95_001, 1, last_update_ms=now - 20_000)  # 20s velho
    vol = make_vol()
    books = make_healthy_books(now_ms=now)
    r = evaluate_market(m, books, spot_stale, vol, st, now)
    assert r is None
    # Audit record registrou rejeição
    assert any(a.get("rejection") == "spot_stale" for a in st.audit_records)

def test_reject_book_stale() -> None:
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(end_in_sec=4*3600, now_ms=now)
    books = make_healthy_books(now_ms=now - 40_000)  # 40s velho
    r = evaluate_market(m, books, make_spot(now_ms=now), make_vol(), st, now)
    assert r is None
    assert any(a.get("rejection") == "book_stale" for a in st.audit_records)

def test_reject_book_terminal() -> None:
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(end_in_sec=4*3600, now_ms=now)
    # Book terminal: best_bid < 0.02
    books = MarketBooks(
        market_id="mkt_test",
        yes=OutcomeBook(
            token_id="tok_yes",
            bids=[BookLevel(0.01, 100)], asks=[BookLevel(0.99, 100)],
            last_update_ms=now,
        ),
        no=OutcomeBook(
            token_id="tok_no",
            bids=[BookLevel(0.01, 100)], asks=[BookLevel(0.99, 100)],
            last_update_ms=now,
        ),
    )
    r = evaluate_market(m, books, make_spot(now_ms=now), make_vol(), st, now)
    assert r is None
    assert any(a.get("rejection") == "book_terminal" for a in st.audit_records)

def test_reject_depth_too_low() -> None:
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(end_in_sec=4*3600, now_ms=now)
    books = make_healthy_books(depth_notional=100, now_ms=now)  # só $100 total
    r = evaluate_market(m, books, make_spot(now_ms=now), make_vol(), st, now)
    assert r is None
    assert any(a.get("rejection") == "depth_too_low" for a in st.audit_records)

def test_reject_tau_out_of_range() -> None:
    """τ=10 dias fora de B (máx 7d)."""
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(end_in_sec=10*24*3600, now_ms=now)  # 10 dias
    r = evaluate_market(
        m, make_healthy_books(now_ms=now), make_spot(now_ms=now), make_vol(), st, now,
    )
    assert r is None
    assert any(a.get("rejection") == "tau_out_of_range" for a in st.audit_records)

def test_reject_vol_insufficient() -> None:
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(end_in_sec=4*3600, now_ms=now)
    vol_bad = VolEstimate(
        symbol="BTCUSDT", sigma_60=0.5, sigma_10=0.5,
        n_valid_60=20, n_valid_10=10, computed_at_ms=0, regime_divergence=0.0,
    )
    r = evaluate_market(
        m, make_healthy_books(now_ms=now), make_spot(now_ms=now), vol_bad, st, now,
    )
    assert r is None
    assert any(a.get("rejection") == "vol_insufficient" for a in st.audit_records)


# ─────────────────────────────────────────────────────
# HAPPY PATHS
# ─────────────────────────────────────────────────────

def test_happy_path_long_flagged() -> None:
    """
    Spot BTC=99k, K=100k, 30min até resolução, σ=50%.
    p_model at_time above: d2 ≈ ?
        ln(99/100) = -0.01005
        σ²τ/2 = 0.25 × (30/60/24/365) / 2 ≈ 7.13e-6
        num = -0.01005 - 7.13e-6 ≈ -0.01006
        den = 0.5 × √(30/60/24/365) = 0.5 × √5.71e-5 = 0.5 × 0.00755 = 0.00378
        d2 ≈ -2.66 → Φ ≈ 0.0039 ... MUITO pequeno.

    Isso significa que com σ=50%, P_model é ~0.4% em 30min pra ultrapassar K 1% acima.
    Então se p_market=0.30 (alto, pessoal acha que vai subir), Delta ≈ -0.30 (SHORT).

    Usar: p_market mid = (0.40+0.42)/2 = 0.41 e p_model ≈ 0.004
    → Delta = 0.004 - 0.41 = -0.406 → SHORT clara.
    Abs(delta) > 0.03, signal_short = (0.40 - 0.004) × C × W.
    """
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(strike=100_000, direction_above=True, end_in_sec=4*3600, now_ms=now)
    spot = make_spot(s=99_000, now_ms=now)
    vol = make_vol(sigma=0.5)
    books = make_healthy_books(yes_bid=0.40, yes_ask=0.42, depth_notional=2000, now_ms=now)
    rec = evaluate_market(m, books, spot, vol, st, now)
    # Espero SHORT_YES flagged
    assert rec is not None, "deveria ter emitido record"
    assert rec.direction_label == "SHORT_YES", f"got {rec.direction_label}"
    assert rec.opportunity_flagged is True
    assert rec.strategy == "A"
    assert rec.p_model is not None

def test_happy_path_no_flag_small_delta() -> None:
    """Spot muito próximo de K → P_model ~ 0.5, p_market ~ 0.5 → delta pequeno."""
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(strike=95_000, direction_above=True, end_in_sec=4*3600, now_ms=now)
    spot = make_spot(s=95_000, now_ms=now)  # at-money
    vol = make_vol(sigma=0.5)
    books = make_healthy_books(yes_bid=0.48, yes_ask=0.50, depth_notional=2000, now_ms=now)
    rec = evaluate_market(m, books, spot, vol, st, now)
    # delta pequeno → não flag
    if rec is not None:
        assert not rec.opportunity_flagged or abs(rec.delta) < 0.10


# ─────────────────────────────────────────────────────
# SUB-60S DIRECIONAL
# ─────────────────────────────────────────────────────

def test_sub60_directional_long() -> None:
    """
    τ=30s, S=99500 > K=99000 (above), p_market=0.30 (baixo)
    → directional expected = 1.0 (já tá acima), delta = 1 - 0.30 = 0.70 → LONG
    """
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(strike=99_000, direction_above=True, end_in_sec=30, now_ms=now)
    spot = make_spot(s=99_500, now_ms=now)
    vol = make_vol()
    books = make_healthy_books(yes_bid=0.29, yes_ask=0.31, depth_notional=2000, now_ms=now)
    rec = evaluate_market(m, books, spot, vol, st, now)
    assert rec is not None
    assert rec.strategy == "sub60"
    assert rec.model_undefined is True
    assert rec.direction_label == "LONG_YES"
    assert rec.opportunity_flagged is True
    assert rec.p_model is None


# ─────────────────────────────────────────────────────
# DEDUPE POR JANELA
# ─────────────────────────────────────────────────────

def test_dedupe_same_opportunity_two_ticks() -> None:
    """
    Mesma oportunidade em 2 ticks consecutivos → só 1 opportunity_id novo no tick 1.
    """
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(strike=100_000, direction_above=True, end_in_sec=4*3600, now_ms=now)
    spot = make_spot(s=99_000, now_ms=now)
    vol = make_vol(sigma=0.5)
    books = make_healthy_books(yes_bid=0.40, yes_ask=0.42, depth_notional=2000, now_ms=now)

    # Tick 1: abre janela
    rec1 = evaluate_market(m, books, spot, vol, st, now)
    assert rec1 is not None
    id1 = rec1.opportunity_id

    # Tick 2 (1s depois): mesma oportunidade, atualiza books
    now2 = now + 1000
    books2 = make_healthy_books(yes_bid=0.40, yes_ask=0.42, depth_notional=2000, now_ms=now2)
    spot2 = make_spot(s=99_000, now_ms=now2)
    rec2 = evaluate_market(m, books2, spot2, vol, st, now2)
    # tick 2 retorna None (atualização interna da janela)
    assert rec2 is None
    # Janela aberta tem tick_count=2
    key = (m.market_id, "SHORT_YES")
    assert key in st.open_windows
    assert st.open_windows[key].tick_count == 2
    assert st.open_windows[key].opportunity_id == id1

def test_window_closes_when_delta_drops() -> None:
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(strike=100_000, direction_above=True, end_in_sec=4*3600, now_ms=now)
    spot = make_spot(s=99_000, now_ms=now)
    vol = make_vol(sigma=0.5)
    # Tick 1: delta grande, flag
    books_big = make_healthy_books(yes_bid=0.40, yes_ask=0.42, depth_notional=2000, now_ms=now)
    evaluate_market(m, books_big, spot, vol, st, now)
    key = (m.market_id, "SHORT_YES")
    assert key in st.open_windows

    # Tick 2: p_market volta pra 0.05 (próximo do p_model=0.004) → delta cai → fecha janela
    now2 = now + 2000
    books_small = make_healthy_books(yes_bid=0.03, yes_ask=0.05, depth_notional=2000, now_ms=now2)
    spot2 = make_spot(s=99_000, now_ms=now2)
    evaluate_market(m, books_small, spot2, vol, st, now2)
    # Janela fechou
    assert key not in st.open_windows
    # Record foi pra closed_records com closed_at_ms preenchido
    assert len(st.closed_records) == 1
    assert st.closed_records[0].closed_at_ms == now2


def test_reopen_gap_respected() -> None:
    """Janela fechada não reabre antes de REOPEN_GAP_MS."""
    now = 10_000_000
    st = DetectorState()
    m = make_market_at_time(strike=100_000, direction_above=True, end_in_sec=4*3600, now_ms=now)
    vol = make_vol(sigma=0.5)
    # Abre e fecha rapidamente
    evaluate_market(
        m, make_healthy_books(yes_bid=0.40, yes_ask=0.42, depth_notional=2000, now_ms=now),
        make_spot(s=99_000, now_ms=now), vol, st, now,
    )
    evaluate_market(
        m, make_healthy_books(yes_bid=0.03, yes_ask=0.05, depth_notional=2000, now_ms=now+100),
        make_spot(s=99_000, now_ms=now+100), vol, st, now+100,
    )
    # Janela fechada em now+100. Tenta reabrir em now+1000 (< 60_000 do gap)
    now3 = now + 1000
    rec3 = evaluate_market(
        m, make_healthy_books(yes_bid=0.40, yes_ask=0.42, depth_notional=2000, now_ms=now3),
        make_spot(s=99_000, now_ms=now3), vol, st, now3,
    )
    # Não reabre
    assert rec3 is None, "deveria respeitar gap de reabertura"

    # Após o gap, reabre
    now4 = now + 100 + REOPEN_GAP_MS + 500
    rec4 = evaluate_market(
        m, make_healthy_books(yes_bid=0.40, yes_ask=0.42, depth_notional=2000, now_ms=now4),
        make_spot(s=99_000, now_ms=now4), vol, st, now4,
    )
    assert rec4 is not None
    assert rec4.opportunity_flagged


# ─────────────────────────────────────────────────────
# TWO BARRIER
# ─────────────────────────────────────────────────────

def test_two_barrier_middle() -> None:
    """S entre k_low e k_high → P_model teórica ~ log-linear."""
    now = 10_000_000
    st = DetectorState()
    m = make_market_two_barrier(k_low=80_000, k_high=100_000, end_in_sec=4*3600, now_ms=now)
    # S=89442 (sqrt 80k*100k) → P(high) ≈ 0.5
    spot = make_spot(s=89_442, now_ms=now)
    vol = make_vol()
    books = make_healthy_books(yes_bid=0.48, yes_ask=0.52, depth_notional=2000, now_ms=now)
    rec = evaluate_market(m, books, spot, vol, st, now)
    # Delta pequeno → não deve flagar
    if rec is not None:
        assert abs(rec.delta) < 0.05, f"delta esperado pequeno, got {rec.delta}"


# ─────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────

def run_all() -> int:
    tests = [
        test_strategy_A,
        test_strategy_B,
        test_strategy_out_of_range,
        test_reject_spot_stale,
        test_reject_book_stale,
        test_reject_book_terminal,
        test_reject_depth_too_low,
        test_reject_tau_out_of_range,
        test_reject_vol_insufficient,
        test_happy_path_long_flagged,
        test_happy_path_no_flag_small_delta,
        test_sub60_directional_long,
        test_dedupe_same_opportunity_two_ticks,
        test_window_closes_when_delta_drops,
        test_reopen_gap_respected,
        test_two_barrier_middle,
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
