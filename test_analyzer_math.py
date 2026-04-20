"""
test_analyzer_math.py — cobertura do núcleo matemático + parser.

Cobre:
- Parse de dólares: k/K/m/M, virgulas, decimais
- Classificação dos 4 tipos de mercado com títulos REAIS do Polymarket
- Sanity K vs spot
- d2, z-score, Φ, P_model para 3 tipos
- Confidence / urgency / fee / slippage
- Edge cases: S=K, tau→0, sigma→0, liquidez insuficiente

Rodar: python3 test_analyzer_math.py
"""
from __future__ import annotations

import math
import sys
import traceback

from analyzer_math import (
    ACCEPTED_SYMBOLS,
    MarketType,
    classify_and_parse,
    compute_d2,
    compute_z_score,
    confidence_factor,
    detect_symbol,
    norm_cdf,
    p_model_at_time,
    p_model_touch,
    p_model_two_barrier,
    parse_all_dollar_amounts,
    parse_dollar_amount,
    polymarket_fee_dynamic,
    urgency_weight,
    vwap_slippage,
)

# ─────────────────────────────────────────────────────
# Helpers de teste
# ─────────────────────────────────────────────────────

def approx(a: float, b: float, tol: float = 1e-6) -> bool:
    return abs(a - b) <= tol

def rel_approx(a: float, b: float, tol: float = 1e-3) -> bool:
    """Tolerância relativa."""
    if b == 0:
        return abs(a) <= tol
    return abs((a - b) / b) <= tol


# ─────────────────────────────────────────────────────
# PARSE DE DÓLARES
# ─────────────────────────────────────────────────────

def test_parse_dollar_basic() -> None:
    assert parse_dollar_amount("$100") == 100.0
    assert parse_dollar_amount("$1,000") == 1000.0
    assert parse_dollar_amount("$1.5") == 1.5

def test_parse_dollar_k_suffix() -> None:
    assert parse_dollar_amount("$95k") == 95_000.0
    assert parse_dollar_amount("$95K") == 95_000.0
    assert parse_dollar_amount("$4.5k") == 4_500.0

def test_parse_dollar_m_suffix() -> None:
    assert parse_dollar_amount("$1m") == 1_000_000.0
    assert parse_dollar_amount("$1M") == 1_000_000.0
    assert parse_dollar_amount("$1.2M") == 1_200_000.0

def test_parse_dollar_comma_big() -> None:
    assert parse_dollar_amount("$116,000") == 116_000.0
    assert parse_dollar_amount("$1,234,567") == 1_234_567.0

def test_parse_dollar_subdollar() -> None:
    """Altcoins tipo DOGE com K < $1."""
    assert parse_dollar_amount("$0.85") == 0.85
    assert parse_dollar_amount("$0.15") == 0.15

def test_parse_dollar_with_spaces() -> None:
    assert parse_dollar_amount("price of $95,000 by Friday") == 95_000.0

def test_parse_dollar_returns_none() -> None:
    assert parse_dollar_amount("no dollar here") is None
    assert parse_dollar_amount("") is None

def test_parse_all_dollar_amounts() -> None:
    """Múltiplos valores em two-barrier."""
    out = parse_all_dollar_amounts("Will Bitcoin hit $80k or $100k first?")
    assert out == [80_000.0, 100_000.0], f"got {out}"

    out2 = parse_all_dollar_amounts("BTC between $90k and $110k?")
    assert out2 == [90_000.0, 110_000.0]


# ─────────────────────────────────────────────────────
# DETECT SYMBOL
# ─────────────────────────────────────────────────────

def test_detect_symbol_btc_variants() -> None:
    assert detect_symbol("Bitcoin above $100k") == "BTCUSDT"
    assert detect_symbol("BTC to hit $95k") == "BTCUSDT"
    assert detect_symbol("will btc reach") == "BTCUSDT"

def test_detect_symbol_all_supported() -> None:
    for kw, expected in ACCEPTED_SYMBOLS.items():
        title = f"{kw} above $100"
        got = detect_symbol(title)
        assert got == expected, f"{kw}: expected {expected}, got {got}"

def test_detect_symbol_unknown() -> None:
    assert detect_symbol("random token above $1") is None
    assert detect_symbol("SHIB to moon") is None  # SHIB não está no universo


# ─────────────────────────────────────────────────────
# CLASSIFICAÇÃO DE MERCADO (títulos REAIS)
# ─────────────────────────────────────────────────────

def test_classify_strike_at_time_real() -> None:
    """Título real do Polymarket hourly."""
    mkt = classify_and_parse("Will BTC be above $95,000 at 5:00 PM ET?", spot_for_sanity=90_000)
    assert mkt.market_type == MarketType.STRIKE_AT_TIME
    assert mkt.symbol == "BTCUSDT"
    assert mkt.strike_primary == 95_000.0
    assert mkt.direction_above is True
    assert mkt.parse_confidence == 1.0

def test_classify_strike_at_time_below() -> None:
    mkt = classify_and_parse("Will ETH close below $3,500 on April 20?", spot_for_sanity=3_800)
    assert mkt.market_type == MarketType.STRIKE_AT_TIME
    assert mkt.direction_above is False
    assert mkt.strike_primary == 3_500.0

def test_classify_strike_touch_real() -> None:
    mkt = classify_and_parse("Will Bitcoin reach $150k in April?", spot_for_sanity=90_000)
    assert mkt.market_type == MarketType.STRIKE_TOUCH
    assert mkt.direction_above is True
    assert mkt.strike_primary == 150_000.0

def test_classify_touch_hit() -> None:
    mkt = classify_and_parse("Will ETH hit $5000?", spot_for_sanity=3_500)
    assert mkt.market_type == MarketType.STRIKE_TOUCH
    assert mkt.strike_primary == 5_000.0

def test_classify_two_barrier_real() -> None:
    """Título real: 'Will Bitcoin hit $80k or $100k first?'"""
    mkt = classify_and_parse("Will Bitcoin hit $80k or $100k first?", spot_for_sanity=90_000)
    assert mkt.market_type == MarketType.TWO_BARRIER
    assert mkt.symbol == "BTCUSDT"
    assert mkt.strike_primary == 80_000.0   # low
    assert mkt.strike_secondary == 100_000.0  # high
    assert mkt.parse_confidence == 1.0

def test_classify_two_barrier_ordering() -> None:
    """Mesmo se título tem ordem invertida, low e high são ordenados corretamente."""
    mkt = classify_and_parse("BTC $100k or $80k first?", spot_for_sanity=90_000)
    assert mkt.market_type == MarketType.TWO_BARRIER
    assert mkt.strike_primary == 80_000.0
    assert mkt.strike_secondary == 100_000.0

# ─────────────────────────────────────────────────────
# DIP-TO (direction_below via touch) — títulos reais do Polymarket
# ─────────────────────────────────────────────────────

def test_classify_dip_to_real() -> None:
    """Título real: 'Will Bitcoin dip to $60,000 in April?'"""
    spots = {"BTCUSDT": 90_000}
    mkt = classify_and_parse("Will Bitcoin dip to $60,000 in April?", spot_by_symbol=spots)
    assert mkt.market_type == MarketType.STRIKE_TOUCH
    assert mkt.strike_primary == 60_000.0
    assert mkt.direction_above is False
    assert mkt.parse_confidence == 1.0

def test_classify_fall_to() -> None:
    spots = {"ETHUSDT": 3800}
    mkt = classify_and_parse("Will Ethereum fall to $3000 in April?", spot_by_symbol=spots)
    assert mkt.market_type == MarketType.STRIKE_TOUCH
    assert mkt.direction_above is False

def test_classify_drop_to() -> None:
    spots = {"SOLUSDT": 180}
    mkt = classify_and_parse("Will Solana drop to $100 in April?", spot_by_symbol=spots)
    assert mkt.market_type == MarketType.STRIKE_TOUCH
    assert mkt.direction_above is False

def test_classify_crash_to() -> None:
    spots = {"BTCUSDT": 90_000}
    mkt = classify_and_parse("Will Bitcoin crash to $50,000 in April?", spot_by_symbol=spots)
    assert mkt.market_type == MarketType.STRIKE_TOUCH
    assert mkt.direction_above is False

def test_classify_dip_rejects_absurd_strike() -> None:
    """Dip to $20k quando BTC=90k → ratio 0.22, sanity rejeita."""
    spots = {"BTCUSDT": 90_000}
    mkt = classify_and_parse("Will Bitcoin dip to $20,000 in April?", spot_by_symbol=spots)
    assert mkt.parse_confidence == 0.0

# ─────────────────────────────────────────────────────
# SPOT_BY_SYMBOL — sanity correto por par
# ─────────────────────────────────────────────────────

def test_spot_by_symbol_eth_correct() -> None:
    """ETH=$3800, K=$4000 → ratio 1.05, aceita (antes rejeitava usando spot BTC)"""
    spots = {"BTCUSDT": 90_000, "ETHUSDT": 3800}
    mkt = classify_and_parse("Will Ethereum reach $4,000 in April?", spot_by_symbol=spots)
    assert mkt.parse_confidence == 1.0
    assert mkt.strike_primary == 4000.0
    assert mkt.symbol == "ETHUSDT"

def test_spot_by_symbol_btc_independent() -> None:
    """BTC e ETH sanity não se interferem."""
    spots = {"BTCUSDT": 90_000, "ETHUSDT": 3800}
    mkt_btc = classify_and_parse("Will Bitcoin reach $100k in April?", spot_by_symbol=spots)
    assert mkt_btc.parse_confidence == 1.0
    mkt_eth = classify_and_parse("Will Ethereum reach $5000 in April?", spot_by_symbol=spots)
    assert mkt_eth.parse_confidence == 1.0

def test_spot_by_symbol_legacy_fallback() -> None:
    """Se passar spot_for_sanity antigo (legacy) ainda funciona."""
    mkt = classify_and_parse("Will Bitcoin reach $100k in April?", spot_for_sanity=90_000)
    assert mkt.parse_confidence == 1.0

def test_classify_up_down_implicit_real() -> None:
    """Título real: 'Bitcoin Up or Down - March 8, 8AM ET'"""
    mkt = classify_and_parse("Bitcoin Up or Down - March 8, 8AM ET")
    assert mkt.market_type == MarketType.UP_DOWN_IMPLICIT
    assert mkt.symbol == "BTCUSDT"
    assert mkt.strike_primary is None
    # MVP: skippa, mas classifica corretamente

def test_classify_no_dollar() -> None:
    mkt = classify_and_parse("BTC up or down today")
    # "up or down" → UP_DOWN_IMPLICIT (não UNKNOWN)
    assert mkt.market_type == MarketType.UP_DOWN_IMPLICIT


# ─────────────────────────────────────────────────────
# SANITY K vs S_t
# ─────────────────────────────────────────────────────

def test_sanity_rejects_absurd_strike() -> None:
    """K=2 quando BTC=90000 → parse errado."""
    mkt = classify_and_parse("BTC above $2 at 5PM", spot_for_sanity=90_000)
    assert mkt.parse_confidence == 0.0, "deveria rejeitar K absurdamente pequeno"

def test_sanity_accepts_valid_ratio() -> None:
    """K=110k quando BTC=90k → ratio 1.22, ok."""
    mkt = classify_and_parse("BTC above $110k at 5PM", spot_for_sanity=90_000)
    assert mkt.parse_confidence == 1.0

def test_sanity_rejects_absurd_high_strike() -> None:
    """K=500k quando BTC=90k → ratio 5.5, rejeita (fora de 3.0)."""
    mkt = classify_and_parse("BTC above $500k at 5PM", spot_for_sanity=90_000)
    assert mkt.parse_confidence == 0.0

def test_sanity_two_barrier_both_checked() -> None:
    """Tanto low quanto high precisam passar sanity."""
    # low=1, high=100k, spot=90k → low/spot = 0.00001, rejeita
    mkt = classify_and_parse("BTC hit $1 or $100k first?", spot_for_sanity=90_000)
    assert mkt.parse_confidence == 0.0

def test_sanity_skipped_if_no_spot() -> None:
    """Sem spot, não aplica sanity."""
    mkt = classify_and_parse("BTC above $500k at 5PM", spot_for_sanity=None)
    assert mkt.parse_confidence == 1.0


# ─────────────────────────────────────────────────────
# NORM CDF
# ─────────────────────────────────────────────────────

def test_norm_cdf_basic() -> None:
    assert approx(norm_cdf(0.0), 0.5, tol=1e-9)
    # Φ(1.96) ≈ 0.975
    assert rel_approx(norm_cdf(1.96), 0.975, tol=1e-3)
    assert rel_approx(norm_cdf(-1.96), 0.025, tol=1e-3)
    # Φ(∞) → 1, Φ(-∞) → 0
    assert norm_cdf(10.0) > 0.9999
    assert norm_cdf(-10.0) < 0.0001


# ─────────────────────────────────────────────────────
# D2 / Z-SCORE
# ─────────────────────────────────────────────────────

def test_d2_at_the_money() -> None:
    """S=K, σ=0.5, τ=1 → d2 = -σ·√τ/2 = -0.25"""
    d2 = compute_d2(spot=100, strike=100, sigma_annual=0.5, tau_years=1.0)
    assert approx(d2, -0.25, tol=1e-6)

def test_d2_in_the_money() -> None:
    """S=110, K=100, σ=0.3, τ=0.5 — valor positivo esperado."""
    d2 = compute_d2(spot=110, strike=100, sigma_annual=0.3, tau_years=0.5)
    # manual: ln(110/100)=0.0953, σ²τ/2 = 0.09·0.5/2=0.0225, num=0.0728, den=0.3·0.707=0.2121
    # d2 ≈ 0.0728/0.2121 ≈ 0.3433
    assert rel_approx(d2, 0.3433, tol=1e-2)

def test_d2_invalid_inputs() -> None:
    try:
        compute_d2(0, 100, 0.5, 1.0)
        raise AssertionError("deveria ter levantado ValueError")
    except ValueError:
        pass
    try:
        compute_d2(100, 100, -0.1, 1.0)
        raise AssertionError
    except ValueError:
        pass
    try:
        compute_d2(100, 100, 0.5, 0.0)
        raise AssertionError
    except ValueError:
        pass

def test_z_score_at_money() -> None:
    """S=K → z=0"""
    z = compute_z_score(100, 100, 0.5, 1.0)
    assert approx(z, 0.0, tol=1e-9)

def test_z_score_above() -> None:
    """S > K → z > 0 (direção clara pra above)"""
    z = compute_z_score(120, 100, 0.3, 1.0)
    # ln(1.2)/0.3 = 0.1823/0.3 = 0.608
    assert rel_approx(z, 0.608, tol=1e-2)


# ─────────────────────────────────────────────────────
# P_MODEL_AT_TIME
# ─────────────────────────────────────────────────────

def test_p_model_at_time_deep_itm() -> None:
    """S muito acima de K, pouco tempo → prob(above) ~ 1."""
    p = p_model_at_time(spot=150, strike=100, sigma_annual=0.3, tau_years=0.01, direction_above=True)
    assert p > 0.99

def test_p_model_at_time_deep_otm() -> None:
    """S muito abaixo de K, pouco tempo → prob(above) ~ 0."""
    p = p_model_at_time(spot=50, strike=100, sigma_annual=0.3, tau_years=0.01, direction_above=True)
    assert p < 0.01

def test_p_model_at_time_at_money() -> None:
    """S=K, σ=0.5, τ=1 → d2=-0.25, Φ(-0.25) ≈ 0.401"""
    p = p_model_at_time(100, 100, 0.5, 1.0, direction_above=True)
    assert rel_approx(p, 0.4013, tol=5e-3)

def test_p_model_at_time_below_is_complement() -> None:
    p_above = p_model_at_time(100, 95, 0.4, 0.5, direction_above=True)
    p_below = p_model_at_time(100, 95, 0.4, 0.5, direction_above=False)
    assert approx(p_above + p_below, 1.0, tol=1e-9)


# ─────────────────────────────────────────────────────
# P_MODEL_TOUCH
# ─────────────────────────────────────────────────────

def test_p_touch_already_hit() -> None:
    """Se K já está abaixo de S e direção é 'above', já foi tocado."""
    p = p_model_touch(spot=110, strike=100, sigma_annual=0.3, tau_years=0.5, direction_above=True)
    assert p == 1.0

def test_p_touch_deep_otm() -> None:
    """K muito longe, pouco tempo → prob baixa."""
    p = p_model_touch(spot=100, strike=200, sigma_annual=0.3, tau_years=0.01, direction_above=True)
    assert p < 0.01

def test_p_touch_approximate_at_money() -> None:
    """Touch > At-time para mesma config (mais inclusivo)."""
    p_touch = p_model_touch(100, 110, 0.4, 0.25, direction_above=True)
    p_at_time = p_model_at_time(100, 110, 0.4, 0.25, direction_above=True)
    assert p_touch >= p_at_time

def test_p_touch_capped_at_1() -> None:
    """2·Φ(d2) nunca pode passar de 1."""
    # K muito próximo, τ grande → 2·Φ(d2) passaria de 1
    p = p_model_touch(spot=100, strike=101, sigma_annual=0.8, tau_years=2.0, direction_above=True)
    assert 0.0 <= p <= 1.0


# ─────────────────────────────────────────────────────
# P_MODEL_TWO_BARRIER
# ─────────────────────────────────────────────────────

def test_two_barrier_middle() -> None:
    """S no centro geométrico → probs próximas de 0.5."""
    # sqrt(80000 · 100000) = 89442
    p_high, p_low = p_model_two_barrier(spot=89442.72, k_low=80_000, k_high=100_000)
    assert rel_approx(p_high, 0.5, tol=1e-3)
    assert rel_approx(p_low, 0.5, tol=1e-3)

def test_two_barrier_close_to_high() -> None:
    """S=99k, entre 80 e 100k → tende a tocar 100k primeiro."""
    p_high, p_low = p_model_two_barrier(spot=99_000, k_low=80_000, k_high=100_000)
    assert p_high > 0.9
    assert approx(p_high + p_low, 1.0, tol=1e-9)

def test_two_barrier_below_low() -> None:
    """S abaixo de k_low: já tocou k_low."""
    p_high, p_low = p_model_two_barrier(spot=70_000, k_low=80_000, k_high=100_000)
    assert p_high == 0.0
    assert p_low == 1.0

def test_two_barrier_invalid() -> None:
    try:
        p_model_two_barrier(spot=100, k_low=100, k_high=80)  # low >= high
        raise AssertionError
    except ValueError:
        pass


# ─────────────────────────────────────────────────────
# CONFIDENCE / URGENCY
# ─────────────────────────────────────────────────────

def test_confidence_at_threshold() -> None:
    """|z|=z_threshold → sigmoid(0) = 0.5"""
    c = confidence_factor(z=1.0, k=2.0, z_threshold=1.0)
    assert rel_approx(c, 0.5, tol=1e-9)

def test_confidence_low_z_uncertain() -> None:
    """|z|=0 → confidence < 0.5"""
    c = confidence_factor(z=0.0, k=2.0, z_threshold=1.0)
    assert c < 0.15  # sigmoid(-2) ≈ 0.119

def test_confidence_high_z_certain() -> None:
    """|z|=3 → confidence > 0.95"""
    c = confidence_factor(z=3.0, k=2.0, z_threshold=1.0)
    assert c > 0.95

def test_confidence_clip_extreme() -> None:
    """|z|=100 não deve overflowar."""
    c = confidence_factor(z=100.0)
    assert 0.0 < c <= 1.0

def test_urgency_fresh_market() -> None:
    """τ = τ_max → urgency = 0"""
    w = urgency_weight(tau_years=0.01, tau_max_years=0.01)
    assert approx(w, 0.0, tol=1e-9)

def test_urgency_near_resolution() -> None:
    """τ → 0 → urgency → 1"""
    w = urgency_weight(tau_years=0.0001, tau_max_years=0.01)
    assert w > 0.98

def test_urgency_clipped() -> None:
    """τ > τ_max não deve dar peso negativo."""
    w = urgency_weight(tau_years=0.02, tau_max_years=0.01)
    assert w == 0.0


# ─────────────────────────────────────────────────────
# FEE DINÂMICO
# ─────────────────────────────────────────────────────

def test_fee_at_p05_is_peak() -> None:
    """Em p=0.5, fee deve ser exatamente rate_peak."""
    assert rel_approx(polymarket_fee_dynamic(0.5, rate_peak=0.072), 0.072, tol=1e-9)

def test_fee_at_extremes_zero() -> None:
    assert polymarket_fee_dynamic(0.0) == 0.0
    assert polymarket_fee_dynamic(1.0) == 0.0

def test_fee_symmetric() -> None:
    """fee(p) == fee(1-p)"""
    assert rel_approx(polymarket_fee_dynamic(0.3), polymarket_fee_dynamic(0.7), tol=1e-9)

def test_fee_clips_outside_01() -> None:
    """Valores fora de [0,1] são clipados."""
    assert polymarket_fee_dynamic(-0.5) == 0.0
    assert polymarket_fee_dynamic(1.5) == 0.0


# ─────────────────────────────────────────────────────
# VWAP SLIPPAGE
# ─────────────────────────────────────────────────────

def test_vwap_exact_fit_best_level() -> None:
    """$500 cabe no best level → slippage = 0."""
    # level: price=0.50, size=10_000 → notional = $5000
    levels = [(0.50, 10_000.0)]
    s = vwap_slippage(levels, notional_target=500.0)
    assert s == 0.0

def test_vwap_crosses_levels() -> None:
    """$500 precisa atravessar 2 levels."""
    # level1: 0.50 × 200 = $100
    # level2: 0.52 × 1000 = $520
    # preciso $500: $100 do lv1 + $400 do lv2 (770 shares ~ 0.5 preço + 769 shares a 0.52)
    # shares_lv1 = 100/0.50 = 200
    # shares_lv2 = 400/0.52 = 769.23
    # total shares = 969.23, total cost = $500
    # avg = 500/969.23 = 0.5158
    # slippage = (0.5158 - 0.50)/0.50 = 0.0317 (~3.17%)
    levels = [(0.50, 200.0), (0.52, 1000.0)]
    s = vwap_slippage(levels, notional_target=500.0)
    assert s is not None
    assert rel_approx(s, 0.0317, tol=1e-2)

def test_vwap_insufficient_liquidity() -> None:
    """$500 mas só tem $200 de book."""
    levels = [(0.50, 400.0)]  # notional $200
    s = vwap_slippage(levels, notional_target=500.0)
    assert s is None

def test_vwap_empty_book() -> None:
    assert vwap_slippage([], 500.0) is None


# ─────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────

def run_all() -> int:
    tests = [
        # Parse
        test_parse_dollar_basic,
        test_parse_dollar_k_suffix,
        test_parse_dollar_m_suffix,
        test_parse_dollar_comma_big,
        test_parse_dollar_subdollar,
        test_parse_dollar_with_spaces,
        test_parse_dollar_returns_none,
        test_parse_all_dollar_amounts,
        # Symbol
        test_detect_symbol_btc_variants,
        test_detect_symbol_all_supported,
        test_detect_symbol_unknown,
        # Classify
        test_classify_strike_at_time_real,
        test_classify_strike_at_time_below,
        test_classify_strike_touch_real,
        test_classify_touch_hit,
        test_classify_two_barrier_real,
        test_classify_two_barrier_ordering,
        test_classify_dip_to_real,
        test_classify_fall_to,
        test_classify_drop_to,
        test_classify_crash_to,
        test_classify_dip_rejects_absurd_strike,
        test_spot_by_symbol_eth_correct,
        test_spot_by_symbol_btc_independent,
        test_spot_by_symbol_legacy_fallback,
        test_classify_up_down_implicit_real,
        test_classify_no_dollar,
        # Sanity
        test_sanity_rejects_absurd_strike,
        test_sanity_accepts_valid_ratio,
        test_sanity_rejects_absurd_high_strike,
        test_sanity_two_barrier_both_checked,
        test_sanity_skipped_if_no_spot,
        # Math
        test_norm_cdf_basic,
        test_d2_at_the_money,
        test_d2_in_the_money,
        test_d2_invalid_inputs,
        test_z_score_at_money,
        test_z_score_above,
        # P_model_at_time
        test_p_model_at_time_deep_itm,
        test_p_model_at_time_deep_otm,
        test_p_model_at_time_at_money,
        test_p_model_at_time_below_is_complement,
        # P_model_touch
        test_p_touch_already_hit,
        test_p_touch_deep_otm,
        test_p_touch_approximate_at_money,
        test_p_touch_capped_at_1,
        # Two_barrier
        test_two_barrier_middle,
        test_two_barrier_close_to_high,
        test_two_barrier_below_low,
        test_two_barrier_invalid,
        # Confidence/urgency
        test_confidence_at_threshold,
        test_confidence_low_z_uncertain,
        test_confidence_high_z_certain,
        test_confidence_clip_extreme,
        test_urgency_fresh_market,
        test_urgency_near_resolution,
        test_urgency_clipped,
        # Fee
        test_fee_at_p05_is_peak,
        test_fee_at_extremes_zero,
        test_fee_symmetric,
        test_fee_clips_outside_01,
        # Slippage
        test_vwap_exact_fit_best_level,
        test_vwap_crosses_levels,
        test_vwap_insufficient_liquidity,
        test_vwap_empty_book,
    ]
    passed = 0
    failed: list[tuple[str, str]] = []
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failed.append((t.__name__, str(e)))
            print(f"  FAIL  {t.__name__}: {e}")
        except Exception as e:
            failed.append((t.__name__, f"{type(e).__name__}: {e}"))
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print(f"\nResultado: {passed}/{len(tests)} passou, {len(failed)} falhou")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(run_all())
