"""
detector.py — núcleo de detecção de oportunidades cross-platform.

Para cada mercado Polymarket, a cada tick de detecção:
  1. Verifica freshness do book Polymarket e do spot Binance.
  2. Computa P_model via modelo apropriado (at_time / touch / two_barrier).
  3. Para LONG (comprar YES) e SHORT (vender YES), calcula Delta, z, C, W, Signal.
  4. Aplica fees dinâmicos + VWAP slippage + margem de segurança.
  5. Emite OpportunityRecord se Signal > threshold E Delta > 3% E book_depth OK.

Dedupe por janela aberta/fechada (igual v2). Uma linha por janela.

Sub-60s: lógica direcional só (sem P_model), flag model_undefined=True.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from analyzer_math import (
    MarketType,
    confidence_factor,
    p_model_at_time,
    p_model_touch,
    p_model_two_barrier,
    compute_z_score,
    polymarket_fee_dynamic,
    urgency_weight,
    vwap_slippage,
    POLYMARKET_FEE_RATE_CRYPTO_PEAK,
)
from binance_client import SpotQuote, VolEstimate
from polymarket_client import MarketBooks, OutcomeBook, PolymarketMarket


# ─────────────────────────────────────────────────────
# Constantes aprovadas pelo operador (20/abr/2026)
# ─────────────────────────────────────────────────────

# Estratégia A: τ ∈ [15min, 1h]
STRATEGY_A_TAU_MIN_SEC = 15 * 60
STRATEGY_A_TAU_MAX_SEC = 60 * 60
# Estratégia B: τ ∈ [60s, 15min]
STRATEGY_B_TAU_MIN_SEC = 60
STRATEGY_B_TAU_MAX_SEC = 15 * 60

# Threshold abaixo do qual P_model é indefinido (numericamente)
MODEL_UNDEFINED_TAU_SEC = 60

# Thresholds de oportunidade
MIN_DELTA_ABS = 0.03              # 3% mínimo de mispricing
MIN_BOOK_DEPTH_USD = 500.0        # pra VWAP-$500 fazer sentido
SAFETY_MARGIN = 0.005             # margem de segurança extra

# Dedupe
REOPEN_GAP_MS = 60_000            # janela precisa ficar fechada >=60s pra reabrir

# Confidence factor
CONFIDENCE_K = 2.0
CONFIDENCE_Z_THRESHOLD = 1.0

# Limites de vol minimamente aceitável
MIN_VOL_CANDLES = 30


# ─────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────

SECONDS_PER_YEAR = 365 * 24 * 3600


@dataclass(frozen=True)
class DetectionInputs:
    """Snapshot das inputs usadas numa detecção — pra auditoria."""
    timestamp_ms: int
    symbol: str
    market_type: str
    strategy: str                   # "A" ou "B"
    binance_bid: float
    binance_ask: float
    binance_mid: float
    polymarket_yes_bid: Optional[float]
    polymarket_yes_ask: Optional[float]
    polymarket_no_bid: Optional[float]
    polymarket_no_ask: Optional[float]
    strike_primary: float
    strike_secondary: Optional[float]
    direction_above: Optional[bool]
    tau_sec: float
    sigma_60: Optional[float]
    sigma_10: Optional[float]
    sigma_n_valid: int


@dataclass(frozen=True)
class OpportunityRecord:
    """Uma oportunidade detectada. Uma linha por janela aberta."""
    opportunity_id: int
    detected_at_ms: int
    closed_at_ms: Optional[int]

    market_id: str
    slug: str
    question: str
    market_type: str
    strategy: str                   # "A" | "B" | "sub60"
    strategies_matched: str          # ex "A+B" se overlap (não deve acontecer com janelas disjuntas)
    symbol: str

    # Inputs
    tau_sec: float
    spot_s: float
    strike_primary: float
    strike_secondary: Optional[float]
    direction_above: Optional[bool]
    sigma_60: Optional[float]
    sigma_10: Optional[float]

    # Probabilities
    p_model: Optional[float]           # None se model_undefined
    p_market: float                     # mid do lado YES
    delta: float                        # p_model - p_market (None → direcional)
    z: Optional[float]
    c: float
    w: float
    signal: Optional[float]             # Delta × C × W, None se model undef

    # Sinais executáveis (ask/bid aware)
    signal_long: Optional[float]        # considerando pagar ask
    signal_short: Optional[float]       # considerando receber bid

    # Custos
    fee_peak_polymarket: float          # 0.072 sempre (worst case)
    fee_dynamic_at_p: float             # fee real em p=p_market
    slippage_long_frac: Optional[float]
    slippage_short_frac: Optional[float]

    # Tamanhos
    book_depth_usd: float
    yes_best_bid: Optional[float]
    yes_best_ask: Optional[float]

    # Flags
    model_undefined: bool
    opportunity_flagged: bool
    direction_label: str              # "LONG_YES" | "SHORT_YES" | "NONE"
    rejection_reason: str             # se não é opportunity, por quê


# ─────────────────────────────────────────────────────
# Funções auxiliares
# ─────────────────────────────────────────────────────

def compute_tau_sec(market: PolymarketMarket, now_ms: int) -> float:
    return (market.end_time_ms - now_ms) / 1000.0


def tau_sec_to_years(tau_sec: float) -> float:
    return tau_sec / SECONDS_PER_YEAR


def classify_strategy(tau_sec: float) -> Optional[str]:
    """'A' / 'B' / None se fora das janelas."""
    if STRATEGY_A_TAU_MIN_SEC <= tau_sec <= STRATEGY_A_TAU_MAX_SEC:
        return "A"
    if STRATEGY_B_TAU_MIN_SEC <= tau_sec < STRATEGY_A_TAU_MIN_SEC:
        return "B"
    return None


def compute_p_market(book: OutcomeBook) -> Optional[float]:
    bb = book.best_bid
    ba = book.best_ask
    if bb is None or ba is None:
        return None
    return (bb + ba) / 2.0


def compute_total_book_depth(book: OutcomeBook) -> float:
    """Soma de price×size dos dois lados."""
    total = 0.0
    for lvl in book.bids:
        total += lvl.price * lvl.size
    for lvl in book.asks:
        total += lvl.price * lvl.size
    return total


def book_is_terminal(book: OutcomeBook) -> bool:
    """Book próximo de 0 ou 1 é terminal (já resolveu-se direcionalmente)."""
    bb = book.best_bid
    ba = book.best_ask
    if bb is None or ba is None:
        return True
    if bb < 0.02 or ba > 0.98:
        return True
    return False


def sum_yes_no_sanity(yes_book: OutcomeBook, no_book: Optional[OutcomeBook]) -> bool:
    """
    Checa yes_mid + no_mid ∈ [0.95, 1.05]. Se no_book for None, não dá pra validar — aceita.
    """
    if no_book is None:
        return True
    y_mid = compute_p_market(yes_book)
    n_mid = compute_p_market(no_book)
    if y_mid is None or n_mid is None:
        return False
    s = y_mid + n_mid
    return 0.95 <= s <= 1.05


# ─────────────────────────────────────────────────────
# Modelo: p_model pra cada tipo
# ─────────────────────────────────────────────────────

def compute_p_model(
    market: PolymarketMarket,
    spot: float,
    sigma_annual: float,
    tau_years: float,
) -> Optional[float]:
    """
    Retorna P_model para o outcome YES. Ou None se tipo não suportado.

    STRIKE_AT_TIME: Φ(d2) com direction_above.
    STRIKE_TOUCH: 2·Φ(d2) aprox.
    TWO_BARRIER: P(touch k_high first) como "YES" por convenção (maior strike = YES).
                 Se o convention for inverso no Polymarket, o operator classifica depois.
    """
    parsed = market.parsed
    if parsed.market_type == MarketType.STRIKE_AT_TIME:
        if parsed.strike_primary is None or parsed.direction_above is None:
            return None
        return p_model_at_time(
            spot=spot, strike=parsed.strike_primary,
            sigma_annual=sigma_annual, tau_years=tau_years,
            direction_above=parsed.direction_above,
        )
    if parsed.market_type == MarketType.STRIKE_TOUCH:
        if parsed.strike_primary is None or parsed.direction_above is None:
            return None
        return p_model_touch(
            spot=spot, strike=parsed.strike_primary,
            sigma_annual=sigma_annual, tau_years=tau_years,
            direction_above=parsed.direction_above,
        )
    if parsed.market_type == MarketType.TWO_BARRIER:
        if parsed.strike_primary is None or parsed.strike_secondary is None:
            return None
        p_high, _ = p_model_two_barrier(
            spot=spot, k_low=parsed.strike_primary, k_high=parsed.strike_secondary,
        )
        # Convenção: YES do Polymarket "hit X or Y first" resolve pro maior.
        # Se for inverso em algum mercado específico, filtrar por slug depois.
        return p_high
    return None


def compute_directional_edge(
    market: PolymarketMarket,
    spot: float,
    p_market: float,
) -> Optional[float]:
    """
    Fallback sub-60s: qual deveria ser p_market direcionalmente?

    - STRIKE_AT_TIME above: S>K → p ~ 1, S<K → p ~ 0.
    - STRIKE_AT_TIME below: invertido.
    - STRIKE_TOUCH: se já tocou (S passou K no lado certo), p=1.
    - TWO_BARRIER: se S acima de k_high → 1, abaixo de k_low → 0.
    """
    parsed = market.parsed
    if parsed.market_type in (MarketType.STRIKE_AT_TIME, MarketType.STRIKE_TOUCH):
        if parsed.strike_primary is None or parsed.direction_above is None:
            return None
        if parsed.direction_above:
            p_expected = 1.0 if spot >= parsed.strike_primary else 0.0
        else:
            p_expected = 1.0 if spot <= parsed.strike_primary else 0.0
        return p_expected - p_market
    if parsed.market_type == MarketType.TWO_BARRIER:
        if parsed.strike_primary is None or parsed.strike_secondary is None:
            return None
        if spot >= parsed.strike_secondary:
            return 1.0 - p_market
        if spot <= parsed.strike_primary:
            return 0.0 - p_market
        return None  # entre as barreiras, precisa do modelo
    return None


# ─────────────────────────────────────────────────────
# Detector principal
# ─────────────────────────────────────────────────────

@dataclass
class OpportunityWindow:
    """Rastreia janela aberta pra dedupe."""
    opportunity_id: int
    market_id: str
    direction_label: str
    opened_at_ms: int
    peak_abs_signal: float = 0.0
    tick_count: int = 0
    last_record: Optional[OpportunityRecord] = None


@dataclass
class DetectorState:
    """Estado interno do detector."""
    next_opportunity_id: int = 1
    open_windows: dict[tuple[str, str], OpportunityWindow] = field(default_factory=dict)
    last_close_ms: dict[tuple[str, str], int] = field(default_factory=dict)
    closed_records: list[OpportunityRecord] = field(default_factory=list)
    audit_records: list[dict] = field(default_factory=list)


def evaluate_market(
    market: PolymarketMarket,
    market_books: MarketBooks,
    spot: SpotQuote,
    vol: VolEstimate,
    state: DetectorState,
    now_ms: int,
    audit: bool = True,
) -> Optional[OpportunityRecord]:
    """
    Faz uma avaliação completa do mercado contra spot/vol.

    Retorna o record criado (ou atualizado) se houve evento de interesse, None caso contrário.
    """
    # 1. Freshness
    if not spot.is_fresh(now_ms):
        return _reject(market, "spot_stale", now_ms, state)
    if not market_books.both_fresh(now_ms):
        return _reject(market, "book_stale", now_ms, state)

    # 2. Books saudáveis
    yes_book = market_books.yes
    if book_is_terminal(yes_book):
        return _reject(market, "book_terminal", now_ms, state)
    if not sum_yes_no_sanity(yes_book, market_books.no):
        return _reject(market, "yes_no_sanity_fail", now_ms, state)
    book_depth = compute_total_book_depth(yes_book)
    if book_depth < MIN_BOOK_DEPTH_USD:
        return _reject(market, "depth_too_low", now_ms, state)

    # 3. Tempo até resolução
    tau_sec = compute_tau_sec(market, now_ms)
    if tau_sec <= 0:
        return _reject(market, "resolved", now_ms, state)

    strategy = classify_strategy(tau_sec)
    model_undefined = False
    if tau_sec < MODEL_UNDEFINED_TAU_SEC:
        model_undefined = True
        strategy = "sub60"
    elif strategy is None:
        return _reject(market, "tau_out_of_range", now_ms, state)

    # 4. Mid do YES
    p_market = compute_p_market(yes_book)
    if p_market is None:
        return _reject(market, "no_yes_mid", now_ms, state)

    # 5. Volatilidade
    if not vol.is_usable(min_candles=MIN_VOL_CANDLES):
        return _reject(market, "vol_insufficient", now_ms, state)

    sigma = vol.sigma_60  # type: ignore  # is_usable() garante não None

    # 6. P_model
    tau_years = tau_sec_to_years(tau_sec)
    s_t = spot.mid
    p_model: Optional[float] = None
    delta: float
    z: Optional[float] = None

    if model_undefined:
        # Lógica direcional
        directional = compute_directional_edge(market, s_t, p_market)
        if directional is None:
            return _reject(market, "sub60_undetermined", now_ms, state)
        delta = directional
        # z não definido sem modelo
    else:
        p_model = compute_p_model(market, s_t, sigma, tau_years)
        if p_model is None:
            return _reject(market, "p_model_undefined", now_ms, state)
        delta = p_model - p_market
        if market.parsed.strike_primary is not None and market.parsed.market_type != MarketType.TWO_BARRIER:
            try:
                z = compute_z_score(s_t, market.parsed.strike_primary, sigma, tau_years)
            except ValueError:
                z = None

    # 7. Confidence / urgency
    c = confidence_factor(z=z if z is not None else 0.0, k=CONFIDENCE_K, z_threshold=CONFIDENCE_Z_THRESHOLD)
    # τ_max depende da estratégia
    if strategy == "A":
        tau_max_years = tau_sec_to_years(STRATEGY_A_TAU_MAX_SEC)
    elif strategy == "B":
        tau_max_years = tau_sec_to_years(STRATEGY_A_TAU_MIN_SEC)  # B usa [60s, 15min], max=15min
    else:
        tau_max_years = tau_sec_to_years(MODEL_UNDEFINED_TAU_SEC)
    w = urgency_weight(tau_years, tau_max_years)

    signal: Optional[float] = None
    if not model_undefined:
        signal = delta * c * w

    # 8. Signals executáveis (ask/bid aware)
    yes_bid = yes_book.best_bid
    yes_ask = yes_book.best_ask
    signal_long: Optional[float] = None
    signal_short: Optional[float] = None
    if yes_ask is not None and p_model is not None:
        delta_long = p_model - yes_ask
        signal_long = delta_long * c * w
    if yes_bid is not None and p_model is not None:
        delta_short = yes_bid - p_model
        signal_short = delta_short * c * w

    # 9. Fees
    fee_dyn = polymarket_fee_dynamic(p_market, rate_peak=POLYMARKET_FEE_RATE_CRYPTO_PEAK)

    # 10. Slippage VWAP-$500
    slip_long = vwap_slippage(
        [(lvl.price, lvl.size) for lvl in yes_book.asks], notional_target=500.0,
    )
    slip_short = vwap_slippage(
        [(lvl.price, lvl.size) for lvl in yes_book.bids], notional_target=500.0,
    )

    # 11. Custo total + decisão
    total_cost = POLYMARKET_FEE_RATE_CRYPTO_PEAK + SAFETY_MARGIN
    if slip_long is not None:
        total_cost_long = total_cost + slip_long
    else:
        total_cost_long = None
    if slip_short is not None:
        total_cost_short = total_cost + slip_short
    else:
        total_cost_short = None

    abs_delta = abs(delta)
    flagged = False
    direction_label = "NONE"
    rejection = ""

    if abs_delta < MIN_DELTA_ABS:
        rejection = "delta_below_min"
    elif signal is None and not model_undefined:
        rejection = "signal_undefined"
    elif model_undefined:
        # Sub-60s: decisão baseada em delta direcional, não em P_model.
        # Signal implícito = |delta| (confidence C e urgency W aplicados apenas
        # pra contextualizar, mas sub-60s é binário: direção já está clara).
        if delta > 0:
            direction_label = "LONG_YES"
            # Custo: fee peak + safety + slippage ask side
            cost = POLYMARKET_FEE_RATE_CRYPTO_PEAK + SAFETY_MARGIN
            if slip_long is not None:
                cost += slip_long
            if abs_delta > cost:
                flagged = True
            else:
                rejection = "sub60_long_cost_exceeds_delta"
        else:
            direction_label = "SHORT_YES"
            cost = POLYMARKET_FEE_RATE_CRYPTO_PEAK + SAFETY_MARGIN
            if slip_short is not None:
                cost += slip_short
            if abs_delta > cost:
                flagged = True
            else:
                rejection = "sub60_short_cost_exceeds_delta"
    else:
        # Decisão direcional (caso com modelo)
        if delta > 0:
            # p_model > p_market → comprar YES (LONG)
            direction_label = "LONG_YES"
            if total_cost_long is not None and signal_long is not None and abs(signal_long) > total_cost_long:
                flagged = True
            else:
                rejection = "long_cost_exceeds_signal"
        else:
            # p_model < p_market → vender YES (SHORT)
            direction_label = "SHORT_YES"
            if total_cost_short is not None and signal_short is not None and abs(signal_short) > total_cost_short:
                flagged = True
            else:
                rejection = "short_cost_exceeds_signal"

    # 12. Record
    record = OpportunityRecord(
        opportunity_id=0,  # será atribuído se flagged
        detected_at_ms=now_ms,
        closed_at_ms=None,
        market_id=market.market_id,
        slug=market.slug,
        question=market.question,
        market_type=market.parsed.market_type.value,
        strategy=strategy,
        strategies_matched=strategy,  # alinhamento com spec, sem overlap nas janelas disjuntas
        symbol=market.parsed.symbol or "",
        tau_sec=tau_sec,
        spot_s=s_t,
        strike_primary=market.parsed.strike_primary or 0.0,
        strike_secondary=market.parsed.strike_secondary,
        direction_above=market.parsed.direction_above,
        sigma_60=vol.sigma_60,
        sigma_10=vol.sigma_10,
        p_model=p_model,
        p_market=p_market,
        delta=delta,
        z=z,
        c=c,
        w=w,
        signal=signal,
        signal_long=signal_long,
        signal_short=signal_short,
        fee_peak_polymarket=POLYMARKET_FEE_RATE_CRYPTO_PEAK,
        fee_dynamic_at_p=fee_dyn,
        slippage_long_frac=slip_long,
        slippage_short_frac=slip_short,
        book_depth_usd=book_depth,
        yes_best_bid=yes_bid,
        yes_best_ask=yes_ask,
        model_undefined=model_undefined,
        opportunity_flagged=flagged,
        direction_label=direction_label if flagged else "NONE",
        rejection_reason=rejection,
    )

    # 13. Audit trail (sempre, se audit=True)
    if audit:
        state.audit_records.append({
            "ts_ms": now_ms,
            "market_id": market.market_id,
            "strategy": strategy,
            "delta": delta,
            "flagged": flagged,
            "rejection": rejection,
        })

    # 14. Dedupe por janela
    if flagged:
        return _open_or_update_window(state, record, now_ms)
    else:
        return _close_window_if_open(state, market.market_id, direction_label, now_ms)


def _reject(
    market: PolymarketMarket, reason: str, now_ms: int, state: DetectorState,
) -> None:
    """Não emite record de oportunidade, mas fecha qualquer janela aberta."""
    for dir_label in ("LONG_YES", "SHORT_YES"):
        _close_window_if_open(state, market.market_id, dir_label, now_ms)
    state.audit_records.append({
        "ts_ms": now_ms,
        "market_id": market.market_id,
        "rejection": reason,
        "flagged": False,
    })
    return None


def _open_or_update_window(
    state: DetectorState,
    record: OpportunityRecord,
    now_ms: int,
) -> Optional[OpportunityRecord]:
    key = (record.market_id, record.direction_label)
    # Já aberta?
    if key in state.open_windows:
        win = state.open_windows[key]
        win.tick_count += 1
        abs_sig = abs(record.signal or record.delta)
        if abs_sig > win.peak_abs_signal:
            win.peak_abs_signal = abs_sig
        # Atualiza last_record
        updated = _with_id(record, win.opportunity_id)
        win.last_record = updated
        return None  # não é nova
    # Respeita gap mínimo de reabertura
    last_close = state.last_close_ms.get(key, 0)
    if now_ms - last_close < REOPEN_GAP_MS:
        return None
    # Abre nova
    new_id = state.next_opportunity_id
    state.next_opportunity_id += 1
    rec_with_id = _with_id(record, new_id)
    win = OpportunityWindow(
        opportunity_id=new_id,
        market_id=record.market_id,
        direction_label=record.direction_label,
        opened_at_ms=now_ms,
        peak_abs_signal=abs(record.signal or record.delta),
        tick_count=1,
        last_record=rec_with_id,
    )
    state.open_windows[key] = win
    return rec_with_id


def _close_window_if_open(
    state: DetectorState,
    market_id: str,
    direction_label: str,
    now_ms: int,
) -> None:
    key = (market_id, direction_label)
    if key not in state.open_windows:
        return None
    win = state.open_windows.pop(key)
    state.last_close_ms[key] = now_ms
    if win.last_record is not None:
        closed = _with_closed_at(win.last_record, now_ms)
        state.closed_records.append(closed)
    return None


def _with_id(rec: OpportunityRecord, new_id: int) -> OpportunityRecord:
    # dataclass frozen → cria novo via __dict__
    from dataclasses import replace
    return replace(rec, opportunity_id=new_id)


def _with_closed_at(rec: OpportunityRecord, closed_at_ms: int) -> OpportunityRecord:
    from dataclasses import replace
    return replace(rec, closed_at_ms=closed_at_ms)
