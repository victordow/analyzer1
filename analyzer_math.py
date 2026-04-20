"""
analyzer_math.py — núcleo matemático do detector de oportunidades cross-platform.

Fórmulas:
- strike_price_at_time (European): P = Φ(d2)
- strike_price_touch (American aprox): P = min(1, 2·Φ(d2))  -- válido para r=0, drift=-σ²/2
- two_barrier ("hit X ou Y primeiro", com S entre X e Y):
    Sob GBM risk-neutral com r=0 e drift -σ²/2:
    P(toca Y antes de X) = (S^α - X^α) / (Y^α - X^α), α = 2μ/σ² - 1, μ = r - σ²/2
    Com r=0: μ = -σ²/2 → α = -2 (aproximação clássica)
    Resultado direto: P(toca Y) = ln(S/X) / ln(Y/X)  -- limite quando α=0 (aritmético)
    Uso a fórmula log-linear: P = ln(S/X) / ln(Y/X). Dá resultado sensato entre 0 e 1.

Parse:
- Classificação em 4 tipos por regex + keywords
- Strike com sanity vs S_t

Nenhuma invenção: constantes são as aprovadas pelo operador.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


# ─────────────────────────────────────────────────────
# Enums + Dataclasses
# ─────────────────────────────────────────────────────

class MarketType(Enum):
    STRIKE_AT_TIME = "strike_at_time"      # "Will BTC be above $X at 5PM ET?"
    STRIKE_TOUCH = "strike_touch"          # "BTC reach/hit $X by date Y"
    TWO_BARRIER = "two_barrier"            # "BTC hit $X or $Y first"
    UP_DOWN_IMPLICIT = "up_down_implicit"  # "BTC Up or Down - April 20, 5PM ET"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ParsedMarket:
    market_type: MarketType
    symbol: Optional[str]                  # "BTC", "ETH", "SOL", etc
    strike_primary: Optional[float]        # K principal (at_time/touch) ou K_low (two_barrier)
    strike_secondary: Optional[float]      # K_high (two_barrier), None nos outros
    direction_above: Optional[bool]        # True se "above/hit", False se "below"
    parse_confidence: float                # 0-1, 1 = certeza alta
    parse_notes: str                       # razão de rejeição ou detalhes


# ─────────────────────────────────────────────────────
# Constants — símbolos aceitos e scale sanity
# ─────────────────────────────────────────────────────

# Aprovado pelo operador: BTC/ETH/SOL/XRP/DOGE/BNB
ACCEPTED_SYMBOLS: dict[str, str] = {
    "BTC": "BTCUSDT",
    "BITCOIN": "BTCUSDT",
    "ETH": "ETHUSDT",
    "ETHEREUM": "ETHUSDT",
    "SOL": "SOLUSDT",
    "SOLANA": "SOLUSDT",
    "XRP": "XRPUSDT",
    "DOGE": "DOGEUSDT",
    "DOGECOIN": "DOGEUSDT",
    "BNB": "BNBUSDT",
}


# ─────────────────────────────────────────────────────
# Parse de strike — formatos suportados
#   $95k, $95K, $95,000, $95000, $95.5k, $0.85
# ─────────────────────────────────────────────────────

_NUM_PATTERN = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\s*([kKmM]?)",
    flags=re.UNICODE,
)


def parse_dollar_amount(s: str) -> Optional[float]:
    """Extrai primeiro valor em dólar como float. Suporta k/K/m/M suffix e vírgulas."""
    m = _NUM_PATTERN.search(s)
    if not m:
        return None
    num_str = m.group(1).replace(",", "")
    try:
        val = float(num_str)
    except ValueError:
        return None
    suffix = m.group(2).lower()
    if suffix == "k":
        val *= 1_000
    elif suffix == "m":
        val *= 1_000_000
    return val


def parse_all_dollar_amounts(s: str) -> list[float]:
    """Todos os valores em dólar na string, em ordem."""
    out = []
    for m in _NUM_PATTERN.finditer(s):
        num_str = m.group(1).replace(",", "")
        try:
            val = float(num_str)
        except ValueError:
            continue
        suffix = m.group(2).lower()
        if suffix == "k":
            val *= 1_000
        elif suffix == "m":
            val *= 1_000_000
        out.append(val)
    return out


# ─────────────────────────────────────────────────────
# Classificação + parse de mercado
# ─────────────────────────────────────────────────────

_SYMBOL_PATTERN = re.compile(
    r"\b(BTC|BITCOIN|ETH|ETHEREUM|SOL|SOLANA|XRP|DOGE|DOGECOIN|BNB)\b",
    flags=re.IGNORECASE,
)

_UP_DOWN_PATTERN = re.compile(
    r"\bup\s+or\s+down\b",
    flags=re.IGNORECASE,
)

_TOUCH_KEYWORDS = re.compile(
    r"\b(reach|hit|touch)\b",
    flags=re.IGNORECASE,
)

_AT_TIME_KEYWORDS = re.compile(
    r"\b(at|close\s+above|close\s+below|closing\s+above|closing\s+below|be\s+above|be\s+below|end\s+above|end\s+below)\b",
    flags=re.IGNORECASE,
)

_FIRST_KEYWORD = re.compile(
    r"\b(first)\b",
    flags=re.IGNORECASE,
)

_BELOW_KEYWORDS = re.compile(
    r"\b(below|under|less\s+than)\b",
    flags=re.IGNORECASE,
)

_ABOVE_KEYWORDS = re.compile(
    r"\b(above|over|greater\s+than|more\s+than|higher\s+than)\b",
    flags=re.IGNORECASE,
)


def detect_symbol(title: str) -> Optional[str]:
    m = _SYMBOL_PATTERN.search(title)
    if not m:
        return None
    raw = m.group(1).upper()
    return ACCEPTED_SYMBOLS.get(raw)


def classify_and_parse(title: str, spot_for_sanity: float | None = None) -> ParsedMarket:
    """
    Retorna ParsedMarket completo. Se parse_confidence < 0.5, rejeitar no consumer.

    spot_for_sanity: S_t atual (opcional) — se fornecido, rejeita K absurdo.
    """
    symbol = detect_symbol(title)
    if symbol is None:
        return ParsedMarket(
            market_type=MarketType.UNKNOWN, symbol=None,
            strike_primary=None, strike_secondary=None,
            direction_above=None, parse_confidence=0.0,
            parse_notes="no crypto symbol matched",
        )

    # Tipo 1: UP/DOWN implícito
    if _UP_DOWN_PATTERN.search(title):
        return ParsedMarket(
            market_type=MarketType.UP_DOWN_IMPLICIT, symbol=symbol,
            strike_primary=None, strike_secondary=None,
            direction_above=None, parse_confidence=1.0,
            parse_notes="up/down implicit (Chainlink feed required, skip in MVP)",
        )

    dollars = parse_all_dollar_amounts(title)

    # Tipo 2: TWO_BARRIER — "hit $X or $Y first"
    if _FIRST_KEYWORD.search(title) and len(dollars) >= 2:
        d_sorted = sorted(dollars[:2])
        k_low, k_high = d_sorted[0], d_sorted[1]
        mkt = ParsedMarket(
            market_type=MarketType.TWO_BARRIER, symbol=symbol,
            strike_primary=k_low, strike_secondary=k_high,
            direction_above=None, parse_confidence=1.0,
            parse_notes="two-barrier (touch first)",
        )
        return _apply_spot_sanity(mkt, spot_for_sanity)

    # Tipos 3 e 4: strike único (at_time ou touch)
    if len(dollars) < 1:
        return ParsedMarket(
            market_type=MarketType.UNKNOWN, symbol=symbol,
            strike_primary=None, strike_secondary=None,
            direction_above=None, parse_confidence=0.0,
            parse_notes="no dollar amount found",
        )

    k = dollars[0]

    # Direção: above/below?
    has_above = bool(_ABOVE_KEYWORDS.search(title))
    has_below = bool(_BELOW_KEYWORDS.search(title))
    if has_above and not has_below:
        direction = True
    elif has_below and not has_above:
        direction = False
    else:
        # Se keyword "reach/hit/touch" sozinha, assume above por convenção Polymarket
        if _TOUCH_KEYWORDS.search(title):
            direction = True
        else:
            return ParsedMarket(
                market_type=MarketType.UNKNOWN, symbol=symbol,
                strike_primary=k, strike_secondary=None,
                direction_above=None, parse_confidence=0.3,
                parse_notes="ambiguous direction (above/below not found)",
            )

    # Tipo STRIKE_AT_TIME se tem keyword "at X PM ET" / "close above" etc
    if _AT_TIME_KEYWORDS.search(title):
        mkt_type = MarketType.STRIKE_AT_TIME
        notes = "european-style (at close of window)"
    elif _TOUCH_KEYWORDS.search(title):
        mkt_type = MarketType.STRIKE_TOUCH
        notes = "american-style (touch any time)"
    else:
        # Fallback conservador: trata como touch (mais inclusivo)
        mkt_type = MarketType.STRIKE_TOUCH
        notes = "assumed touch (no explicit at-time keyword)"

    mkt = ParsedMarket(
        market_type=mkt_type, symbol=symbol,
        strike_primary=k, strike_secondary=None,
        direction_above=direction, parse_confidence=1.0,
        parse_notes=notes,
    )
    return _apply_spot_sanity(mkt, spot_for_sanity)


def _apply_spot_sanity(mkt: ParsedMarket, spot: float | None) -> ParsedMarket:
    """Rejeita se K/S fora de [0.3, 3.0] — provável parse errado ou mercado absurdo."""
    if spot is None or mkt.strike_primary is None:
        return mkt
    ratio = mkt.strike_primary / spot
    if not (0.3 <= ratio <= 3.0):
        return ParsedMarket(
            market_type=mkt.market_type, symbol=mkt.symbol,
            strike_primary=mkt.strike_primary, strike_secondary=mkt.strike_secondary,
            direction_above=mkt.direction_above, parse_confidence=0.0,
            parse_notes=f"strike {mkt.strike_primary} fails sanity vs spot {spot} (ratio {ratio:.2f})",
        )
    if mkt.strike_secondary is not None:
        ratio2 = mkt.strike_secondary / spot
        if not (0.3 <= ratio2 <= 3.0):
            return ParsedMarket(
                market_type=mkt.market_type, symbol=mkt.symbol,
                strike_primary=mkt.strike_primary, strike_secondary=mkt.strike_secondary,
                direction_above=mkt.direction_above, parse_confidence=0.0,
                parse_notes=f"strike_secondary {mkt.strike_secondary} fails sanity vs spot {spot}",
            )
    return mkt


# ─────────────────────────────────────────────────────
# Modelo matemático — Φ, d2, probabilidades
# ─────────────────────────────────────────────────────

def norm_cdf(x: float) -> float:
    """CDF da normal padrão via math.erf (sem dep de scipy)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def compute_d2(spot: float, strike: float, sigma_annual: float, tau_years: float) -> float:
    """
    d2 de Black-Scholes para binary, com r=0:
        d2 = [ln(S/K) - σ²τ/2] / (σ·√τ)
    Requer: spot>0, strike>0, sigma>0, tau>0.
    """
    if spot <= 0 or strike <= 0 or sigma_annual <= 0 or tau_years <= 0:
        raise ValueError(
            f"compute_d2: inputs invalidos spot={spot} K={strike} sigma={sigma_annual} tau={tau_years}"
        )
    return (math.log(spot / strike) - 0.5 * sigma_annual**2 * tau_years) / (
        sigma_annual * math.sqrt(tau_years)
    )


def compute_z_score(spot: float, strike: float, sigma_annual: float, tau_years: float) -> float:
    """
    z = ln(S/K) / (σ√τ) — clareza direcional em unidades de volatilidade.
    |z| grande → direção clara; |z| < 0.5 → genuinamente incerto.
    """
    if spot <= 0 or strike <= 0 or sigma_annual <= 0 or tau_years <= 0:
        raise ValueError("compute_z_score: inputs invalidos")
    return math.log(spot / strike) / (sigma_annual * math.sqrt(tau_years))


def p_model_at_time(
    spot: float, strike: float, sigma_annual: float, tau_years: float,
    direction_above: bool,
) -> float:
    """
    European-style binary. P(S_T > K) = Φ(d2). Se direção for "below", usa 1-P.
    """
    d2 = compute_d2(spot, strike, sigma_annual, tau_years)
    p_above = norm_cdf(d2)
    return p_above if direction_above else (1.0 - p_above)


def p_model_touch(
    spot: float, strike: float, sigma_annual: float, tau_years: float,
    direction_above: bool,
) -> float:
    """
    American-style touch, aproximação clássica para r=0: P(touch) = min(1, 2·Φ(d2_touch)).
    Onde d2_touch usa o sinal correto dependendo se K está acima ou abaixo de S.

    Obs: para r=0 e drift negativo pequeno, a aproximação 2Φ(d2) é um limite superior
    razoável. Para maior precisão usaria reflexão, mas operador aceitou aproximação.
    """
    if direction_above:
        # K acima de S: quero prob de atingir K a qualquer momento
        if strike <= spot:
            return 1.0  # já tocou
        d = compute_d2(spot, strike, sigma_annual, tau_years)
        return min(1.0, 2.0 * norm_cdf(d))
    else:
        if strike >= spot:
            return 1.0
        d = compute_d2(strike, spot, sigma_annual, tau_years)  # inverte S,K
        return min(1.0, 2.0 * norm_cdf(d))


def p_model_two_barrier(
    spot: float, k_low: float, k_high: float,
) -> tuple[float, float]:
    """
    Probabilidade de tocar k_high antes de k_low, assumindo GBM com drift=-σ²/2.
    Aproximação log-linear (válida para r=0, simetria em log-price):
        P(toca k_high primeiro) = ln(S/k_low) / ln(k_high/k_low)
        P(toca k_low primeiro)  = 1 - P_high

    Requer: 0 < k_low < S < k_high. Se S fora do intervalo: 1 ou 0 determinístico.
    Retorna: (p_high, p_low)
    """
    if k_low <= 0 or k_high <= k_low or spot <= 0:
        raise ValueError(f"two_barrier inputs invalidos: S={spot} low={k_low} high={k_high}")
    if spot >= k_high:
        return (1.0, 0.0)
    if spot <= k_low:
        return (0.0, 1.0)
    p_high = math.log(spot / k_low) / math.log(k_high / k_low)
    return (p_high, 1.0 - p_high)


# ─────────────────────────────────────────────────────
# Confidence / weight / signal
# ─────────────────────────────────────────────────────

def confidence_factor(z: float, k: float = 2.0, z_threshold: float = 1.0) -> float:
    """C = sigmoid(k · (|z| - z_threshold)). Clip |z| a 10 pra evitar overflow."""
    z_abs = min(abs(z), 10.0)
    x = k * (z_abs - z_threshold)
    # sigmoid estável
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    else:
        return math.exp(x) / (1.0 + math.exp(x))


def urgency_weight(tau_years: float, tau_max_years: float) -> float:
    """W = 1 - tau/tau_max, clipped a [0, 1]."""
    if tau_max_years <= 0:
        return 0.0
    w = 1.0 - (tau_years / tau_max_years)
    return max(0.0, min(1.0, w))


# ─────────────────────────────────────────────────────
# Fee Polymarket (dinâmico) — docs.polymarket.com/trading/fees
# ─────────────────────────────────────────────────────

# Approved pelo operador em 20/abr/2026 após verificar docs oficiais.
POLYMARKET_FEE_RATE_CRYPTO_PEAK = 0.072   # 7.2% peak em p=0.5


def polymarket_fee_dynamic(price: float, rate_peak: float = POLYMARKET_FEE_RATE_CRYPTO_PEAK) -> float:
    """
    Fee como fração do notional. Peak em p=0.5, zero nos extremos.
        fee(p) = rate_peak · p · (1-p) / 0.25
    Em p=0.5 → rate_peak. Em p=0 ou p=1 → 0.
    """
    p = max(0.0, min(1.0, price))
    return rate_peak * p * (1.0 - p) / 0.25


# ─────────────────────────────────────────────────────
# Slippage — VWAP-$500
# ─────────────────────────────────────────────────────

def vwap_slippage(
    levels: list[tuple[float, float]],
    notional_target: float = 500.0,
) -> Optional[float]:
    """
    Dado levels [(price, size)] ordenados pelo lado correto (asks asc pra buy, bids desc pra sell),
    calcula fração de slippage vs best price para preencher notional_target USD.

    Retorna (avg_price - best_price) / best_price, ou None se liquidez insuficiente.
    """
    if not levels or notional_target <= 0:
        return None
    best_price = levels[0][0]
    remaining = notional_target
    total_cost = 0.0
    total_shares = 0.0
    for price, size in levels:
        if price <= 0 or size <= 0:
            continue
        level_notional = price * size
        take = min(remaining, level_notional)
        shares = take / price
        total_cost += take
        total_shares += shares
        remaining -= take
        if remaining <= 0:
            break
    if remaining > 0:
        return None  # liquidez insuficiente
    if total_shares <= 0 or best_price <= 0:
        return None
    avg_price = total_cost / total_shares
    return abs(avg_price - best_price) / best_price
