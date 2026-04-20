"""
test_binance_client.py — testa lógica pura do binance_client (sem rede).

Cobre:
- parse_book_ticker com payloads wrap (multi-stream) e plain (single)
- Rejeição de payloads malformed
- vol_estimate_from_klines com klines reais e com incompletas
- compute_sigma_annual
- build_ws_url

Rodar: python3 test_binance_client.py
"""
from __future__ import annotations

import math
import sys
import time
import traceback

# Stubs se libs não instaladas localmente
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
    mod.connect = lambda *a, **kw: None
    sys.modules["websockets"] = mod
    sys.modules["websockets.exceptions"] = exc

from binance_client import (
    build_ws_url,
    compute_sigma_annual,
    parse_book_ticker,
    vol_estimate_from_klines,
    MINUTES_PER_YEAR,
)


def approx(a: float, b: float, tol: float = 1e-6) -> bool:
    return abs(a - b) <= tol

def rel_approx(a: float, b: float, tol: float = 1e-3) -> bool:
    if b == 0:
        return abs(a) <= tol
    return abs((a - b) / b) <= tol


# ─────────────────────────────────────────────────────
# PARSE BOOK TICKER
# ─────────────────────────────────────────────────────

def test_parse_multi_stream_wrap() -> None:
    """Formato real multi-stream: {"stream":..., "data":{...}}"""
    msg = {
        "stream": "btcusdt@bookTicker",
        "data": {
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "95000.50",
            "B": "1.5",
            "a": "95001.00",
            "A": "2.0",
        },
    }
    q = parse_book_ticker(msg)
    assert q is not None
    assert q.symbol == "BTCUSDT"
    assert q.bid_price == 95000.5
    assert q.ask_price == 95001.0
    assert q.bid_qty == 1.5
    assert q.ask_qty == 2.0

def test_parse_single_stream_plain() -> None:
    """Sem wrap."""
    msg = {"u": 1, "s": "ETHUSDT", "b": "3500.00", "B": "10", "a": "3500.50", "A": "5"}
    q = parse_book_ticker(msg)
    assert q is not None
    assert q.symbol == "ETHUSDT"

def test_parse_rejects_missing_fields() -> None:
    assert parse_book_ticker({"s": "BTCUSDT"}) is None
    assert parse_book_ticker({}) is None
    assert parse_book_ticker({"data": "not a dict"}) is None

def test_parse_rejects_non_positive() -> None:
    msg = {"s": "BTCUSDT", "b": "0", "B": "1", "a": "95001", "A": "1"}
    assert parse_book_ticker(msg) is None

def test_parse_rejects_crossed_book() -> None:
    """ask < bid → rejeita."""
    msg = {"s": "BTCUSDT", "b": "95001", "B": "1", "a": "95000", "A": "1"}
    assert parse_book_ticker(msg) is None

def test_parse_quote_computes_mid() -> None:
    msg = {"s": "BTCUSDT", "b": "100", "B": "1", "a": "102", "A": "1"}
    q = parse_book_ticker(msg)
    assert q is not None
    assert q.mid == 101.0


# ─────────────────────────────────────────────────────
# COMPUTE SIGMA ANNUAL
# ─────────────────────────────────────────────────────

def test_sigma_flat_is_zero() -> None:
    """Preços constantes → σ = 0."""
    closes = [100.0] * 10
    s = compute_sigma_annual(closes)
    assert s == 0.0

def test_sigma_requires_minimum() -> None:
    """Menos de 2 closes → None."""
    assert compute_sigma_annual([]) is None
    assert compute_sigma_annual([100.0]) is None

def test_sigma_known_input() -> None:
    """
    Input com retornos conhecidos: log-returns = [+0.01, -0.01, +0.01, -0.01]
    stdev = sqrt(var). var de amostra com n=4:
    mean=0, deviations=[0.01,-0.01,0.01,-0.01], sum_sq=0.0004, var=0.0004/3≈0.0001333
    stdev ≈ 0.01155
    σ_annual = 0.01155 × √525600 ≈ 8.375
    """
    # Construir closes que dão esses log returns: log(c[i]/c[i-1])
    closes = [100.0]
    returns = [0.01, -0.01, 0.01, -0.01]
    for r in returns:
        closes.append(closes[-1] * math.exp(r))
    s = compute_sigma_annual(closes)
    assert s is not None
    # Valor esperado: statistics.stdev([0.01,-0.01,0.01,-0.01]) × sqrt(525600)
    import statistics
    expected = statistics.stdev(returns) * math.sqrt(MINUTES_PER_YEAR)
    assert rel_approx(s, expected, tol=1e-6), f"got {s}, expected {expected}"

def test_sigma_skips_non_positive() -> None:
    """Closes <=0 são filtrados antes."""
    closes = [100.0, 101.0, 0.0, -5.0, 102.0]
    s = compute_sigma_annual(closes)
    # Válidos: [100, 101, 102] → log returns entre eles
    assert s is not None
    assert s > 0


# ─────────────────────────────────────────────────────
# VOL ESTIMATE FROM KLINES
# ─────────────────────────────────────────────────────

def _make_kline(open_time: int, close: float, close_time: int) -> list:
    """Formato Binance: [openTime, open, high, low, close, volume, closeTime, ...]"""
    return [open_time, "0", "0", "0", str(close), "0", close_time, "0", 0, "0", "0", "0"]

def test_vol_estimate_basic() -> None:
    """10 klines completas, todas fechadas antes de now."""
    now = 10_000_000
    klines = []
    for i in range(10):
        close_time = now - (10 - i) * 60_000  # todas no passado
        open_time = close_time - 60_000
        price = 100.0 * (1.01 ** i)  # sobe 1% por minuto
        klines.append(_make_kline(open_time, price, close_time))

    ve = vol_estimate_from_klines("BTCUSDT", klines, now_ms=now)
    assert ve.symbol == "BTCUSDT"
    assert ve.sigma_60 is not None
    assert ve.sigma_60 > 0
    assert ve.n_valid_60 == 10
    assert ve.n_valid_10 == 10
    assert ve.sigma_10 is not None

def test_vol_estimate_rejects_incomplete() -> None:
    """Última kline com closeTime > now → deve ser excluída."""
    now = 10_000_000
    klines = []
    for i in range(5):
        close_time = now - (5 - i) * 60_000
        open_time = close_time - 60_000
        klines.append(_make_kline(open_time, 100.0 + i, close_time))
    # Adiciona kline "incompleta" (close_time no futuro)
    klines.append(_make_kline(now, 200.0, now + 60_000))

    ve = vol_estimate_from_klines("BTCUSDT", klines, now_ms=now)
    # Só 5 klines válidas
    assert ve.n_valid_60 == 5

def test_vol_estimate_empty_input() -> None:
    ve = vol_estimate_from_klines("BTCUSDT", [], now_ms=10_000_000)
    assert ve.sigma_60 is None
    assert ve.sigma_10 is None
    assert ve.n_valid_60 == 0

def test_vol_estimate_regime_divergence() -> None:
    """
    Preços crescem devagar por 50 minutos, depois explodem por 10 minutos.
    σ_10 >> σ_60, regime_divergence alto.
    """
    now = 10_000_000
    klines = []
    # Fase 1: 50 klines com ±0.01% de variação
    price = 100.0
    for i in range(50):
        close_time = now - (60 - i) * 60_000
        open_time = close_time - 60_000
        price *= math.exp(0.0001 if i % 2 == 0 else -0.0001)
        klines.append(_make_kline(open_time, price, close_time))
    # Fase 2: 10 klines com ±2% de variação
    for i in range(10):
        close_time = now - (10 - i) * 60_000
        open_time = close_time - 60_000
        price *= math.exp(0.02 if i % 2 == 0 else -0.02)
        klines.append(_make_kline(open_time, price, close_time))

    ve = vol_estimate_from_klines("BTCUSDT", klines, now_ms=now)
    assert ve.sigma_60 is not None and ve.sigma_10 is not None
    assert ve.regime_divergence is not None
    assert ve.regime_divergence > 0.5, f"divergência esperada alta, got {ve.regime_divergence}"

def test_vol_estimate_is_usable() -> None:
    """is_usable precisa de sigma válido E >= min_candles."""
    now = 10_000_000
    # 25 klines → abaixo do mínimo 30
    klines = [_make_kline(now - (25-i)*60_000, 100.0 * (1.01 ** i), now - (25-i)*60_000 + 60_000 - 1)
              for i in range(25)]
    ve = vol_estimate_from_klines("BTCUSDT", klines, now_ms=now)
    assert not ve.is_usable(min_candles=30)

    # 40 klines → passa
    klines2 = [_make_kline(now - (40-i)*60_000, 100.0 * (1.01 ** i), now - (40-i)*60_000 + 60_000 - 1)
               for i in range(40)]
    ve2 = vol_estimate_from_klines("BTCUSDT", klines2, now_ms=now)
    assert ve2.is_usable(min_candles=30)


# ─────────────────────────────────────────────────────
# BUILD WS URL
# ─────────────────────────────────────────────────────

def test_build_ws_url_single() -> None:
    url = build_ws_url(["BTCUSDT"])
    assert "btcusdt@bookTicker" in url
    assert url.startswith("wss://data-stream.binance.vision")

def test_build_ws_url_multi() -> None:
    url = build_ws_url(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    assert "btcusdt@bookTicker/ethusdt@bookTicker/solusdt@bookTicker" in url

def test_build_ws_url_lowercases() -> None:
    url = build_ws_url(["BtcUsdt"])
    assert "btcusdt@bookTicker" in url


# ─────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────

def run_all() -> int:
    tests = [
        test_parse_multi_stream_wrap,
        test_parse_single_stream_plain,
        test_parse_rejects_missing_fields,
        test_parse_rejects_non_positive,
        test_parse_rejects_crossed_book,
        test_parse_quote_computes_mid,
        test_sigma_flat_is_zero,
        test_sigma_requires_minimum,
        test_sigma_known_input,
        test_sigma_skips_non_positive,
        test_vol_estimate_basic,
        test_vol_estimate_rejects_incomplete,
        test_vol_estimate_empty_input,
        test_vol_estimate_regime_divergence,
        test_vol_estimate_is_usable,
        test_build_ws_url_single,
        test_build_ws_url_multi,
        test_build_ws_url_lowercases,
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
