"""
binance_client.py — cliente de dados Binance para o analyzer.

Responsabilidades:
- Manter S_t fresco via WebSocket bookTicker (multi-stream em uma conexão).
- Buscar candles 1min via REST para estimar σ_annual (janelas 60 e 10).
- Detectar staleness e gap em dados.
- Reconectar WS a cada 24h forçado + backoff em falhas.
- Medir drift de clock contra server time da Binance.

Endpoints:
- WS market data only: wss://data-stream.binance.vision/stream?streams=...
- REST klines: https://api.binance.com/api/v3/klines
- REST time: https://api.binance.com/api/v3/time (para drift)

Sem invenção: todos os timestamps/limites vêm da spec oficial da Binance confirmada.
"""
from __future__ import annotations

import asyncio
import json
import math
import time
import statistics
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
    HAS_WS = True
except ImportError:
    HAS_WS = False
    class ConnectionClosed(Exception):
        pass


# ─────────────────────────────────────────────────────
# Endpoints (oficiais, confirmados em pesquisa)
# ─────────────────────────────────────────────────────

BINANCE_WS_BASE = "wss://data-stream.binance.vision"
BINANCE_REST_BASE = "https://api.binance.com"
WS_RECONNECT_HARD_LIMIT_SEC = 23 * 3600  # Binance força disconnect a cada 24h, reconectamos antes


# ─────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────

@dataclass
class SpotQuote:
    """Top-of-book de um símbolo Binance."""
    symbol: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    last_update_ms: int  # wall clock local quando recebemos a msg

    @property
    def mid(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    def is_fresh(self, now_ms: int, max_age_ms: int = 10_000) -> bool:
        return (now_ms - self.last_update_ms) <= max_age_ms


@dataclass
class VolEstimate:
    """
    σ anualizada + metadados pra auditoria.

    sigma_60: baseado em 60 candles 1min recentes.
    sigma_10: baseado em últimos 10 candles (detecta mudança de regime).
    n_valid_60: quantas candles de fato entraram no cálculo (rejeita last incompleta).
    """
    symbol: str
    sigma_60: Optional[float]
    sigma_10: Optional[float]
    n_valid_60: int
    n_valid_10: int
    computed_at_ms: int
    # regime_divergence = |sigma_10 - sigma_60| / sigma_60, quando ambos existem
    regime_divergence: Optional[float]

    def is_usable(self, min_candles: int = 30) -> bool:
        return self.sigma_60 is not None and self.n_valid_60 >= min_candles


# ─────────────────────────────────────────────────────
# Volatility estimation
# ─────────────────────────────────────────────────────

MINUTES_PER_YEAR = 525_600  # 365 * 24 * 60


def compute_sigma_annual(closes: list[float]) -> Optional[float]:
    """
    Realized vol anualizada a partir de closes 1min.
    σ_annual = std(log_returns) × √(525600)

    Requer no mínimo 2 closes válidos (>0).
    Retorna None se input insuficiente.
    """
    valid = [c for c in closes if c is not None and c > 0]
    if len(valid) < 2:
        return None
    log_returns = []
    for i in range(1, len(valid)):
        r = math.log(valid[i] / valid[i - 1])
        log_returns.append(r)
    if len(log_returns) < 2:
        return None
    try:
        # stdev com n-1 no denominador (sample std)
        sd = statistics.stdev(log_returns)
    except statistics.StatisticsError:
        return None
    return sd * math.sqrt(MINUTES_PER_YEAR)


def vol_estimate_from_klines(symbol: str, klines: list[list], now_ms: int) -> VolEstimate:
    """
    klines: resposta crua do endpoint /api/v3/klines.
    Formato da Binance: [openTime, open, high, low, close, volume, closeTime, ...]

    Regra anti-bug: rejeita a kline mais recente se closeTime > now_ms (incompleta).
    """
    if not klines:
        return VolEstimate(symbol=symbol, sigma_60=None, sigma_10=None,
                           n_valid_60=0, n_valid_10=0, computed_at_ms=now_ms,
                           regime_divergence=None)

    # Filtra incompletas (closeTime está no índice 6)
    complete = []
    for k in klines:
        if len(k) < 7:
            continue
        close_time = int(k[6])
        if close_time > now_ms:
            continue  # kline ainda sendo formada, pula
        try:
            close_price = float(k[4])
        except (ValueError, TypeError):
            continue
        if close_price <= 0:
            continue
        complete.append(close_price)

    # σ_60: últimas 60 candles completas
    closes_60 = complete[-60:] if len(complete) >= 2 else complete
    sigma_60 = compute_sigma_annual(closes_60)
    n_valid_60 = len(closes_60)

    # σ_10: últimas 10 candles completas
    closes_10 = complete[-10:] if len(complete) >= 2 else complete
    sigma_10 = compute_sigma_annual(closes_10)
    n_valid_10 = len(closes_10)

    divergence = None
    if sigma_60 and sigma_10 and sigma_60 > 0:
        divergence = abs(sigma_10 - sigma_60) / sigma_60

    return VolEstimate(
        symbol=symbol, sigma_60=sigma_60, sigma_10=sigma_10,
        n_valid_60=n_valid_60, n_valid_10=n_valid_10,
        computed_at_ms=now_ms, regime_divergence=divergence,
    )


# ─────────────────────────────────────────────────────
# REST client
# ─────────────────────────────────────────────────────

async def fetch_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = 60,
    client: Optional[Any] = None,
) -> list[list]:
    """
    GET /api/v3/klines?symbol=BTCUSDT&interval=1m&limit=60

    Retorna lista crua de klines. Se falhar, retorna [].
    """
    if not HAS_HTTPX:
        raise RuntimeError("httpx not installed")
    url = f"{BINANCE_REST_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    owns_client = client is None
    try:
        if owns_client:
            client = httpx.AsyncClient(timeout=10.0)
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return []
        return data
    except Exception:
        return []
    finally:
        if owns_client and client is not None:
            await client.aclose()


async def fetch_server_time(client: Optional[Any] = None) -> Optional[int]:
    """GET /api/v3/time — retorna serverTime em ms, ou None se falhar."""
    if not HAS_HTTPX:
        return None
    url = f"{BINANCE_REST_BASE}/api/v3/time"
    owns_client = client is None
    try:
        if owns_client:
            client = httpx.AsyncClient(timeout=5.0)
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        st = data.get("serverTime")
        return int(st) if st else None
    except Exception:
        return None
    finally:
        if owns_client and client is not None:
            await client.aclose()


async def measure_clock_drift_ms(client: Optional[Any] = None) -> Optional[int]:
    """
    Drift = local_ms - binance_server_ms, após account pra RTT/2.
    Positivo = relógio local adiantado.
    """
    if not HAS_HTTPX:
        return None
    owns_client = client is None
    try:
        if owns_client:
            client = httpx.AsyncClient(timeout=5.0)
        t0 = int(time.time() * 1000)
        server_ms = await fetch_server_time(client)
        t1 = int(time.time() * 1000)
        if server_ms is None:
            return None
        rtt_half = (t1 - t0) // 2
        local_est = (t0 + t1) // 2
        return local_est - server_ms
    finally:
        if owns_client and client is not None:
            await client.aclose()


# ─────────────────────────────────────────────────────
# WebSocket bookTicker handler
# ─────────────────────────────────────────────────────

def parse_book_ticker(msg: dict) -> Optional[SpotQuote]:
    """
    Multi-stream payload:
        {"stream": "btcusdt@bookTicker", "data": {"u":..., "s":"BTCUSDT", "b":..., "B":..., "a":..., "A":...}}

    Ou single-stream (fallback):
        {"u":..., "s":"BTCUSDT", "b":..., "B":..., "a":..., "A":...}
    """
    data = msg.get("data") if "data" in msg else msg
    if not isinstance(data, dict):
        return None
    try:
        symbol = str(data["s"]).upper()
        bid_price = float(data["b"])
        bid_qty = float(data["B"])
        ask_price = float(data["a"])
        ask_qty = float(data["A"])
    except (KeyError, ValueError, TypeError):
        return None
    if bid_price <= 0 or ask_price <= 0 or ask_price < bid_price:
        return None
    now_ms = int(time.time() * 1000)
    return SpotQuote(
        symbol=symbol, bid_price=bid_price, bid_qty=bid_qty,
        ask_price=ask_price, ask_qty=ask_qty, last_update_ms=now_ms,
    )


def build_ws_url(symbols: list[str]) -> str:
    """Constrói URL combinada: wss://.../stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/..."""
    streams = "/".join(f"{s.lower()}@bookTicker" for s in symbols)
    return f"{BINANCE_WS_BASE}/stream?streams={streams}"


async def run_book_ticker_stream(
    symbols: list[str],
    on_quote: Callable[[SpotQuote], None],
    shutdown: asyncio.Event,
    log: Callable[[str], None],
) -> None:
    """
    Loop principal do WS. Reconexão automática com backoff exponencial (cap 60s).
    Hard-disconnect forçado a cada 23h para evitar queda brusca do server-side 24h.
    """
    if not HAS_WS:
        log("websockets não instalado, skipping Binance WS")
        return

    backoff = 1.0
    url = build_ws_url(symbols)
    while not shutdown.is_set():
        connected_at = time.time()
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=60,
                max_size=2 * 1024 * 1024,
                close_timeout=5,
            ) as ws:
                log(f"[binance] conectado, streams={len(symbols)}")
                backoff = 1.0
                while not shutdown.is_set():
                    # Forçar reconexão antes do limite de 24h
                    if time.time() - connected_at > WS_RECONNECT_HARD_LIMIT_SEC:
                        log("[binance] 23h atingidas, reconectando preventivo")
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        continue
                    except ConnectionClosed as exc:
                        log(f"[binance] WS closed: {exc}")
                        break
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    quote = parse_book_ticker(msg)
                    if quote is not None:
                        try:
                            on_quote(quote)
                        except Exception as exc:
                            log(f"[binance] on_quote error: {type(exc).__name__}: {exc}")
        except Exception as exc:
            log(f"[binance] reconnect caused by {type(exc).__name__}: {exc}")

        if not shutdown.is_set():
            sleep_sec = min(backoff, 60.0)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=sleep_sec)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60.0)


# ─────────────────────────────────────────────────────
# Cache / orquestrador de vol
# ─────────────────────────────────────────────────────

@dataclass
class BinanceState:
    """Estado compartilhado entre o WS, a task de vol e o detector."""
    quotes: dict[str, SpotQuote] = field(default_factory=dict)
    vol: dict[str, VolEstimate] = field(default_factory=dict)
    last_drift_ms: Optional[int] = None
    last_drift_at_ms: int = 0

    def on_quote(self, q: SpotQuote) -> None:
        self.quotes[q.symbol] = q

    def get_spot(self, symbol: str, max_age_ms: int = 10_000) -> Optional[SpotQuote]:
        q = self.quotes.get(symbol)
        if q is None:
            return None
        if not q.is_fresh(int(time.time() * 1000), max_age_ms=max_age_ms):
            return None
        return q

    def get_vol(self, symbol: str) -> Optional[VolEstimate]:
        return self.vol.get(symbol)


async def vol_refresh_task(
    symbols: list[str],
    state: BinanceState,
    shutdown: asyncio.Event,
    log: Callable[[str], None],
    interval_sec: int = 60,
) -> None:
    """
    A cada interval_sec (default 1min), puxa klines e atualiza vol estimates.
    Rate: 6 símbolos × 1/min = 6 calls/min (trivial vs limite 1200/min).
    """
    if not HAS_HTTPX:
        log("[binance] httpx não instalado, vol task desabilitada")
        return

    client = httpx.AsyncClient(timeout=10.0)
    try:
        while not shutdown.is_set():
            now_ms = int(time.time() * 1000)
            for sym in symbols:
                klines = await fetch_klines(sym, interval="1m", limit=60, client=client)
                ve = vol_estimate_from_klines(sym, klines, now_ms)
                state.vol[sym] = ve
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
    finally:
        await client.aclose()


async def drift_monitor_task(
    state: BinanceState,
    shutdown: asyncio.Event,
    log: Callable[[str], None],
    interval_sec: int = 300,
) -> None:
    """Mede clock drift local vs Binance a cada 5min."""
    if not HAS_HTTPX:
        return
    client = httpx.AsyncClient(timeout=5.0)
    try:
        while not shutdown.is_set():
            drift = await measure_clock_drift_ms(client)
            if drift is not None:
                state.last_drift_ms = drift
                state.last_drift_at_ms = int(time.time() * 1000)
                if abs(drift) > 2000:
                    log(f"[binance] clock drift {drift}ms — considere NTP sync")
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                pass
    finally:
        await client.aclose()
