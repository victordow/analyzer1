"""
polymarket_client.py — descoberta e streaming de mercados crypto hourly no Polymarket.

Responsabilidades:
- Descobrir mercados via tag 102175 (crypto hourly) na Gamma API.
- Parse de cada mercado: extrai strike, classifica tipo, mapeia token YES/NO.
- Stream WebSocket CLOB pra top-of-book + depth.
- Periodic re-discovery pra detectar mercados novos/resolvidos.

Endpoints (oficiais, confirmados):
- Gamma REST: https://gamma-api.polymarket.com/events (com params active, closed, tag_id)
- CLOB WS: wss://ws-subscriptions-clob.polymarket.com/ws/market
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
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

from analyzer_math import (
    MarketType,
    ParsedMarket,
    classify_and_parse,
)

# ─────────────────────────────────────────────────────
# Endpoints / constantes
# ─────────────────────────────────────────────────────

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
TAG_HOURLY_CRYPTO = 102175  # confirmado via pesquisa (deepwiki dr_manhattan)

# Tipos aceitos pelo MVP (operador excluiu UP_DOWN_IMPLICIT)
ACCEPTED_TYPES = {
    MarketType.STRIKE_AT_TIME,
    MarketType.STRIKE_TOUCH,
    MarketType.TWO_BARRIER,
}

# Rejeição imediata de books
MAX_STALENESS_MS = 30_000
MIN_BID = 0.02
MAX_ASK = 0.98
SUM_BID_ASK_RANGE = (0.95, 1.05)


# ─────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────

@dataclass
class BookLevel:
    price: float
    size: float


@dataclass
class OutcomeBook:
    """
    Estado do book de UM outcome (YES ou NO).

    Guarda levels completos pra computar VWAP slippage.
    """
    token_id: str
    bids: list[BookLevel] = field(default_factory=list)  # desc por price
    asks: list[BookLevel] = field(default_factory=list)  # asc
    last_update_ms: int = 0

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    def is_fresh(self, now_ms: int) -> bool:
        return (now_ms - self.last_update_ms) <= MAX_STALENESS_MS


@dataclass
class PolymarketMarket:
    """
    Um mercado Polymarket já parseado e pronto pra comparação cross-platform.

    Tem 1 mercado binário com YES/NO tokens (diferente do NegRisk do v2).
    """
    market_id: str
    condition_id: str
    slug: str
    question: str                # título original
    parsed: ParsedMarket         # classificação + strike
    end_time_ms: int             # timestamp de resolução
    yes_token_id: str
    no_token_id: Optional[str]
    volume_usd: float
    category: str = "crypto"


@dataclass
class MarketBooks:
    """Container dos books YES e NO pra um mercado."""
    market_id: str
    yes: OutcomeBook
    no: Optional[OutcomeBook]

    def both_fresh(self, now_ms: int) -> bool:
        if not self.yes.is_fresh(now_ms):
            return False
        if self.no is not None and not self.no.is_fresh(now_ms):
            return False
        return True


# ─────────────────────────────────────────────────────
# Parse de endTime
# ─────────────────────────────────────────────────────

def parse_end_time(raw: Any) -> Optional[int]:
    """
    Gamma API retorna endDate como "2026-04-20T21:00:00Z". Retorna ms unix.
    """
    if not raw or not isinstance(raw, str):
        return None
    s = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def parse_clob_token_ids(raw: Any) -> Optional[list[str]]:
    """
    Gamma API às vezes retorna clobTokenIds como string JSON, outras como list.
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, list):
            return [str(t) for t in parsed]
        return None
    if isinstance(raw, list):
        return [str(t) for t in raw]
    return None


def parse_float_safe(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


# ─────────────────────────────────────────────────────
# Descoberta de mercados via Gamma API
# ─────────────────────────────────────────────────────

async def fetch_events_by_tag(
    tag_id: int,
    client: Optional[Any] = None,
) -> list[dict]:
    """
    [Legado, mantido pra compat] GET /events?tag_id=<tag>, paginated.
    """
    if not HAS_HTTPX:
        raise RuntimeError("httpx not installed")
    owns_client = client is None
    collected: list[dict] = []
    offset = 0
    try:
        if owns_client:
            client = httpx.AsyncClient(base_url=GAMMA_BASE, timeout=20.0)
        for _ in range(20):
            params = {
                "tag_id": tag_id,
                "limit": 200,
                "offset": offset,
                "active": "true",
                "closed": "false",
            }
            try:
                resp = await client.get("/events", params=params)
                resp.raise_for_status()
                batch = resp.json()
            except Exception:
                break
            if not isinstance(batch, list) or not batch:
                break
            collected.extend(batch)
            if len(batch) < 200:
                break
            offset += 200
        return collected
    finally:
        if owns_client and client is not None:
            await client.aclose()


# Universo de tokens aceitos pro filtro textual (descoberta nova).
# Match case-insensitive. Incluimos nome completo + tickers.
_CRYPTO_KEYWORDS_FOR_SEARCH = [
    "bitcoin", "ethereum", "solana", "xrp", "dogecoin", "bnb",
    "btc", "eth", "sol", "doge",
]


def _market_is_crypto_candidate(market: dict) -> bool:
    """Heurística leve pra pré-filtrar markets com chance de ser crypto-strike."""
    question = (market.get("question") or "").lower()
    if not question:
        return False
    # Precisa ter $ no título (strike explícito)
    if "$" not in (market.get("question") or ""):
        return False
    # Precisa mencionar alguma crypto
    return any(k in question for k in _CRYPTO_KEYWORDS_FOR_SEARCH)


async def fetch_markets_crypto(
    client: Optional[Any] = None,
    max_pages: int = 10,
    page_size: int = 500,
) -> list[dict]:
    """
    GET /markets?limit=N&active=true&closed=false&order=volume24hr&ascending=false

    Retorna mercados crypto candidatos (filtro textual por símbolo + $).
    Paginação por offset até max_pages ou fim da resposta.

    Uso o endpoint /markets (não /events) porque:
    - Cada item já é um market completo (não precisa explodir event.markets[])
    - Pode ordenar por volume24hr (volume ativo, não histórico)
    - Filtro textual pos-fetch evita depender de tag_id específica
    """
    if not HAS_HTTPX:
        raise RuntimeError("httpx not installed")
    owns_client = client is None
    collected: list[dict] = []
    offset = 0
    try:
        if owns_client:
            client = httpx.AsyncClient(base_url=GAMMA_BASE, timeout=20.0)
        for _ in range(max_pages):
            params = {
                "limit": page_size,
                "offset": offset,
                "active": "true",
                "closed": "false",
                "order": "volume24hr",
                "ascending": "false",
            }
            try:
                resp = await client.get("/markets", params=params)
                resp.raise_for_status()
                batch = resp.json()
            except Exception:
                break
            if not isinstance(batch, list) or not batch:
                break
            for m in batch:
                if isinstance(m, dict) and _market_is_crypto_candidate(m):
                    collected.append(m)
            if len(batch) < page_size:
                break
            offset += page_size
        return collected
    finally:
        if owns_client and client is not None:
            await client.aclose()


def build_polymarket_market(
    event: Optional[dict],
    market: dict,
    spot_for_sanity: Optional[float] = None,
    spot_by_symbol: Optional[dict[str, float]] = None,
) -> Optional[PolymarketMarket]:
    """
    Converte 1 (event, market) par pra PolymarketMarket.

    event pode ser None quando descoberta vem direto de /markets.

    Retorna None se:
    - Não é tradable / não tem tokens
    - Parse de título não classificou
    - Tipo não é aceito no MVP
    """
    event = event or {}

    # Tradable?
    if not market.get("enableOrderBook"):
        return None
    if not market.get("acceptingOrders", False):
        return None
    if market.get("closed"):
        return None
    if not market.get("active", True):
        return None

    tokens = parse_clob_token_ids(
        market.get("clobTokenIds") or market.get("clob_token_ids")
    )
    if not tokens or len(tokens) < 1:
        return None

    # Título: o mercado individual tem "question", o event tem "title". Preferimos question.
    title = market.get("question") or event.get("title") or ""
    if not title:
        return None

    parsed = classify_and_parse(
        title,
        spot_for_sanity=spot_for_sanity,
        spot_by_symbol=spot_by_symbol,
    )
    if parsed.parse_confidence < 0.5:
        return None
    if parsed.market_type not in ACCEPTED_TYPES:
        return None

    end_time_ms = parse_end_time(market.get("endDate") or event.get("endDate"))
    if end_time_ms is None:
        return None

    # Volume: preferir volume24hr (atividade atual), fallback pra volume total
    vol = parse_float_safe(market.get("volume24hr"), 0.0)
    if vol == 0.0:
        vol = parse_float_safe(market.get("volume"), 0.0)
    if vol == 0.0:
        vol = parse_float_safe(event.get("volume"), 0.0)

    market_id = str(market.get("id") or market.get("conditionId") or "")
    if not market_id:
        return None

    return PolymarketMarket(
        market_id=market_id,
        condition_id=str(market.get("conditionId", "")),
        slug=str(market.get("slug") or event.get("slug") or ""),
        question=title,
        parsed=parsed,
        end_time_ms=end_time_ms,
        yes_token_id=tokens[0],
        no_token_id=tokens[1] if len(tokens) >= 2 else None,
        volume_usd=vol,
    )


async def discover_markets(
    client: Optional[Any] = None,
    spot_snapshot: Optional[dict[str, float]] = None,
    min_volume_usd: float = 1000.0,
    tau_min_sec: int = 60 * 60,               # 1h (mínimo da estratégia A)
    tau_max_sec: int = 7 * 24 * 60 * 60,      # 7 dias (máximo da estratégia B)
) -> list[PolymarketMarket]:
    """
    Descobre mercados crypto com strike explícito (ex: "Will Bitcoin reach $X in April?").

    - spot_snapshot: {symbol: spot_price} pra sanity K/S (passado direto pro parser)
    - min_volume_usd: default 1000 (filtrar dust antes mesmo de subscribe WS)
    - tau_min/max_sec: janela temporal aceita. Default cobre A e B.

    Rejeita mercados fora da janela e sem strike explícito.
    """
    raw_markets = await fetch_markets_crypto(client=client)
    out: list[PolymarketMarket] = []
    now_ms = int(time.time() * 1000)

    for m in raw_markets:
        pm = build_polymarket_market(
            event=None, market=m,
            spot_by_symbol=spot_snapshot,
        )
        if pm is None:
            continue
        if pm.volume_usd < min_volume_usd:
            continue
        tau_sec = (pm.end_time_ms - now_ms) / 1000.0
        if tau_sec < tau_min_sec or tau_sec > tau_max_sec:
            continue
        out.append(pm)
    return out


# ─────────────────────────────────────────────────────
# CLOB WebSocket — parsing
# ─────────────────────────────────────────────────────

def parse_book_levels(raw_levels: Any) -> list[BookLevel]:
    """Parseia [{"price": "...", "size": "..."}] para BookLevel[]."""
    out = []
    if not isinstance(raw_levels, list):
        return out
    for raw in raw_levels:
        if not isinstance(raw, dict):
            continue
        try:
            price = float(raw.get("price", 0))
            size = float(raw.get("size", 0))
        except (ValueError, TypeError):
            continue
        if price <= 0 or size <= 0:
            continue
        out.append(BookLevel(price=price, size=size))
    return out


def sort_levels(
    bids: list[BookLevel], asks: list[BookLevel],
) -> tuple[list[BookLevel], list[BookLevel]]:
    return (
        sorted(bids, key=lambda x: x.price, reverse=True),
        sorted(asks, key=lambda x: x.price),
    )


def normalize_polymarket_timestamp(raw: Any) -> int:
    """
    Polymarket CLOB manda timestamp em SEGUNDOS como string.
    Se < 10^11, multiplica por 1000. Confirmado no v2.
    """
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return int(time.time() * 1000)
    if val <= 0:
        return int(time.time() * 1000)
    if val < 10**11:
        val *= 1000
    return val


def handle_book_msg(book: OutcomeBook, msg: dict) -> None:
    """Snapshot completo: substitui bids e asks."""
    bids = parse_book_levels(msg.get("bids"))
    asks = parse_book_levels(msg.get("asks"))
    bids, asks = sort_levels(bids, asks)
    book.bids = bids
    book.asks = asks
    book.last_update_ms = normalize_polymarket_timestamp(msg.get("timestamp"))


def handle_price_change_msg(book: OutcomeBook, msg: dict) -> None:
    """Delta: changes = [{price, side, size}]."""
    changes = msg.get("changes") or msg.get("price_changes") or []
    if not isinstance(changes, list):
        return
    bids_by_price = {lvl.price: lvl for lvl in book.bids}
    asks_by_price = {lvl.price: lvl for lvl in book.asks}
    for ch in changes:
        if not isinstance(ch, dict):
            continue
        try:
            price = float(ch.get("price", 0))
            size = float(ch.get("size", 0))
        except (ValueError, TypeError):
            continue
        side = str(ch.get("side", "")).upper()
        if price <= 0:
            continue
        if side in ("BUY", "BID"):
            if size == 0:
                bids_by_price.pop(price, None)
            else:
                bids_by_price[price] = BookLevel(price, size)
        elif side in ("SELL", "ASK"):
            if size == 0:
                asks_by_price.pop(price, None)
            else:
                asks_by_price[price] = BookLevel(price, size)
    bids, asks = sort_levels(list(bids_by_price.values()), list(asks_by_price.values()))
    book.bids = bids
    book.asks = asks
    book.last_update_ms = normalize_polymarket_timestamp(msg.get("timestamp"))


def handle_best_bid_ask_msg(book: OutcomeBook, msg: dict) -> None:
    """Top-of-book update. Se não bate com topo local, substitui placeholder."""
    try:
        bb = float(msg.get("best_bid", 0))
        ba = float(msg.get("best_ask", 0))
    except (ValueError, TypeError):
        return
    if bb > 0:
        if not book.bids or book.bids[0].price != bb:
            book.bids = [BookLevel(bb, 1.0)]
    if ba > 0:
        if not book.asks or book.asks[0].price != ba:
            book.asks = [BookLevel(ba, 1.0)]
    book.last_update_ms = normalize_polymarket_timestamp(msg.get("timestamp"))


def process_clob_msg(
    books_by_token: dict[str, OutcomeBook],
    msg: dict,
) -> None:
    aid = msg.get("asset_id")
    if not aid:
        return
    book = books_by_token.get(str(aid))
    if book is None:
        return
    et = msg.get("event_type")
    if et == "book":
        handle_book_msg(book, msg)
    elif et == "price_change":
        handle_price_change_msg(book, msg)
    elif et == "best_bid_ask":
        handle_best_bid_ask_msg(book, msg)


# ─────────────────────────────────────────────────────
# WebSocket runner
# ─────────────────────────────────────────────────────

MAX_TOKENS_PER_CONN = 50


async def run_clob_stream(
    tokens_chunk: list[str],
    books_by_token: dict[str, OutcomeBook],
    shutdown: asyncio.Event,
    log: Callable[[str], None],
    conn_id: int = 0,
) -> None:
    if not HAS_WS:
        log("[polymarket] websockets não instalado")
        return

    sub_msg = {"assets_ids": tokens_chunk, "type": "market"}
    backoff = 1.0

    while not shutdown.is_set():
        connected_at = time.time()
        try:
            async with websockets.connect(
                CLOB_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                max_size=10 * 1024 * 1024,
                close_timeout=5,
            ) as ws:
                await ws.send(json.dumps(sub_msg))
                log(f"[polymarket conn {conn_id}] subscribed {len(tokens_chunk)} tokens")
                backoff = 1.0
                while not shutdown.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        if time.time() - connected_at > 60:
                            backoff = 1.0
                        continue
                    except ConnectionClosed as exc:
                        log(f"[polymarket conn {conn_id}] WS closed: {exc}")
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    msgs = data if isinstance(data, list) else [data]
                    for m in msgs:
                        if isinstance(m, dict):
                            try:
                                process_clob_msg(books_by_token, m)
                            except Exception as exc:
                                log(f"[polymarket conn {conn_id}] proc: {type(exc).__name__}: {exc}")
        except Exception as exc:
            log(f"[polymarket conn {conn_id}] reconnect: {type(exc).__name__}: {exc}")

        if not shutdown.is_set():
            sleep_sec = min(backoff, 60.0)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=sleep_sec)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60.0)
