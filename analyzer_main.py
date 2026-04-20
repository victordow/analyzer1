#!/usr/bin/env python3
"""
analyzer_main.py — orquestrador do detector cross-platform Polymarket vs Binance.

Roda 18h por padrão. Read-only. Zero execução de ordem.

Arquitetura:
  - Task 1: Binance WS bookTicker (S_t fresco pros 6 pares)
  - Task 2: Binance REST klines (σ atualizada a cada 60s)
  - Task 3: Binance drift monitor (clock drift a cada 5min)
  - Task 4: Polymarket CLOB WS (books YES/NO)
  - Task 5: Re-discovery (30min — mercados novos)
  - Task 6: Detector loop (a cada 500ms)
  - Task 7: Persist incremental (15min, parquet)
  - Task 8: Status writer (30s, status.json)

Shutdown graceful via SIGINT/SIGTERM.

Uso:
    python3 analyzer_main.py                   # 18h default
    python3 analyzer_main.py --hours 2         # smoke test
    python3 analyzer_main.py --min-volume 100  # universo maior
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
import traceback
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

from analyzer_math import ACCEPTED_SYMBOLS
from binance_client import (
    BinanceState, SpotQuote, drift_monitor_task,
    run_book_ticker_stream, vol_refresh_task,
)
from polymarket_client import (
    MAX_TOKENS_PER_CONN, MarketBooks, OutcomeBook,
    PolymarketMarket, discover_markets, run_clob_stream,
)
from detector import (
    DetectorState, OpportunityRecord, STRATEGY_A_TAU_MAX_SEC,
    STRATEGY_B_TAU_MIN_SEC, evaluate_market,
)

# ─────────────────────────────────────────────────────
# Constantes operacionais
# ─────────────────────────────────────────────────────

DEFAULT_DURATION_HOURS = 18.0
DEFAULT_MIN_VOLUME_USD = 500.0

DETECTION_INTERVAL_SEC = 0.5
PERSIST_INTERVAL_SEC = 900       # 15 min
STATUS_INTERVAL_SEC = 30
REDISCOVERY_INTERVAL_SEC = 1800  # 30 min
STDOUT_SUMMARY_INTERVAL_SEC = 300  # 5 min

VOL_REFRESH_INTERVAL_SEC = 60
DRIFT_INTERVAL_SEC = 300

# Universo (operador aprovou)
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT"]


# ─────────────────────────────────────────────────────
# Main runtime
# ─────────────────────────────────────────────────────

class Analyzer:
    def __init__(
        self,
        duration_sec: float,
        min_volume_usd: float,
        output_dir: Path,
    ):
        self.duration_sec = duration_sec
        self.min_volume_usd = min_volume_usd
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Estado Binance (compartilhado)
        self.binance_state = BinanceState()

        # Estado Polymarket
        self.markets: dict[str, PolymarketMarket] = {}
        self.market_books: dict[str, MarketBooks] = {}
        self.books_by_token: dict[str, OutcomeBook] = {}

        # Detector
        self.detector_state = DetectorState()

        # Persist incremental cursors
        self._records_closed_cursor: int = 0
        self._audit_cursor: int = 0
        self._persist_seq: int = 0

        # Sinal de shutdown
        self.shutdown_event: Optional[asyncio.Event] = None
        self.start_time: Optional[float] = None

        # WS tasks Polymarket (dinâmicas, uma por chunk de tokens)
        self._poly_tasks: list[asyncio.Task] = []

        # Arquivos
        self.log_path = output_dir / "analyzer.log"
        self.status_path = output_dir / "status.json"
        self.jsonl_path = output_dir / "events.jsonl"
        self.log_file = open(self.log_path, "a", buffering=1)
        self.jsonl_file = open(self.jsonl_path, "a", buffering=1)

        # Métricas por stdout summary
        self.last_stdout_summary_ms = 0

    def ensure_shutdown_event(self) -> asyncio.Event:
        if self.shutdown_event is None:
            self.shutdown_event = asyncio.Event()
        return self.shutdown_event

    def log(self, msg: str, level: str = "INFO") -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{ts}] [{level}] {msg}"
        print(line, flush=True)
        try:
            self.log_file.write(line + "\n")
        except Exception:
            pass

    def log_jsonl(self, kind: str, payload: dict) -> None:
        record = {"ts_ms": int(time.time() * 1000), "kind": kind, **payload}
        try:
            self.jsonl_file.write(json.dumps(record, default=str) + "\n")
        except Exception:
            pass

    def close(self) -> None:
        try:
            self.log_file.close()
        except Exception:
            pass
        try:
            self.jsonl_file.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────
# Discovery + subscription management
# ─────────────────────────────────────────────────────

def build_spot_snapshot(state: BinanceState) -> dict[str, float]:
    """Retorna {symbol: mid} pros pares com quote fresca."""
    out: dict[str, float] = {}
    now_ms = int(time.time() * 1000)
    for sym, q in state.quotes.items():
        if q.is_fresh(now_ms):
            out[sym] = q.mid
    return out


async def run_discovery_and_subscribe(az: Analyzer) -> None:
    """
    Descobre mercados crypto hourly, adiciona aos tracked, sobe WS pros tokens novos.
    """
    spot_snapshot = build_spot_snapshot(az.binance_state)
    az.log(f"[discovery] spot snapshot: {len(spot_snapshot)} symbols fresh")
    try:
        markets = await discover_markets(
            spot_snapshot=spot_snapshot,
            min_volume_usd=az.min_volume_usd,
            tau_min_sec=STRATEGY_B_TAU_MIN_SEC,
            tau_max_sec=STRATEGY_A_TAU_MAX_SEC,
        )
    except Exception as exc:
        az.log(f"[discovery] erro: {type(exc).__name__}: {exc}", level="WARN")
        return

    # Diff com mercados atuais
    new_market_ids = {m.market_id for m in markets}
    existing_ids = set(az.markets.keys())
    added = new_market_ids - existing_ids
    removed = existing_ids - new_market_ids

    new_tokens: list[str] = []
    for m in markets:
        if m.market_id in existing_ids:
            continue
        # Adiciona
        az.markets[m.market_id] = m
        yes_book = OutcomeBook(token_id=m.yes_token_id)
        no_book = OutcomeBook(token_id=m.no_token_id) if m.no_token_id else None
        az.market_books[m.market_id] = MarketBooks(
            market_id=m.market_id, yes=yes_book, no=no_book,
        )
        az.books_by_token[m.yes_token_id] = yes_book
        new_tokens.append(m.yes_token_id)
        if no_book is not None and m.no_token_id:
            az.books_by_token[m.no_token_id] = no_book
            new_tokens.append(m.no_token_id)

    # Mercados removidos: mantemos books até encerrar janelas eventualmente abertas,
    # mas desregistramos do universo ativo
    for mid in removed:
        az.markets.pop(mid, None)

    az.log(
        f"[discovery] total={len(markets)} added={len(added)} removed={len(removed)}"
    )
    az.log_jsonl("discovery", {
        "total": len(markets), "added": len(added), "removed": len(removed),
    })

    # Abre novos WS chunks pra tokens novos
    if new_tokens:
        chunks = [
            new_tokens[i:i + MAX_TOKENS_PER_CONN]
            for i in range(0, len(new_tokens), MAX_TOKENS_PER_CONN)
        ]
        shutdown = az.ensure_shutdown_event()
        for i, chunk in enumerate(chunks):
            conn_id = len(az._poly_tasks)
            t = asyncio.create_task(
                run_clob_stream(
                    tokens_chunk=chunk,
                    books_by_token=az.books_by_token,
                    shutdown=shutdown,
                    log=az.log,
                    conn_id=conn_id,
                )
            )
            az._poly_tasks.append(t)
        az.log(f"[discovery] +{len(chunks)} WS chunks ({len(new_tokens)} tokens)")


async def rediscovery_loop(az: Analyzer, deadline: float) -> None:
    """A cada 30min, re-discovery pra pegar mercados novos."""
    shutdown = az.ensure_shutdown_event()
    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=REDISCOVERY_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass
        if time.time() >= deadline:
            break
        await run_discovery_and_subscribe(az)


# ─────────────────────────────────────────────────────
# Detector loop
# ─────────────────────────────────────────────────────

async def detector_loop(az: Analyzer, deadline: float) -> None:
    shutdown = az.ensure_shutdown_event()
    st = az.detector_state
    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=DETECTION_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass
        now_ms = int(time.time() * 1000)

        # Para cada mercado, verifica: spot fresco? books frescos? avalia.
        for mid, market in list(az.markets.items()):
            symbol = market.parsed.symbol
            if symbol is None:
                continue
            spot = az.binance_state.get_spot(symbol)
            if spot is None:
                continue
            vol = az.binance_state.get_vol(symbol)
            if vol is None:
                continue
            books = az.market_books.get(mid)
            if books is None:
                continue
            try:
                rec = evaluate_market(
                    market=market, market_books=books,
                    spot=spot, vol=vol, state=st, now_ms=now_ms,
                )
                if rec is not None and rec.opportunity_flagged:
                    az.log_jsonl("opportunity_opened", {
                        "id": rec.opportunity_id, "market": market.question[:60],
                        "strategy": rec.strategy, "delta": rec.delta,
                        "direction": rec.direction_label,
                        "p_model": rec.p_model, "p_market": rec.p_market,
                        "spot_s": rec.spot_s, "tau_sec": rec.tau_sec,
                    })
            except Exception as exc:
                az.log(
                    f"[detector] erro mkt={mid}: {type(exc).__name__}: {exc}",
                    level="WARN",
                )


# ─────────────────────────────────────────────────────
# Persist incremental
# ─────────────────────────────────────────────────────

def _record_to_row(rec: OpportunityRecord) -> dict:
    d = asdict(rec)
    # Nada a fazer, dataclass já serializa tudo
    return d


def persist_incremental(az: Analyzer, tag: str = "") -> None:
    if not HAS_PANDAS:
        az.log("[persist] pandas ausente, skip", level="WARN")
        return
    try:
        closed_all = az.detector_state.closed_records
        audit_all = az.detector_state.audit_records
        new_closed = closed_all[az._records_closed_cursor:]
        new_audit = audit_all[az._audit_cursor:]
        if not new_closed and not new_audit:
            return

        az._persist_seq += 1
        seq = az._persist_seq
        label = tag or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

        if new_closed:
            rows = [_record_to_row(r) for r in new_closed]
            df = pd.DataFrame(rows)
            out = az.output_dir / f"opportunities_{seq:04d}_{label}.parquet"
            df.to_parquet(out, index=False)
            az._records_closed_cursor = len(closed_all)

        if new_audit:
            df = pd.DataFrame(new_audit)
            out = az.output_dir / f"audit_{seq:04d}_{label}.parquet"
            df.to_parquet(out, index=False)
            az._audit_cursor = len(audit_all)

        az.log(
            f"[persist #{seq}] +{len(new_closed)} closed, +{len(new_audit)} audit "
            f"(cursores: closed={az._records_closed_cursor}, audit={az._audit_cursor})"
        )
    except Exception as exc:
        az.log(
            f"[persist] erro: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            level="ERROR",
        )


async def persist_loop(az: Analyzer, deadline: float) -> None:
    shutdown = az.ensure_shutdown_event()
    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=PERSIST_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass
        persist_incremental(az)


# ─────────────────────────────────────────────────────
# Status / stdout summary
# ─────────────────────────────────────────────────────

def compute_status(az: Analyzer) -> dict:
    now_ms = int(time.time() * 1000)
    elapsed = time.time() - (az.start_time or time.time())

    # Quotes fresh
    fresh_syms = []
    for sym, q in az.binance_state.quotes.items():
        if q.is_fresh(now_ms):
            fresh_syms.append(sym)

    # Vol usable
    vol_usable = []
    for sym, ve in az.binance_state.vol.items():
        if ve.is_usable(min_candles=30):
            vol_usable.append(sym)

    # Open windows
    open_opportunities = len(az.detector_state.open_windows)
    closed_opportunities = len(az.detector_state.closed_records)

    # Top rejections
    reject_counter: Counter = Counter()
    for a in az.detector_state.audit_records[-5000:]:
        r = a.get("rejection")
        if r:
            reject_counter[r] += 1

    # Por estratégia
    strategy_counter: Counter = Counter()
    for rec in az.detector_state.closed_records:
        strategy_counter[rec.strategy] += 1

    # Drift
    drift_ms = az.binance_state.last_drift_ms

    return {
        "elapsed_min": round(elapsed / 60, 1),
        "duration_min": round(az.duration_sec / 60, 1),
        "pct_done": round(100 * elapsed / az.duration_sec, 1) if az.duration_sec > 0 else 0,
        "binance_symbols_fresh": fresh_syms,
        "binance_vol_usable": vol_usable,
        "binance_clock_drift_ms": drift_ms,
        "polymarket_markets": len(az.markets),
        "polymarket_tokens_tracked": len(az.books_by_token),
        "polymarket_ws_conns": len(az._poly_tasks),
        "detections_open": open_opportunities,
        "detections_closed": closed_opportunities,
        "detections_by_strategy": dict(strategy_counter),
        "top_rejections": dict(reject_counter.most_common(10)),
        "audit_records_total": len(az.detector_state.audit_records),
        "last_update_utc": datetime.now(timezone.utc).isoformat(),
    }


def write_status_atomic(az: Analyzer, status: dict) -> None:
    try:
        tmp = az.status_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(status, f, indent=2, default=str)
        os.replace(tmp, az.status_path)
    except Exception as exc:
        az.log(f"[status] erro escrevendo: {exc}", level="WARN")


async def status_loop(az: Analyzer, deadline: float) -> None:
    shutdown = az.ensure_shutdown_event()
    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=STATUS_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass
        status = compute_status(az)
        write_status_atomic(az, status)

        # Stdout resumido a cada 5min
        now = int(time.time() * 1000)
        if now - az.last_stdout_summary_ms >= STDOUT_SUMMARY_INTERVAL_SEC * 1000:
            az.last_stdout_summary_ms = now
            by_strat = status.get("detections_by_strategy", {})
            az.log(
                f"[{status['elapsed_min']:.1f}min/{status['duration_min']:.0f}min] "
                f"mkts={status['polymarket_markets']} "
                f"spot_fresh={len(status['binance_symbols_fresh'])}/6 "
                f"vol_ok={len(status['binance_vol_usable'])}/6 "
                f"open={status['detections_open']} closed={status['detections_closed']} "
                f"by_strat={by_strat} drift={status['binance_clock_drift_ms']}ms"
            )


# ─────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────

async def run(az: Analyzer) -> None:
    az.log("=== Analyzer Polymarket × Binance ===")
    az.log(f"Duração: {az.duration_sec/3600:.1f}h")
    az.log(f"Universo: {SYMBOLS}")
    az.log(f"Output: {az.output_dir}")

    az.start_time = time.time()
    deadline = az.start_time + az.duration_sec
    shutdown = az.ensure_shutdown_event()

    # Signal handlers
    loop = asyncio.get_running_loop()

    def _set_shutdown() -> None:
        az.log("Shutdown signal recebido, finalizando...")
        shutdown.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _set_shutdown)
        loop.add_signal_handler(signal.SIGTERM, _set_shutdown)
    except (NotImplementedError, RuntimeError):
        pass

    # 1. Binance WS + vol + drift (start primeiro — precisamos de S_t pra discovery sanity)
    binance_ws_task = asyncio.create_task(
        run_book_ticker_stream(
            symbols=SYMBOLS,
            on_quote=az.binance_state.on_quote,
            shutdown=shutdown,
            log=az.log,
        )
    )
    vol_task = asyncio.create_task(
        vol_refresh_task(
            symbols=SYMBOLS, state=az.binance_state,
            shutdown=shutdown, log=az.log,
            interval_sec=VOL_REFRESH_INTERVAL_SEC,
        )
    )
    drift_task = asyncio.create_task(
        drift_monitor_task(
            state=az.binance_state, shutdown=shutdown, log=az.log,
            interval_sec=DRIFT_INTERVAL_SEC,
        )
    )

    # 2. Espera 10s pra Binance preencher spot/vol antes do discovery
    az.log("Aguardando 10s pra Binance popular spot/vol...")
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=10.0)
        az.log("Shutdown durante warmup, saindo")
        return
    except asyncio.TimeoutError:
        pass

    # 3. Discovery inicial + subscribe Polymarket
    await run_discovery_and_subscribe(az)
    if not az.markets:
        az.log("Nenhum mercado descoberto na primeira tentativa — continuando mesmo assim")

    # 4. Loops paralelos
    detector_t = asyncio.create_task(detector_loop(az, deadline))
    persist_t = asyncio.create_task(persist_loop(az, deadline))
    status_t = asyncio.create_task(status_loop(az, deadline))
    rediscover_t = asyncio.create_task(rediscovery_loop(az, deadline))

    # 5. Espera deadline OU shutdown
    try:
        await asyncio.wait(
            [asyncio.create_task(shutdown.wait())],
            timeout=deadline - time.time(),
        )
    except Exception as exc:
        az.log(f"[main] wait erro: {exc}", level="WARN")

    # 6. Cleanup
    shutdown.set()
    az.log("Finalizando tasks...")
    await asyncio.sleep(1)

    # Aguarda tasks fecharem
    all_tasks = [binance_ws_task, vol_task, drift_task, detector_t, persist_t, status_t, rediscover_t] + az._poly_tasks
    await asyncio.gather(*all_tasks, return_exceptions=True)

    # Persist final + status final
    persist_incremental(az, tag="final")
    status = compute_status(az)
    write_status_atomic(az, status)

    az.log(f"Oportunidades fechadas: {len(az.detector_state.closed_records)}")
    az.log(f"Oportunidades abertas no fim: {len(az.detector_state.open_windows)}")
    az.log(f"Audit records: {len(az.detector_state.audit_records)}")
    az.log(f"Output em: {az.output_dir}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyzer Polymarket × Binance")
    parser.add_argument(
        "--hours", type=float, default=DEFAULT_DURATION_HOURS,
        help=f"Duração em horas (default {DEFAULT_DURATION_HOURS})",
    )
    parser.add_argument(
        "--min-volume", type=float, default=DEFAULT_MIN_VOLUME_USD,
        help=f"Volume mínimo USD por mercado (default {DEFAULT_MIN_VOLUME_USD})",
    )
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    duration_sec = args.hours * 3600
    if args.output:
        output_dir = Path(args.output)
    else:
        run_name = datetime.now(timezone.utc).strftime("run_%Y-%m-%d_%Hh%M")
        output_dir = Path("./analyzer_output") / run_name

    az = Analyzer(
        duration_sec=duration_sec,
        min_volume_usd=args.min_volume,
        output_dir=output_dir,
    )

    try:
        asyncio.run(run(az))
    except KeyboardInterrupt:
        az.log("KeyboardInterrupt")
    except Exception as exc:
        az.log(
            f"FATAL: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            level="ERROR",
        )
        return 1
    finally:
        az.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
