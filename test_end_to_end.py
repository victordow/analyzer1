"""
test_end_to_end.py — valida pipeline: gera records → persist → read → report.
"""
from __future__ import annotations

import json
import sys
import tempfile
import traceback
from dataclasses import asdict
from pathlib import Path

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

from detector import OpportunityRecord


def make_fake_record(opp_id: int, strategy: str = "A", symbol: str = "BTCUSDT",
                     delta: float = -0.10, flagged: bool = True) -> OpportunityRecord:
    return OpportunityRecord(
        opportunity_id=opp_id,
        detected_at_ms=1_700_000_000_000 + opp_id * 1000,
        closed_at_ms=1_700_000_000_000 + opp_id * 1000 + 60_000,
        market_id=f"mkt_{opp_id}",
        slug=f"slug_{opp_id}",
        question=f"Will BTC be above $100k at 5PM ET #{opp_id}?",
        market_type="strike_at_time",
        strategy=strategy,
        strategies_matched=strategy,
        symbol=symbol,
        tau_sec=1800.0,
        spot_s=99_000.0,
        strike_primary=100_000.0,
        strike_secondary=None,
        direction_above=True,
        sigma_60=0.5,
        sigma_10=0.48,
        p_model=0.05,
        p_market=0.15,
        delta=delta,
        z=-1.2,
        c=0.65,
        w=0.5,
        signal=delta * 0.65 * 0.5,
        signal_long=delta * 0.65 * 0.5,
        signal_short=abs(delta) * 0.65 * 0.5,
        fee_peak_polymarket=0.072,
        fee_dynamic_at_p=0.036,
        slippage_long_frac=0.005,
        slippage_short_frac=0.005,
        book_depth_usd=1500.0,
        yes_best_bid=0.14,
        yes_best_ask=0.16,
        model_undefined=False,
        opportunity_flagged=flagged,
        direction_label="SHORT_YES" if delta < 0 else "LONG_YES",
        rejection_reason="",
    )


def test_persist_and_report() -> None:
    """
    Pipeline completo: persiste 5 records, roda generate_report, valida outputs.
    Skip se pyarrow ausente (VPS tem, sandbox não).
    """
    try:
        import pyarrow  # noqa
    except ImportError:
        print("  SKIP  test_persist_and_report: pyarrow ausente (ok no VPS)")
        return
    import pandas as pd
    tmp = Path(tempfile.mkdtemp())

    # Simula o persist_incremental: cria parquet com 5 records
    records = [
        make_fake_record(1, strategy="A", symbol="BTCUSDT", delta=-0.12),
        make_fake_record(2, strategy="A", symbol="ETHUSDT", delta=-0.08),
        make_fake_record(3, strategy="B", symbol="BTCUSDT", delta=0.15),
        make_fake_record(4, strategy="A", symbol="SOLUSDT", delta=-0.25),
        make_fake_record(5, strategy="B", symbol="BTCUSDT", delta=-0.06),
    ]
    df = pd.DataFrame([asdict(r) for r in records])
    out = tmp / "opportunities_0001_test.parquet"
    df.to_parquet(out, index=False)

    # Audit
    audit = pd.DataFrame([
        {"ts_ms": 1_700_000_000_000, "market_id": "mkt_X", "rejection": "book_stale", "flagged": False},
        {"ts_ms": 1_700_000_000_000, "market_id": "mkt_Y", "rejection": "depth_too_low", "flagged": False},
        {"ts_ms": 1_700_000_000_000, "market_id": "mkt_Z", "rejection": "", "flagged": True},
    ])
    audit_out = tmp / "audit_0001_test.parquet"
    audit.to_parquet(audit_out, index=False)

    # Gera report
    from generate_report import generate
    report = generate(tmp, fmt="both")

    # Valida estrutura do report
    assert report["opportunities"]["total"] == 5
    assert report["opportunities"]["by_strategy"]["A"] == 3
    assert report["opportunities"]["by_strategy"]["B"] == 2
    assert report["opportunities"]["by_symbol"]["BTCUSDT"] == 3
    assert report["audit"]["total"] == 3

    # Arquivos foram criados
    assert (tmp / "report.txt").exists()
    assert (tmp / "report.json").exists()
    assert (tmp / "opportunities_all.parquet").exists()

    # JSON é válido
    report_json = json.loads((tmp / "report.json").read_text())
    assert report_json["opportunities"]["total"] == 5

    # TXT não está vazio
    txt = (tmp / "report.txt").read_text()
    assert "RELATÓRIO FINAL" in txt
    assert "Total de oportunidades: 5" in txt


def test_empty_run_report() -> None:
    """Report com zero oportunidades não crasha."""
    tmp = Path(tempfile.mkdtemp())
    from generate_report import generate
    report = generate(tmp, fmt="both")
    assert report["opportunities"]["total"] == 0
    assert (tmp / "report.txt").exists()
    txt = (tmp / "report.txt").read_text()
    assert "Nenhuma oportunidade" in txt


def test_edge_bucket_classification() -> None:
    from generate_report import edge_bucket
    assert edge_bucket(0.04) == "3-5%"
    assert edge_bucket(0.07) == "5-10%"
    assert edge_bucket(0.15) == "10-20%"
    assert edge_bucket(0.25) == "20-30%"
    assert edge_bucket(0.50) == ">30%"


def run_all() -> int:
    tests = [
        test_persist_and_report,
        test_empty_run_report,
        test_edge_bucket_classification,
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
