#!/usr/bin/env python3
"""
generate_report.py — lê os parquets persistidos e gera relatório final.

Produz:
  - report.txt: resumo executivo em texto
  - report.json: dados estruturados
  - opportunities_all.parquet: consolidação de todas as oportunidades

Uso:
    python3 generate_report.py --run analyzer_output/run_2026-04-20_04h00/
    python3 generate_report.py --run <dir>  --format json
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

try:
    import pandas as pd
except ImportError:
    print("ERRO: pandas necessário. pip install pandas pyarrow", file=sys.stderr)
    sys.exit(1)


def load_parquets(directory: Path, prefix: str) -> Optional["pd.DataFrame"]:
    files = sorted(directory.glob(f"{prefix}_*.parquet"))
    if not files:
        return None
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as exc:
            print(f"[warn] erro lendo {f.name}: {exc}", file=sys.stderr)
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def edge_bucket(delta_abs: float) -> str:
    """Bucketiza |delta| em faixas para o relatório."""
    if delta_abs < 0.05:
        return "3-5%"
    if delta_abs < 0.10:
        return "5-10%"
    if delta_abs < 0.20:
        return "10-20%"
    if delta_abs < 0.30:
        return "20-30%"
    return ">30%"


def percentile(s: "pd.Series", p: float) -> Optional[float]:
    s = s.dropna()
    if len(s) == 0:
        return None
    return float(s.quantile(p))


def generate(run_dir: Path, fmt: str = "both") -> dict:
    """Gera estruturas de resumo. Retorna dict do relatório."""
    opps = load_parquets(run_dir, "opportunities")
    audit = load_parquets(run_dir, "audit")

    report: dict = {
        "run_dir": str(run_dir),
        "opportunities": {"total": 0},
        "audit": {"total": 0},
    }

    # Lê status.json se existir, pra contexto
    status_path = run_dir / "status.json"
    if status_path.exists():
        try:
            report["final_status"] = json.loads(status_path.read_text())
        except Exception:
            pass

    if opps is not None and not opps.empty:
        opps = opps.copy()
        opps["delta_abs"] = opps["delta"].abs()
        opps["edge_bucket"] = opps["delta_abs"].apply(edge_bucket)
        opps["duration_sec"] = (
            (opps["closed_at_ms"].fillna(opps["detected_at_ms"]) - opps["detected_at_ms"]) / 1000.0
        )

        by_strategy = opps.groupby("strategy").size().to_dict()
        by_symbol = opps.groupby("symbol").size().to_dict()
        by_market_type = opps.groupby("market_type").size().to_dict()
        by_edge_bucket = opps.groupby("edge_bucket").size().to_dict()
        by_direction = opps.groupby("direction_label").size().to_dict()

        # Oportunidades por hora
        opps["detected_hour"] = (opps["detected_at_ms"] // 3600000).astype(int)
        per_hour = opps.groupby("detected_hour").size().to_dict()

        # Top mercados por frequência
        top_markets = opps.groupby(["market_id", "question"]).size() \
            .sort_values(ascending=False).head(10)
        top_markets_list = [
            {"market_id": mid, "question": q[:80], "count": int(cnt)}
            for (mid, q), cnt in top_markets.items()
        ]

        report["opportunities"] = {
            "total": int(len(opps)),
            "by_strategy": {str(k): int(v) for k, v in by_strategy.items()},
            "by_symbol": {str(k): int(v) for k, v in by_symbol.items()},
            "by_market_type": {str(k): int(v) for k, v in by_market_type.items()},
            "by_direction": {str(k): int(v) for k, v in by_direction.items()},
            "by_edge_bucket": {str(k): int(v) for k, v in by_edge_bucket.items()},
            "per_hour_distribution": {str(k): int(v) for k, v in per_hour.items()},
            "delta_abs_p50": percentile(opps["delta_abs"], 0.50),
            "delta_abs_p90": percentile(opps["delta_abs"], 0.90),
            "delta_abs_max": float(opps["delta_abs"].max()),
            "duration_sec_p50": percentile(opps["duration_sec"], 0.50),
            "duration_sec_p90": percentile(opps["duration_sec"], 0.90),
            "top_markets": top_markets_list,
        }

        # Salva consolidado
        consolidated = run_dir / "opportunities_all.parquet"
        opps.to_parquet(consolidated, index=False)

    if audit is not None and not audit.empty:
        audit = audit.copy()
        rej_counter = Counter(audit["rejection"].dropna().tolist())
        report["audit"] = {
            "total": int(len(audit)),
            "top_rejections": dict(rej_counter.most_common(15)),
            "flagged_count": int(audit["flagged"].fillna(False).sum()),
        }

    # Write output
    if fmt in ("json", "both"):
        out_json = run_dir / "report.json"
        out_json.write_text(json.dumps(report, indent=2, default=str))
        print(f"Relatório JSON: {out_json}")
    if fmt in ("txt", "both"):
        out_txt = run_dir / "report.txt"
        out_txt.write_text(format_text_report(report))
        print(f"Relatório TXT:  {out_txt}")

    return report


def format_text_report(r: dict) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append("RELATÓRIO FINAL — ANALYZER POLYMARKET × BINANCE")
    lines.append("=" * 60)
    lines.append(f"Run dir: {r.get('run_dir')}")
    lines.append("")

    opp = r.get("opportunities", {})
    if opp.get("total", 0) == 0:
        lines.append("Nenhuma oportunidade detectada.")
    else:
        lines.append(f"Total de oportunidades: {opp['total']}")
        lines.append("")
        lines.append("Por estratégia:")
        for k, v in opp.get("by_strategy", {}).items():
            lines.append(f"  {k}: {v}")
        lines.append("")
        lines.append("Por símbolo:")
        for k, v in sorted(opp.get("by_symbol", {}).items(), key=lambda kv: -kv[1]):
            lines.append(f"  {k}: {v}")
        lines.append("")
        lines.append("Por tipo de mercado:")
        for k, v in opp.get("by_market_type", {}).items():
            lines.append(f"  {k}: {v}")
        lines.append("")
        lines.append("Por direção:")
        for k, v in opp.get("by_direction", {}).items():
            lines.append(f"  {k}: {v}")
        lines.append("")
        lines.append("Distribuição de |delta|:")
        for k, v in opp.get("by_edge_bucket", {}).items():
            lines.append(f"  {k}: {v}")
        lines.append("")
        lines.append("Percentis:")
        p50 = opp.get("delta_abs_p50")
        p90 = opp.get("delta_abs_p90")
        pmax = opp.get("delta_abs_max")
        if p50 is not None:
            lines.append(f"  |delta| p50: {p50:.4f}")
        if p90 is not None:
            lines.append(f"  |delta| p90: {p90:.4f}")
        if pmax is not None:
            lines.append(f"  |delta| max: {pmax:.4f}")
        dur_p50 = opp.get("duration_sec_p50")
        dur_p90 = opp.get("duration_sec_p90")
        if dur_p50 is not None:
            lines.append(f"  duração p50: {dur_p50:.1f}s")
        if dur_p90 is not None:
            lines.append(f"  duração p90: {dur_p90:.1f}s")
        lines.append("")
        lines.append("Top 10 mercados por frequência:")
        for item in opp.get("top_markets", []):
            lines.append(f"  [{item['count']:3d}] {item['question']}")
        lines.append("")

    audit = r.get("audit", {})
    lines.append(f"Audit: {audit.get('total', 0)} records")
    rej = audit.get("top_rejections", {})
    if rej:
        lines.append("Top rejeições:")
        for k, v in rej.items():
            lines.append(f"  {k}: {v}")

    status = r.get("final_status")
    if status:
        lines.append("")
        lines.append("Contexto final:")
        lines.append(f"  drift_ms: {status.get('binance_clock_drift_ms')}")
        lines.append(f"  symbols_fresh: {status.get('binance_symbols_fresh')}")
        lines.append(f"  vol_usable: {status.get('binance_vol_usable')}")
        lines.append(f"  polymarket_markets_end: {status.get('polymarket_markets')}")

    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True, help="path to analyzer_output/run_*/")
    parser.add_argument("--format", choices=["txt", "json", "both"], default="both")
    args = parser.parse_args()
    run_dir = Path(args.run)
    if not run_dir.exists():
        print(f"Diretório não existe: {run_dir}", file=sys.stderr)
        return 1
    try:
        generate(run_dir, fmt=args.format)
    except Exception as exc:
        import traceback
        print(f"ERRO: {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
