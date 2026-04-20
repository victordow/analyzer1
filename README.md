# Analyzer Polymarket × Binance

Detector read-only de mispricing entre mercados crypto hourly no Polymarket
e o preço spot na Binance. Roda 18h por padrão, gera parquet incremental e
relatório final.

**Zero execução de ordem. Apenas log.**

## Estratégias

- **A**: τ ∈ [15min, 1h] — arbitragem cross-platform com modelo Black-Scholes
- **B**: τ ∈ [60s, 15min] — descasamento próximo da resolução
- **Sub-60s**: lógica direcional pura (modelo Φ(d2) é numericamente instável)

## Tipos de mercado suportados

- `strike_at_time`: "Will BTC be above $100k at 5PM ET?" (European binary, Φ(d2))
- `strike_touch`: "Will BTC reach $150k in April?" (American touch, 2·Φ(d2))
- `two_barrier`: "Will BTC hit $80k or $100k first?" (fórmula log-linear)

## Tipos excluídos (MVP)

- `up_down_implicit` ("Bitcoin Up or Down - April 20, 5PM ET"): strike vem do
  Chainlink no momento de abertura da janela, requer feed separado.

## Setup

```
cd ~/analyzer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Smoke test (20 min)

```
python3 analyzer_main.py --hours 0.33
```

Valide:
- `cat analyzer_output/run_*/status.json` → `binance_symbols_fresh` lista os 6 pares,
  `binance_vol_usable` também, `top_rejections` não dominado por `spot_stale` ou `book_stale`.
- Se houver detecções, `detections_closed > 0`.
- Zero exceptions no `analyzer.log`.

## Run de 18h

```
tmux new -s analyzer
source venv/bin/activate
python3 analyzer_main.py --hours 18
# Ctrl+B D pra detach
```

Depois:

```
tmux attach -t analyzer          # ver log
cat analyzer_output/run_*/status.json  # status rápido
```

## Gerar relatório pós-run

```
python3 generate_report.py --run analyzer_output/run_2026-04-20_04h00/
```

Saídas:
- `report.txt`: resumo executivo humano
- `report.json`: estrutura completa
- `opportunities_all.parquet`: consolidado para análise posterior

## Auditoria

Todos os ticks do detector são logados em `audit_*.parquet` (mesmo os rejeitados).
Cada record tem:
- `ts_ms`, `market_id`, `strategy`, `delta`, `flagged`, `rejection`

Pra investigar "por que não detectou?":

```python
import pandas as pd
audit = pd.concat([pd.read_parquet(f) for f in sorted(Path("analyzer_output/run_*/").glob("audit_*.parquet"))])
audit["rejection"].value_counts()
```

## Parâmetros aprovados (20/abr/2026)

- Fee Polymarket crypto: 7.2% peak (docs.polymarket.com/trading/fees)
- Fee dinâmico: `rate × p × (1-p) × 4` (peak em p=0.5)
- Pares: BTC, ETH, SOL, XRP, DOGE, BNB
- Slippage: VWAP-$500
- Margem de segurança: 0.5% extra
- σ mínimo: 30 candles válidas de 1min
- Dedupe: janela aberta/fechada, reopen gap 60s
- Sanity K/S: ratio ∈ [0.3, 3.0]
- Book terminal: bid<0.02 ou ask>0.98
- Staleness: 30s

## Estrutura de arquivos

```
analyzer_math.py         # Parser + classificação + fórmulas matemáticas
binance_client.py        # WS bookTicker + REST klines + σ
polymarket_client.py     # Gamma discovery + CLOB WS
detector.py              # Framework matemático aplicado + dedupe
analyzer_main.py         # Orquestrador (roda 18h)
generate_report.py       # Gera relatório de um run_*/
test_*.py                # Suite (121 testes)
```

## Rodar testes

```
for f in test_*.py; do python3 $f; done
```
