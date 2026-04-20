# Analyzer Polymarket × Binance (v2 — crypto strike)

Detector read-only de mispricing entre mercados **crypto com strike explícito**
no Polymarket e o preço spot na Binance. Roda 18h por padrão, gera parquet
incremental e relatório final.

**Zero execução de ordem. Apenas log.**

## O que mudou em v2 (20/abr/2026)

- Descoberta via `/markets?order=volume24hr` + filtro textual (não mais tag 102175)
- Janelas temporais adaptadas ao universo real:
  - **A**: τ ∈ [1h, 24h] — mercados tipo "above $X on April 21"
  - **B**: τ ∈ (24h, 7d] — mercados tipo "reach $X in April"
- Suporte a "dip to / fall to / drop to / crash to" como `STRIKE_TOUCH` com `direction_above=False`
- Sanity K/S agora usa `spot_by_symbol` (bug fix: ETH não é mais rejeitado com spot BTC)
- Volume mínimo default: $1000/24h

## Estratégias

- **A**: τ ∈ [1h, 24h] — cross-platform com modelo Black-Scholes completo
- **B**: τ ∈ (24h, 7d] — mesmo modelo, horizonte maior
- **Sub-60s**: lógica direcional pura (modelo Φ(d2) vira instável)

## Tipos de mercado suportados

- `strike_at_time`: "Will BTC be above $X on April 21?"
- `strike_touch` (above): "Will BTC reach $X in April?"
- `strike_touch` (below): "Will BTC dip to $X in April?"
- `two_barrier`: "Hit $X or $Y first?"

## Tipos excluídos (MVP)

- `up_down_implicit`: strike vem do Chainlink no momento de abertura. Requer
  captura histórica de spot, descartado no MVP porque a divergência Binance vs
  Chainlink no tick de settlement é exatamente o edge que buscaríamos.

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

Valide `status.json`:
- `binance_symbols_fresh` lista os 6 pares
- `binance_vol_usable` também
- `polymarket_markets` > 0 (universo descoberto)
- Zero exceptions no `analyzer.log`

## Run de 18h

```
tmux new -s analyzer
source venv/bin/activate
python3 analyzer_main.py --hours 18
# Ctrl+B D
```

## Gerar relatório

```
python3 generate_report.py --run analyzer_output/run_*/
```

## Parâmetros

- Pares: BTC, ETH, SOL, XRP, DOGE, BNB
- Fee Polymarket crypto: 7.2% peak, dinâmico `rate × p × (1-p) × 4`
- Slippage: VWAP-$500
- Margem: +0.5%
- σ: realized vol 60 candles 1min × √525600, min 30 candles
- Sanity K/S: ratio ∈ [0.3, 3.0] por símbolo
- Book terminal: bid<0.02 ou ask>0.98
- Staleness: 30s
- Dedupe: janela aberta/fechada, reopen gap 60s

## Testes

```
for f in test_*.py; do python3 $f; done
```

135/135 passando.
