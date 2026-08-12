# Getting started

The fastest path to seeing it work, then the path to a real month-long run.
Everything here uses fake or paper money — nothing in this guide can lose
real funds.

## 0. Install Go

```sh
go version
```

Need 1.23+? Get it from [go.dev/dl](https://go.dev/dl/), or:

```sh
# macOS
brew install go

# Linux
curl -LO https://go.dev/dl/go1.24.7.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.24.7.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc && source ~/.bashrc
```

Then get the code:

```sh
git clone https://github.com/Rextrc/real-time-market-data-pipeline.git
cd real-time-market-data-pipeline
git checkout claude/market-data-pipeline-1amxx1
```

---

## 1. See it run — no keys, no network needed (2 minutes)

Two terminals.

**Terminal 1** — a fake exchange, so you don't need a real connection to Binance:

```sh
go run ./cmd/fakevenue -rate 300
```

**Terminal 2** — the pipeline, pointed at it, with two strategies trading fake money against each other:

```sh
go run ./cmd/mdp -endpoint ws://127.0.0.1:9443/stream \
  -paper -strategy momentum,meanrev \
  -candle-interval 1s -eval-interval 5s
```

Let it run a minute, then in a third terminal:

```sh
curl -s localhost:8080/v1/paper | jq .
```

You'll see two accounts — `momentum` and `meanrev` — each with equity, P&L,
and trades, fed from the identical fake tick stream. That's the whole
pipeline working: ingest, storage, backpressure, two strategies, API.

Ctrl-C both terminals when you're done looking.

---

## 2. Run it against the real market (5 minutes)

Same command, no fake venue — just point it at Binance:

```sh
go run ./cmd/mdp -symbols BTC-USDT,ETH-USDT,SOL-USDT \
  -paper -strategy momentum,meanrev
```

```sh
curl -s localhost:8080/v1/paper | jq .
curl -s localhost:8080/v1/quote/BTC-USDT | jq .
```

Still simulated money — nothing here touches a real account. This is the
version worth leaving running on your own machine for a while before you
deploy anything.

---

## 3. (Optional) Add the LLM strategy

Needs an Anthropic API key. Get one at [console.anthropic.com](https://console.anthropic.com).

```sh
export ANTHROPIC_API_KEY=sk-ant-...
go run ./cmd/mdp -paper -strategy momentum,meanrev,llm \
  -eval-interval 15m -llm-model claude-haiku-4-5
```

**Use `-eval-interval 15m`, not the 60s default.** At 60s an LLM strategy
racks up ~43,000 calls/month — real money on the API bill, separate from
the (fake) trading money. At 15m it's a few dollars a month. The process
warns you at startup if you forget.

---

## 4. (Optional) Real orders via Alpaca paper trading

A step up from the built-in simulator: this places actual orders into
Alpaca's paper-trading account — real order acceptance, real fills from
their engine, still fake money.

1. Sign up free at [alpaca.markets](https://alpaca.markets)
2. In their dashboard, switch to **Paper Trading** (top toggle) and generate
   an API key pair from there — **not** the live-trading keys page. Paper
   keys and live keys look similar; get this right.
3. Run:

```sh
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
go run ./cmd/mdp -paper -strategy momentum,meanrev \
  -alpaca -alpaca-strategy momentum -alpaca-eval-interval 5m
```

Watch for `"live fill"` in the log, then check it landed in your Alpaca
dashboard. This code defaults to Alpaca's paper endpoint and structurally
refuses to touch their live endpoint unless you pass two separate explicit
flags — see the README's *Real orders* section before you ever go near
`-alpaca-allow-live`.

---

## 5. Deploy for a real month-long run

Local `go run` stops when your laptop sleeps. For an actual month, it needs
to live somewhere that stays on. You mentioned Railway:

1. Push this repo to your own GitHub (or use this one directly)
2. [railway.app](https://railway.app) → New Project → Deploy from GitHub repo
3. **Attach a Volume, mounted at `/data`.** This is the step that's easy to
   skip and ruins everything if you do — without it, every redeploy wipes
   your archive and paper accounts and the month silently restarts at zero.
4. Set environment variables (Settings → Variables):

```
MDP_PAPER=true
MDP_STRATEGY=momentum,meanrev
MDP_SYMBOLS=BTC-USDT,ETH-USDT,SOL-USDT
MDP_RETAIN_RAW=24h
MDP_RETAIN_TICKS=336h
MDP_MAX_DB_BYTES=8000000000
```

Add `ANTHROPIC_API_KEY` if you're running `llm`, or `ALPACA_API_KEY` /
`ALPACA_API_SECRET` if you're running `-alpaca` (also add `-alpaca` and
`-alpaca-strategy` to the start command in that case).

5. Deploy. Railway builds the Dockerfile and starts it — no other config
   needed, `PORT` is picked up automatically.

Check it's alive:

```sh
curl -s https://<your-app>.up.railway.app/healthz
curl -s https://<your-app>.up.railway.app/v1/paper | jq .
```

Full sizing tables, retention tuning, and LLM cost breakdown are in the
[README](README.md#deploying-to-railway).

---

## Cheat sheet

| I want to... | Command |
|---|---|
| See it work right now | `go run ./cmd/fakevenue -rate 300` + `go run ./cmd/mdp -endpoint ws://127.0.0.1:9443/stream -paper -eval-interval 5s` |
| Run against real prices | `go run ./cmd/mdp -paper -strategy momentum,meanrev` |
| Check the scoreboard | `curl -s localhost:8080/v1/paper \| jq .` |
| Check a live price | `curl -s localhost:8080/v1/quote/BTC-USDT \| jq .` |
| Watch for backpressure problems | `curl -s localhost:8080/metrics \| jq .subscribers` |
| Break it on purpose | see README → *Deliberately breaking it* |

If something doesn't start, the error is almost always one of: Go version
too old (`go version`), port 8080 already in use (add `-http :8090`), or a
missing `data/` directory (the binary creates it, but check disk permissions
if it fails).
