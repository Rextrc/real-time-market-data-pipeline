# Getting started

The fastest path to seeing it work, then the path to a real month-long run.
Everything here uses fake or paper money — nothing in this guide can lose
real funds.

**Just want to upload it and have it running, no local setup?** Skip to
[Deploy straight to Railway](#deploy-straight-to-railway) below — it's a
GitHub-connect and four env vars, no `go run` required.

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

## 5. (Optional) Trade by texting it

Lets you text the bot things like `buy 2 shares of AAPL with a take profit
of 160 and stop loss of 140` and it executes them against your Alpaca
account (same paper-by-default safety as above — same keys, same interlock).

1. Message [@BotFather](https://t.me/BotFather) on Telegram, send `/newbot`,
   follow the prompts. It gives you a token that looks like
   `123456789:AAH...`.
2. Send your new bot any message (Telegram won't let it message you first).
3. Find your chat ID: open
   `https://api.telegram.org/bot<your token>/getUpdates` in a browser — it's
   the `"chat":{"id": ...}` number in the reply. This is what locks the bot
   to only you; without it anyone who finds the bot's username could place
   orders on your account.
4. Run:

```sh
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
go run ./cmd/mdp -telegram
```

Text your bot `help` to see the command list, or just try `buy 1 shares of
AAPL`. `-telegram-max-notional` (default `$1000`, env
`MDP_TELEGRAM_MAX_NOTIONAL`) caps how big a single order it'll place no
matter what you type — raise it once you trust the parsing, not before.

---

## 6. (Optional) Trade a basket of stocks automatically

No texting required — this evaluates and trades a list of stocks on its own,
continuously.

```sh
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
go run ./cmd/mdp -equity-auto
```

Defaults to ten liquid large-caps and trades whenever its bandit strategy
sees a crossover — how often that actually happens depends on real price
movement, not a fixed schedule. Watch for `"equity fill"` in the log. See
the README's *Automatic stock trading* section before raising
`-equity-max-position` or pointing it anywhere but the paper endpoint.

Want Claude weighing in on each decision instead of (or alongside) the
mechanical bandit? Add `ANTHROPIC_API_KEY` and:

```sh
go run ./cmd/mdp -equity-auto -equity-strategy llm+adaptive
```

---

## 7. Deploy straight to Railway

No local Go, no terminal commands on your machine at all — Railway builds
the repo's Dockerfile and runs it. This is the "upload it and it's running"
path.

**Step 1 — get the code into your own GitHub**, so Railway has something to
deploy from:

- Easiest: on this repo's GitHub page, click **Fork** (top right) to get
  your own copy under your account.
- Or if you already cloned it locally, push it to a new repo of yours:
  `git remote add mine https://github.com/<you>/<name>.git && git push mine claude/market-data-pipeline-1amxx1:main`

**Step 2 — connect it to Railway:**

1. [railway.app](https://railway.app) → **New Project** → **Deploy from
   GitHub repo** → pick your fork.
2. Railway detects the `Dockerfile` and `railway.json` in the repo
   automatically — nothing to configure there. It'll try to build and
   deploy immediately; that first deploy will come up but with an empty
   archive on ephemeral storage, which step 3 fixes.

**Step 3 — attach a Volume.** This is the one step that's easy to skip and
ruins everything if you do: without it, every redeploy wipes the archive
*and* the paper trading accounts, and a month-long run silently restarts
from zero each time you push a change.

In the service → **Settings** → **Volumes** → **New Volume** → mount path
`/data`. Takes 30 seconds.

**Step 4 — set the trading config.** Service → **Variables** → **Raw
Editor**, paste:

```
MDP_PAPER=true
MDP_STRATEGY=momentum,meanrev
MDP_SYMBOLS=BTC-USDT,ETH-USDT,SOL-USDT
MDP_RETAIN_RAW=24h
MDP_RETAIN_TICKS=336h
MDP_MAX_DB_BYTES=8000000000
```

That's the whole thing — momentum and mean-reversion trading fake money
side by side, real BTC/ETH/SOL prices, sized retention so a small volume
doesn't fill up over a month. `PORT` is handled automatically; don't set it.

Optional additions to the same variable block:
- `ANTHROPIC_API_KEY=sk-ant-...` plus changing `MDP_STRATEGY` to include
  `llm` — see step 3 above for why you also want `MDP_EVAL_INTERVAL=15m` if
  you do this.
- `ALPACA_API_KEY` / `ALPACA_API_SECRET` for real paper-account orders — see
  step 4 above; also add `MDP_ALPACA=true` and `MDP_ALPACA_STRATEGY=adaptive`
  to the variables.
- `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` (plus the same `ALPACA_API_KEY` /
  `ALPACA_API_SECRET` above) for the Telegram bot from step 5 — also add
  `MDP_TELEGRAM=true`.
- The same `ALPACA_API_KEY` / `ALPACA_API_SECRET` again plus
  `MDP_EQUITY_AUTO=true` for the automatic stock basket from step 6 — add
  `MDP_EQUITY_STRATEGY=llm+adaptive` and `ANTHROPIC_API_KEY` too if you want
  Claude driving those decisions.

**Step 5 — save.** Railway redeploys automatically whenever you change a
variable or push to the branch. Watch the **Deployments** tab; once it says
healthy, you're live.

Check it from anywhere:

```sh
curl -s https://<your-app>.up.railway.app/healthz
curl -s https://<your-app>.up.railway.app/v1/paper | jq .
```

(Railway gives you that `.up.railway.app` URL under **Settings** →
**Networking** → **Generate Domain**, if it isn't already showing one.)

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
| Trade by texting the bot | see step 5 above; then just text it `buy 2 shares of AAPL` |
| Break it on purpose | see README → *Deliberately breaking it* |

If something doesn't start, the error is almost always one of: Go version
too old (`go version`), port 8080 already in use (add `-http :8090`), or a
missing `data/` directory (the binary creates it, but check disk permissions
if it fails).
