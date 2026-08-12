# mdp — real-time market data pipeline

Ingests live crypto trades over websockets, normalizes them, stores them
durably, fans them out to consumers running at different speeds, serves the
result over HTTP and websockets, and optionally paper-trades a strategy
against the live feed.

Single static binary. One SQLite file. No external services required.

See [DESIGN.md](DESIGN.md) for the architecture and the reasoning behind the
decisions that are expensive to reverse.

---

## Quick start

**No network needed** — a local feed generator ships with it:

```sh
go run ./cmd/fakevenue -rate 300 &
go run ./cmd/mdp -endpoint ws://127.0.0.1:9443/stream \
  -paper -candle-interval 1s -eval-interval 2s
```

**Against the real exchange:**

```sh
go run ./cmd/mdp -symbols BTC-USDT,ETH-USDT,SOL-USDT -paper
```

**Docker, for a long unattended run:**

```sh
docker compose up -d --build
docker compose logs -f
```

The archive lives on a named volume, so `docker compose down` and image
rebuilds do not take your captured history with them.

---

## API

| Endpoint | What it gives you |
|---|---|
| `GET /` | dashboard — scoreboard, equity curves, live prices, trade log, pipeline health |
| `GET /healthz` | liveness |
| `GET /metrics` | queue depths, drops, lag, persister counters, paper P&L |
| `GET /v1/instruments` | configured instruments + what the archive holds |
| `GET /v1/quote/{instrument}` | latest trade |
| `GET /v1/candles/{instrument}?limit=100` | OHLCV bars |
| `GET /v1/ticks?instrument=&start=&end=&limit=&order=` | historical range, cursor-paged |
| `GET /v1/paper` | scoreboard: every strategy side by side |
| `GET /v1/paper/{strategy}` | one account: equity, P&L, drawdown, positions |
| `GET /v1/paper/{strategy}/fills?limit=100` | trade log, each with the reason it fired |
| `GET /v1/paper/{strategy}/history?since=&limit=` | equity snapshots over time, for charting |
| `WS /v1/stream?instruments=BTC-USDT` | live ticks |

**Dashboard.** Visit the server's root URL in a browser (locally: `http://localhost:8080/`,
on Railway: your deployed URL) and you get a live view of the bot — no separate
frontend, no build step, it's a single HTML page embedded in the Go binary via
`go:embed`. It polls the API every 5s (prices, health, scoreboard) and every 60s
(equity history) and works in light or dark mode. Equity snapshots are recorded
every `-history-interval` (default 5m, env `MDP_HISTORY_INTERVAL`) — shorten it
if you want the chart to fill in faster on a short test run.

```sh
curl localhost:8080/v1/paper | jq .
curl 'localhost:8080/v1/ticks?instrument=BTC-USDT&limit=5' | jq .
```

Historical paging is cursor-based, not `OFFSET` — on a live feed, rows
arriving mid-scan would make an offset skip or repeat records.

---

## Paper trading

Simulated only. No exchange credentials, no orders, no way to lose money.

```sh
go run ./cmd/mdp -paper -strategy adaptive
```

| Flag | Default | Meaning |
|---|---|---|
| `-strategy` | `adaptive` | comma-separated: `adaptive`, `momentum`, `meanrev`, `llm`, `llm+momentum` — each gets its own independent account on identical data |
| `-paper-cash` | `10000` | starting balance |
| `-paper-fee` | `0.001` | fee per fill (10bps, ≈ Binance spot taker) |
| `-paper-slippage` | `0.0005` | spread crossed per fill |
| `-paper-max-position` | `0.25` | max fraction of equity per position |
| `-eval-interval` | `60s` | how often the strategy runs |
| `-fill-latency` | `500ms` | delay between signal and fill |

**The simulation is deliberately pessimistic**, because the alternative is
worse than useless. A paper engine that fills instantly at the last trade
price with no costs makes almost any strategy look profitable, and that
flattery is the most common reason a strategy that "worked" on paper loses
money live. Three real costs are modelled: fees on both sides, slippage (you
buy above and sell below the last print), and latency (fills use the price
after a delay, not the one that triggered the signal).

Still **not** modelled, all of which make live results worse: order book
depth, partial fills, rejects, exchange downtime, and funding. Treat paper
P&L as an optimistic bound, not a forecast.

### Strategies

**`adaptive`** (default) — a bandit over five SMA-crossover configurations
(2/6, 3/9, 5/15, 8/21, 13/34), fast to slow. On every evaluation it looks at
whichever of those "arms" is currently signaling a buy and picks one:
mostly the arm with the best realized return so far (an EWMA over that arm's
own closed trades), occasionally (15% of the time) a random eligible arm
instead, so one that's gone quiet still gets re-tested rather than written
off forever. Each position is closed only by the same arm that opened it, so
a trade's outcome is always credited to the rule that actually made the
call. The short arms are what make it trade many times a day instead of a
few times a week — that's also what makes it noisier and more fee-sensitive
than `momentum`. Read this as "chases whatever has recently been working,"
not as a forecaster: a streak of luck looks identical to real edge over a
handful of trades, and it adapts to a regime only after the regime has
already happened. It's a more active baseline to compare against, not a
strategy to trust because it says "adaptive."

**`momentum`** — a 9/27 SMA crossover. It is a baseline, not an edge:
crossover systems are the most-published trading rule in existence, which is
a good reason to expect no free money in one. On liquid pairs, after fees,
they historically bleed in ranging markets and make it back only in sustained
trends. Its P&L is the number any other strategy has to beat.

**`meanrev`** — buys when price falls 2σ below its 30-bar mean and sells when
it reverts. Deliberately near-opposite to momentum, so the two disagree most
of the time and the pair is more informative than either alone.

**`llm`** — asks Claude for a buy/sell/hold per instrument from recent
candles. Needs `ANTHROPIC_API_KEY`. **`llm+momentum`** additionally shows the
model the baseline's signals so it can agree or disagree explicitly.

Be clear-eyed about what this is. A language model is not a forecasting
model: it has no training signal for "what does BTC do in the next fifteen
minutes", it cannot see order flow or positioning, and its view of the market
is a few hundred numbers in a prompt. What it is genuinely good at is stating
a rationale in English, which makes it a *legible* strategy — every trade in
the log carries a reason you can read and argue with. The honest prior is
that it underperforms the momentum baseline after fees. Run both and let the
P&L settle it.

The model's entire authority is one of three words per instrument. Position
sizing, the per-position cap, and the cash check are enforced by the engine,
so a confused response cannot do anything worse than a bad trade — and an
unparseable one becomes a hold, never a guess.

**Cost:** an evaluation is one API call, and at the 60s default that is
~43,000 calls a month. See the cost table under *Deploying to Railway* before
running this unattended.

---

## Real orders: Alpaca paper trading

Everything above is simulated. This is not — it drives one strategy's
signals into Alpaca's actual paper-trading account: a real Alpaca account
with fake money, real order acceptance and rejection, real fills reported by
Alpaca itself. It is a step up in realism from the built-in paper engine, not
a replacement for it, and it needs Alpaca API keys.

```sh
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
go run ./cmd/mdp -paper -strategy adaptive,momentum,meanrev \
  -alpaca -alpaca-strategy adaptive -alpaca-eval-interval 2m
```

`-alpaca-strategy` builds a second, independent instance of one of the
strategies named in `buildStrategy` (same set as `-strategy`) and gives it a
real execution path — so you can watch the same kind of decisions play out in
both an in-process book and a real Alpaca account, side by side. It doesn't
have to also appear in `-strategy`; it's fine to run it live-only.

| Flag | Default | Meaning |
|---|---|---|
| `-alpaca` | off | enable real order execution |
| `-alpaca-strategy` | `adaptive` | which strategy drives it |
| `-alpaca-max-position` | `0.1` | max fraction of Alpaca account equity per order — enforced here, independent of the strategy and of whatever Alpaca itself would allow |
| `-alpaca-eval-interval` | `2m` | how often it can trade; also the ceiling on order frequency |
| `-alpaca-base-url` | Alpaca's paper endpoint | see below |
| `-alpaca-allow-live` | off | see below |

### The one interlock that matters here

The Alpaca SDK this is built on **defaults to the live endpoint** when none
is configured — not paper. That default is inverted in this codebase:
`New` defaults to paper and a non-paper URL is refused unless
`-alpaca-allow-live` is also set, and even then only Alpaca's actual live
host is accepted — the flag grants "trade for real on Alpaca," not "trust
any URL." No environment variable or typo can flip this on its own; reaching
live money requires two separate, explicit settings agreeing.

```
go: unrecognized alpaca error: alpaca: a non-paper base URL was given but
AllowLive is not set; this package defaults to Alpaca's paper endpoint and
requires an explicit opt-in to reach anything else
```

is the interlock working as intended, not a bug to route around.

### What "paper" means here, and what it doesn't fix

Alpaca's paper environment is a real account with simulated fills — real
order acceptance/rejection semantics, real position and cash tracking, no
local guesswork about slippage. It is **not** free of the risks that make
automated trading risky in the first place:

- **Crypto never closes.** There is no market-hours boundary to stop trading
  while you're asleep or the process is mid-redeploy. `-alpaca-eval-interval`
  is the only throttle — keep it conservative.
- **A bug in the strategy is now a bug with consequences**, even against
  fake money: a runaway buy loop would exhaust the paper account exactly as
  it would a real one. `-alpaca-max-position` is what limits the damage.
- **This does not become live trading by editing a flag.** It requires the
  interlock above, on purpose.

---

## Trading from Telegram

Text the bot an instruction, it executes it against Alpaca — same account,
same paper-by-default interlock as above. This is a manual, on-demand path
alongside the automated strategies, not a replacement for them: it exists
for "buy 2 shares of AAPL with a take profit of 160 and stop loss of 140"
typed as a sentence.

```sh
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
go run ./cmd/mdp -telegram
```

See `GETTING_STARTED.md` step 5 for getting a bot token from @BotFather and
finding your chat ID.

```
buy 2 shares of AAPL
buy 2 shares of AAPL with a take profit of 160 and stop loss of 140
buy 2 AAPL tp 5% sl 3%
buy $500 of tesla
sell 2 shares of AAPL
close AAPL          (sells the whole position)
price AAPL
positions
status
```

| Flag | Default | Meaning |
|---|---|---|
| `-telegram` | off | enable the bot |
| `-telegram-chat-id` | — | the only chat ID the bot acts on; required |
| `-telegram-max-notional` | `1000` | hard dollar cap per order, independent of Alpaca's own limits |

Three things keep a typo from being expensive:

- **One allowed chat.** Anyone who finds the bot's username can message it,
  but only `TELEGRAM_CHAT_ID` gets a response or an order — everything else
  is logged and silently ignored (no "not authorized" reply, so a stranger
  probing it learns nothing).
- **Fails closed on ambiguity.** The parser is regex-based, not an LLM, on
  purpose: an instruction either matches a known shape or comes back with
  exactly what it couldn't find ("couldn't find a stock symbol in..."). It
  never guesses at intent for something this consequential.
- **A `sell` only ever closes or trims a position the bot can see.** If you
  don't hold the symbol, or ask to sell more than you hold, it does nothing
  and says so rather than opening a short.

Take-profit/stop-loss are submitted as a real Alpaca bracket order — Alpaca
manages both exit legs and cancels whichever didn't fire, so the position
still exits correctly even if this process is redeployed or crashes between
the entry filling and the target being hit.

---

## Automatic stock trading

The automated strategies elsewhere in this README (`-strategy`, `-alpaca`)
trade the crypto pairs the Binance pipeline feeds them. This is the same
idea for a basket of stocks — no manual "buy" texts, it evaluates and trades
on its own, continuously, across several symbols at once.

```sh
export ALPACA_API_KEY=...
export ALPACA_API_SECRET=...
go run ./cmd/mdp -equity-auto
```

| Flag | Default | Meaning |
|---|---|---|
| `-equity-auto` | off | enable it |
| `-equity-symbols` | `AAPL,MSFT,NVDA,TSLA,AMD,META,AMZN,GOOGL,NFLX,COIN` | the traded basket |
| `-equity-poll-interval` | `30s` | how often each symbol's quote is sampled |
| `-equity-eval-interval` | `60s` | how often the strategy runs across the basket |
| `-equity-max-position` | `0.05` | max fraction of equity per stock position — smaller than the crypto default since several can be held at once |

There is no live stock tick stream elsewhere in this system — the crypto
pipeline is fed by Binance's websocket, and building a second one just for a
stock basket wasn't worth it here. Instead this engine polls Alpaca's quote
endpoint on a timer and builds its own short price history from that, which
it feeds to a bandit strategy (the same `Adaptive` used elsewhere) tuned
faster than the crypto default — shorter arms (down to 1/3), higher
exploration — since the point of this engine specifically is to trade often
across several symbols rather than sit on one long-lived call.

**Be honest with yourself about "trade often."** How many trades an hour
actually happen depends on real price movement, not a schedule — this
engine has no quota and will not manufacture a trade just to hit a number.
Short SMA arms on 30-second-sampled quotes *will* fire often, but that also
means it's the most whipsaw-prone, most fee-sensitive setup in this
repository. It stays paper-only by default via the same interlock as the
rest of the Alpaca integration. If you ever do point this at a live
account: trading this frequently will run into Alpaca's/FINRA's Pattern Day
Trader rule (4+ day trades in 5 business days requires $25k equity) well
within the first day — this is not something to run against real money
without understanding that first.

---

## Backpressure — the part worth understanding

Every consumer gets **its own queue** and declares **its own policy**. Sharing
one queue couples unrelated consumers: the slowest sets the pace for all of
them, which is head-of-line blocking and almost always a bug.

| Policy | On a full queue | Used by |
|---|---|---|
| `block` | producer waits | nothing here — see below |
| `drop_newest` | discard arriving | sampling, telemetry |
| `drop_oldest` | evict head, keep newest | persister, candles |
| `coalesce` | keep only latest per instrument | websocket fan-out, strategy |

**Nothing in this pipeline uses `block` against the exchange**, and that is
the central lesson. Blocking the publisher blocks the socket read; the kernel
receive buffer fills; the TCP window closes; the heartbeat is missed; the
exchange disconnects you. Backpressure applied to a producer that cannot slow
down doesn't slow it down — it converts a local queueing problem into a data
gap. Against a feed you don't control the only real choices are drop, buffer,
or conflate, and choosing means deciding what the data is *for*.

Coalescing is the one policy safe for an arbitrarily slow consumer: queue
depth is bounded by the number of instruments rather than by the tick rate.

Watch it live:

```sh
curl -s localhost:8080/metrics | jq .subscribers
```

`dropped` climbing means a consumer can't keep up. `blocked_nanos` above zero
means something is applying backpressure to ingest — on a live feed, treat
that as an incident.

---

## Deliberately breaking it

The feed generator has fault-injection flags, and the failures are worth
seeing rather than taking on trust.

**Slow consumer / memory growth** — flood it and watch the drop counters:

```sh
go run ./cmd/fakevenue -rate 20000 &
go run ./cmd/mdp -endpoint ws://127.0.0.1:9443/stream
curl -s localhost:8080/metrics | jq .subscribers
```

**Exchange disconnect** — sever every connection after 4s:

```sh
go run ./cmd/fakevenue -drop-after 4s &
go run ./cmd/mdp -endpoint ws://127.0.0.1:9443/stream
```

The supervisor reconnects with exponential backoff and jitter (~1s, 2s, 4s…).
Capture continues across the gaps; the archive is idempotent, so replayed
trades don't double-count.

**Dead-but-open connection** — the interesting one:

```sh
go run ./cmd/fakevenue -stall-after 5s &
go run ./cmd/mdp -endpoint ws://127.0.0.1:9443/stream -ping-interval 0 -read-timeout 1h
```

With `-ping-interval 0` the process sits there indefinitely, no error, looking
perfectly healthy, receiving nothing. Put the heartbeat back and it's caught
in seconds. **A dead TCP connection is indistinguishable from a quiet one** —
any read deadline short enough to catch a failure also kills a healthy stream
on a quiet instrument. Only an application-level ping separates the two.

**Wedged websocket client** — your own clients are as dangerous as the
exchange:

```sh
nc localhost 8080   # then SIGSTOP it
```

The server conflates per client, applies a write deadline, and kicks anything
that still can't keep up (`kicked` in `/metrics`).

**Durability** — `kill -9` mid-run and count what you lost:

```sh
go run ./cmd/mdp -durability off    # vs. normal, vs. full
```

---

## Deploying to Railway

The Dockerfile is the build; `railway.json` sets the health check and restart
policy. Two things will silently ruin a long run if you skip them.

**1. Attach a Volume, mounted at `/data`.** Railway's container filesystem is
ephemeral — without a volume, every redeploy and every restart wipes the
archive *and* the paper books, and a month-long experiment quietly restarts
from zero each time you push. `MDP_DB` and `MDP_PAPER_STATE` already point at
`/data`.

**2. Set retention to fit that volume.** See the sizing table below. Ingest
never stops; an unbounded archive ends the run when the disk fills.

`PORT` is injected by Railway and taken automatically — don't set `MDP_HTTP`.

### Environment variables

Every flag has an `MDP_`-prefixed equivalent. The ones that matter:

| Variable | Suggested | Why |
|---|---|---|
| `MDP_SYMBOLS` | `BTC-USDT,ETH-USDT,SOL-USDT` | more symbols = proportionally more disk |
| `MDP_PAPER` | `true` | enable trading |
| `MDP_STRATEGY` | `momentum,meanrev` | comma-separated; each gets its own account |
| `MDP_EVAL_INTERVAL` | `60s` (`15m` if using `llm`) | see the cost table |
| `MDP_RETAIN_RAW` | `24h` | raw frames are ~2/3 of the bytes |
| `MDP_RETAIN_TICKS` | fit your volume | `0` keeps forever |
| `MDP_MAX_DB_BYTES` | ~80% of volume | last-resort guard |
| `ANTHROPIC_API_KEY` | — | only for `llm` strategies |

### Sizing a month

Measured at ~434 bytes per tick with raw frames stored, ~150 without. At a
realistic ~50 trades/sec across three majors:

| | with raw | normalized only |
|---|---|---|
| per day | ~1.9 GB | ~0.65 GB |
| per month | ~56 GB | ~20 GB |

So a month of everything does not fit on a small volume, and the interesting
question is what to throw away. Settings that fit:

| Volume | `MDP_RETAIN_RAW` | `MDP_RETAIN_TICKS` | `MDP_MAX_DB_BYTES` |
|---|---|---|---|
| 5 GB | `6h` | `144h` (6 days) | `4000000000` |
| 10 GB | `24h` | `336h` (14 days) | `8000000000` |
| 50 GB | `72h` | `0` (keep all) | `40000000000` |

Retention runs hourly, deletes by age, and then drops whole days oldest-first
if the database is still over `MDP_MAX_DB_BYTES`. Freed pages are returned to
the filesystem via incremental auto-vacuum.

**A database created before this feature existed cannot reclaim space** — the
`auto_vacuum` mode is fixed at creation and silently ignores later attempts to
change it. The process warns at startup if it detects one; restart once with
`-vacuum-on-start` (or `MDP_VACUUM_ON_START=true`) to rebuild it.

### LLM cost over a month

An LLM evaluation is one API call, and the candle payload dominates the tokens
— the cached system prompt is a small fraction, so caching saves less here
than you'd hope. Rough monthly totals at ~2,100 input and ~300 output tokens
per call:

| interval | calls/month | Opus 5 | Sonnet 5 | Haiku 4.5 |
|---|---|---|---|---|
| `60s` | 43,200 | ~$790 | ~$470 | ~$160 |
| `5m` | 8,640 | ~$160 | ~$95 | ~$30 |
| `15m` | 2,880 | ~$50 | ~$30 | ~$10 |
| `1h` | 720 | ~$13 | ~$8 | ~$3 |

Estimates from list prices at time of writing — check current pricing. **The
60s default is fine for `momentum` and expensive for `llm`.** Set
`MDP_EVAL_INTERVAL=15m` and `MDP_LLM_MODEL=claude-haiku-4-5` unless you have a
reason not to; the process logs its projected call count at startup and warns
below 5 minutes.

A 15-minute cadence is not a handicap for this strategy. It reads candles, not
order flow — there is nothing in the data at 60s that isn't in it at 15m.

---

## Running a month

```
MDP_PAPER=true
MDP_STRATEGY=momentum,meanrev
MDP_RETAIN_RAW=24h
MDP_RETAIN_TICKS=336h
MDP_MAX_DB_BYTES=8000000000
```

Watch the scoreboard:

```sh
curl -s $URL/v1/paper | jq '.strategies[] | {strategy, pl: .account.total_pl, trades: .account.trades}'
curl -s $URL/metrics | jq '{archive, subscribers: [.subscribers[] | {name, dropped}]}'
```

Two operational facts worth knowing before you start:

**Candle history is in memory, not on disk.** After a restart or redeploy the
builder starts empty, and `momentum(9/27)` needs 29 closed bars before it can
signal — about half an hour on 1-minute candles. Paper books survive (they're
on the volume); the warm-up doesn't. Redeploying daily means the strategies
spend a meaningful share of the month blind, so batch your changes.

**`dropped` climbing on the persister means the disk can't keep up.** It's the
one counter worth alerting on. `blocked_nanos` above zero anywhere means
something is applying backpressure to ingest — on a live feed, that's an
incident.

### What a month will and won't tell you

It's a real test of the *system*: 720 hours of reconnects, redeploys, disk
pressure, and exchange hiccups is a genuine soak test, and things that survive
that usually work.

It is **not** a verdict on the strategies. A month is one sample of one
market regime. Momentum and mean reversion are near-opposites by construction,
so the informative result isn't which one won — it's the shape: if both made
money, the market trended and chopped in turn; if both lost, you're looking at
fees; if they mirror each other, you're looking at noise. Read the pair, not
the winner.

Nothing here can place a real order. There is no exchange credential in the
system and no code path to one.

---

## Tests

```sh
go test -race ./...
```

No network required. Coverage worth knowing about:

- **Backpressure policies** — each policy's eviction behavior, plus a test
  that a subscriber which never reads cannot stall a fast one.
- **Paper P&L** — a round trip at a flat price must *lose* about 30bps. A
  paper engine that breaks even there is lying.
- **Idempotency** — re-appending a batch stores zero rows.
- **Decimal math** — exactness across scales, and the overflow case below.
- **Goroutine leaks** — 20 connections against a long-lived context, verified
  to fail when the fix is removed.

---

## Layout

```
cmd/mdp/            the only place wiring happens
cmd/fakevenue/      local feed generator + fault injection
internal/model/     Tick, exact Decimal, Clock — imports nothing
internal/venue/     ingest boundary (transport only)
internal/normalize/ wire formats -> Tick, sequence assignment
internal/bus/       fan-out with per-subscriber backpressure policy
internal/store/     durable archive (SQLite)
internal/consumer/  persist, candles, paper
internal/strategy/  momentum, llm
internal/api/       HTTP + websocket
internal/supervise/ backoff and restart
```

`model` imports nothing. No package outside `cmd/mdp` names a concrete
implementation. Those two rules are what make the extensions cheap.

---

## Bugs this found, and what they cost

Kept because each one is a category, not a one-off:

**`encoding/json` matches keys to tags case-insensitively.** Binance sends
both `m` (buyer-is-maker) and `M` (a deprecated flag). Without a field bound
to `M`, it also binds to `m` and **inverts the side of every trade** —
silently, invisible in any aggregate. The field must also be *exported*;
`encoding/json` skips unexported ones, so the obvious fix doesn't work.

**Fixed-point multiply overflows at crypto scales.** A scale-8 price times a
scale-8 quantity is a scale-16 result with a mantissa around 1e20 — past
int64. It saturated, the notional became garbage, and the paper engine
silently rejected every order while reporting no error. Intermediates now go
through `big.Int`.

**A full-size order couldn't afford its own fee.** With
`-paper-max-position 1.0` the budget equalled cash exactly, so `notional +
fee > cash` and every order was rejected — again silently. The budget now
reserves fee headroom.

**Publish the diagnosis before closing the connection.** Closing a socket to
unblock a stuck read makes that read fail with "use of closed network
connection", which wins the race and buries the real reason. The ping loop
sends its error, *then* closes.

Two of those four produced no error at all — they just quietly did nothing,
or did the wrong thing. That's the failure mode to design tests around.
