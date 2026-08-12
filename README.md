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
| `GET /healthz` | liveness |
| `GET /metrics` | queue depths, drops, lag, persister counters, paper P&L |
| `GET /v1/instruments` | configured instruments + what the archive holds |
| `GET /v1/quote/{instrument}` | latest trade |
| `GET /v1/candles/{instrument}?limit=100` | OHLCV bars |
| `GET /v1/ticks?instrument=&start=&end=&limit=&order=` | historical range, cursor-paged |
| `GET /v1/paper` | account snapshot: equity, P&L, drawdown, positions |
| `GET /v1/paper/fills?limit=100` | trade log, each with the reason it fired |
| `WS /v1/stream?instruments=BTC-USDT` | live ticks |

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
go run ./cmd/mdp -paper -strategy momentum
```

| Flag | Default | Meaning |
|---|---|---|
| `-strategy` | `momentum` | `momentum`, `llm`, or `llm+momentum` |
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

**`momentum`** — a 9/27 SMA crossover. It is a baseline, not an edge:
crossover systems are the most-published trading rule in existence, which is
a good reason to expect no free money in one. On liquid pairs, after fees,
they historically bleed in ranging markets and make it back only in sustained
trends. Its P&L is the number any other strategy has to beat.

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

**Cost:** an evaluation is one API call. At `-eval-interval 60s` that's ~1,440
calls/day; the system prompt is cached, so most of the per-call input bills at
cache-read rates. Raise the interval to cut it.

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
