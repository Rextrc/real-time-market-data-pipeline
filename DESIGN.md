# Real-Time Market Data Pipeline — Design & Learning Plan

**Goals, in priority order**

1. Learn backend concurrency, backpressure, and pub/sub mechanics by building them.
2. Keep module boundaries clean enough that this could become a product later.
3. Explicitly *not* now: auth, billing, multi-tenancy, k8s, microservices.

Goal 1 and goal 2 pull in the same direction more often than not, with one
important exception noted below: the infrastructure that would make this
"production-grade" fastest (Kafka) is also the infrastructure that hides the
lesson you're trying to learn. That tension drives the main recommendation.

---

## 0. Language: Go

You left the choice open. Take Go.

| | Go | Rust | Python |
|---|---|---|---|
| Concurrency primitives you'd learn | goroutines, bounded channels, `select`, `context` — the exact vocabulary of this problem | `tokio`, `mpsc`/`broadcast`/`watch`, `Stream` — richest model, most precise | `asyncio` queues, tasks |
| Time spent on the *lesson* vs the *language* | high / low | medium / high | high / low |
| Does the runtime hide the failure modes? | No — you build the queues, you pick the policy | No | Partly. GIL + a single event loop make "slow consumer" look like "everything is slow", which muddies the diagnosis |
| Ops story for a later real product | single static binary | single static binary | packaging pain |

Go's `select` with a `default` clause **is** the drop-vs-block decision, written
in one line of syntax. That visibility is worth a lot here. Rust teaches the
same lessons more rigorously and will cost you 2–3× the wall-clock time,
mostly on borrow-checker fights that are orthogonal to concurrency
architecture. Python is fine for the ingest but makes the backpressure
experiments harder to interpret, because the GIL adds a second, confounding
source of stalling.

Everything below is Go-specific in its examples but the architecture is
language-neutral.

Concrete library picks (all cheap to reverse — do not agonize):

- WebSocket: `github.com/coder/websocket` (the maintained successor to
  `nhooyr.io/websocket`). Cleaner context integration than `gorilla/websocket`.
- Storage: `modernc.org/sqlite` (pure Go, no cgo) — keeps cross-compilation trivial.
- Lifecycle: `golang.org/x/sync/errgroup` + `context`. No DI framework.
- Metrics: `prometheus/client_golang`. Even locally — you need the numbers to
  *see* backpressure, and `curl /metrics` beats reading logs.
- Fault injection: `shopify/toxiproxy` as a local TCP proxy in front of the
  exchange. This is the single most useful tool in the whole plan; it turns
  "how do I test a disconnect" from an unanswerable question into a CLI call.

---

## 1. Architecture options and honest tradeoffs

The question "channels vs Redis Streams vs Kafka" is really the question
**where does the buffer live, and who is allowed to block whom?** That framing
makes the tradeoffs fall out.

### Option A — In-process channels

```
   ws ──► normalize ──► hub ──┬──► [queue] ──► persister   (lossless)
                              ├──► [queue] ──► candles     (moderate)
                              └──► [queue] ──► ws fanout   (conflating)
```

**Pros**

- Fan-out latency in the low microseconds. No serialization on the hot path.
- Backpressure is *yours*, visible in code, per consumer. You choose the policy
  at each edge, and you can change it in one line and immediately measure the
  difference. This is the learning goal, made tangible.
- Zero operational surface. `go run ./cmd/mdp` and it's up. Fast iteration loop
  matters more than it sounds for a learning project.
- Trivially testable: a fake venue and a fake clock give you deterministic tests
  with no containers.

**Cons**

- One failure domain. A panic in the candle builder kills ingest. (Mitigable
  with per-consumer recover, but the coupling is real.)
- No replay after a crash except from your own store, and no consumer groups.
- Consumers can't be in other processes or languages.
- Delivery is at-most-once by default. Nothing redelivers.
- Backpressure has nowhere to go. When the persister falls behind, RAM is your
  only buffer.

### Option B — Redis Streams

**Pros**

- Real broker semantics on one `docker run`: consumer groups, `XACK`, the
  pending-entries list, `XAUTOCLAIM` for stuck consumers. You learn at-least-once
  delivery, redelivery, and lag tracking — concepts that do not exist in Option A
  and that you cannot fake convincingly.
- Consumers become separate processes, so a crashed consumer no longer takes
  down ingest, and you can restart one independently.
- Introduces a genuinely different failure mode worth seeing: with `MAXLEN`
  trimming, the *broker* silently drops your oldest data; without it, the broker
  eats all your RAM. Neither is a corner case.

**Cons**

- Durability is fuzzy. AOF `everysec` can lose ~1s of writes on a hard kill;
  RDB snapshots lose much more. It is not a system of record — you'd still want
  your own store underneath, so it doesn't discharge the persistence requirement.
- Single-threaded server, single node. Fine to ~100k msg/s of small messages on
  a laptop, then it's a wall — and the wall is the server's CPU, which you can't
  shard your way around without Redis Cluster.
- You now pay serialization + a network hop per message, on every message. At
  tick rates that cost dominates your entire compute budget.
- Ordering is per-stream. Fan-out across many symbols means either one hot stream
  or many streams to manage.

### Option C — Kafka (or Redpanda)

**Pros**

- The genuinely correct answer at scale, and the model everything else imitates:
  partitions, per-key ordering, consumer groups with committed offsets,
  time-based retention, replay from an arbitrary offset.
- Its backpressure model is the important architectural lesson in the whole
  space: consumption is *pull*-based, so a slow consumer simply accrues lag and
  the producer never notices. Decoupling by durable log is the real answer to
  "how do I handle a slow consumer" in a large system.
- Storage and transport are the same thing, which collapses two of your
  requirements into one component.

**Cons — and one of these is disqualifying for you**

- **It solves your learning goal away.** If the broker absorbs everything, you
  never have to make the drop-vs-buffer-vs-block decision, because the answer is
  always "buffer, on the broker's disk, for seven days." You'd finish the project
  without having formed an opinion on the thing you set out to learn.
- Large operational surface for one machine. Redpanda removes the JVM and
  ZooKeeper but is still a serious dependency, and a slow local dev loop taxes
  every subsequent milestone.
- Higher latency floor (batching + `linger.ms` + fsync), and tuning it well is
  its own multi-week subject.
- Exactly-once semantics in Kafka is a deep, subtle topic. You will be tempted.
  It will eat a month.

### Recommendation

**Start with Option A. Build the `Bus` interface on day one. Add Option B at
Milestone 8 as a second implementation of that same interface, and A/B them.
Do not build Option C for this project.**

Rationale: in-process channels maximize learning-per-hour precisely *because*
they force you to solve backpressure by hand. Then swapping in Redis Streams
behind an unchanged interface teaches the second, deeper lesson — that moving
the queue out of process changes the *delivery contract*, not just the
transport — and it teaches it as a diff you can read, which is the best form.
Kafka after that is a config exercise, not a conceptual one; you'll have
learned its lessons already.

**Be honest about the leak in that interface.** An abstraction over
in-process channels and Kafka is not clean. Delivery semantics differ
(at-most-once vs at-least-once), acknowledgement models differ, ordering
scope differs, and message size limits differ. Do not pretend otherwise by
writing a lowest-common-denominator interface and hoping. Instead:

- Define the interface at the coarsest honest level (publish a batch; subscribe
  with a declared policy).
- Write every consumer to be **idempotent from day one**, even though today's
  in-process bus never redelivers. Then at-least-once later is a no-op for
  correctness rather than a rewrite. See §5.

---

## 2. Module boundaries

```
cmd/
  mdp/                 # the only place wiring happens; subcommands: run, replay, inspect
internal/
  model/               # Tick, InstrumentID, VenueID, Seq. Imports NOTHING.
  venue/
    venue.go           # interface: Stream(ctx, []InstrumentID) (<-chan RawMessage, error)
    binance/
    coinbase/
    synthetic/         # load generator: N ticks/sec on demand
    replay/            # reads from store, satisfies the same interface
  normalize/           # RawMessage -> Tick, per-venue decoders + symbol mapping
  bus/
    bus.go             # Bus interface, SubscriberSpec, Policy enum
    inproc/            # bounded queues + policy engine
    redisstream/       # Milestone 8
  store/
    store.go           # Appender + Reader interfaces
    sqlite/            # Milestone 3
    parquet/           # later, cold tier
  consumer/
    persist/           # lossless
    candles/           # OHLCV aggregation, moderate speed
    slowpoke/          # deliberately, configurably slow — a first-class component
  api/
    http/              # snapshot, health, metrics
    ws/                # live push, per-client conflation
  obs/                 # metrics + structured logging, one definition of each
  supervise/           # backoff, restart, lifecycle
```

**The rules that actually keep extension cheap** (each is worth more than the
directory layout itself):

1. **`model` imports nothing.** Everything imports `model`. Dependency arrows
   point inward, always. If you ever want to import `store` from `model`, the
   type belongs somewhere else.
2. **Wiring lives only in `cmd/mdp`.** No package imports a concrete venue,
   store, or bus implementation. This is what makes "extract this consumer into
   its own process" a copy-paste instead of a refactor.
3. **Every long-lived component is `New(deps...) (*T, error)` + `Run(ctx) error`.**
   No package-level state, no `init()`, no globals, no singletons. `Run` returns
   when `ctx` is cancelled and not before, and returns the reason.
4. **Serialization does not live in `model`.** Wire and disk encodings live in
   the `bus` and `store` implementations. Put JSON tags on your domain struct and
   you've silently made your internal type a public API you can't change.
5. **The bus knows nothing about consumers; consumers know nothing about venues.**
   The only shared vocabulary is `model.Tick`.
6. **Time is a dependency.** `type Clock interface { Now() time.Time }`, injected.
   You will need this for deterministic tests and for replay, and retrofitting it
   means touching everything.

**What *not* to build now**, because it's fake extensibility that costs real
time: a config framework (use env vars + flags), a plugin system, a DI
container, reflection-driven adapter registration, gRPC, an event-sourcing
framework, or a generic `interface{}`-typed message envelope. The interfaces
listed above are enough. Extensibility comes from four small interfaces and the
wiring rule, not from machinery.

---

## 3 & 4. Milestones — each independently runnable, each with a concept and a break-it exercise

Every milestone should end with something you can run and a test you can execute.
The "break it" step is not optional; it is where the learning is. Write down what
you predicted would happen before you run it, then compare.

### M1 — Connect and print

One symbol, one venue, dump raw frames to stdout. Ping/pong handling. Clean
shutdown on SIGINT via `context`.

- **Concept:** goroutine lifecycle and `context` cancellation; the WebSocket
  ping/pong keepalive contract.
- **Break it:** run behind toxiproxy and cut the connection with the proxy — but
  first, *omit* `SetReadDeadline` and observe that the read blocks forever with
  no error. Then black-hole the traffic (`toxiproxy timeout` toxic) rather than
  resetting it, and watch the same hang with the connection nominally "up".
- **Lesson:** a dead TCP connection is indistinguishable from a quiet one until
  you impose a deadline. Every read loop needs a deadline refreshed by pongs.

### M2 — Normalize, multi-symbol, the `Tick` type

Decode to `model.Tick`. Subscribe to 5–10 symbols on one connection. Assign a
per-`(venue, instrument)` monotonic sequence number at ingest.

- **Concept:** the adapter boundary; clock discipline — exchange event time vs
  local receive time vs monotonic time are three different things.
- **Break it:** feed a recorded frame with a missing field, a string where a
  number was expected, and a number that overflows. Then set your system clock
  backward 5 seconds mid-run and see what your ordering does.
- **Lesson:** never let wall-clock time be a sort key or a duration source.
  Carry all three timestamps; use monotonic for elapsed, exchange time for
  market semantics, receive time for pipeline diagnostics.

### M3 — Durable store

SQLite in WAL mode, batched inserts, one transaction per batch or per N ms.

- **Concept:** the durability/throughput dial — fsync frequency, batch size,
  write amplification.
- **Break it:** three experiments. (a) One insert per tick, no batching: measure
  the throughput collapse. (b) `kill -9` mid-batch and count exactly how many
  ticks you lost. (c) Set `PRAGMA synchronous=OFF`, kill again, and compare.
- **Lesson:** durability is a dial with named notches, not a boolean. You must be
  able to state which notch you're on and how many milliseconds of data that
  costs you.

### M4 — Fan-out hub with consumers of different speeds

Three consumers: persister (fast), candle builder (moderate), slowpoke
(configurably slow). Plus the synthetic venue as a load generator so you can dial
pressure precisely instead of waiting for a volatile market.

**Build the wrong version first, deliberately.** This is the highest-value hour
in the whole plan.

- **Concept:** bounded channels, `select`/`default`, per-subscriber queues,
  head-of-line blocking.
- **Break it, in three stages:**
  1. One shared **unbounded** channel → watch RSS climb until the OOM killer
     arrives. Graph the queue depth on the way up.
  2. One shared **bounded** channel → watch *every* consumer stall at the speed
     of the slowest one. That's head-of-line blocking, and it's why a single
     shared queue is almost always wrong.
  3. **Per-consumer queues** → only the slow consumer suffers. Now the question
     "what should happen to the slow one" is finally a real question, which is
     exactly M5.
- **Lesson:** fan-out means one queue per consumer, always. Sharing a queue
  couples the latency of unrelated consumers.

### M5 — Backpressure policies, made explicit and observable

Each subscriber declares its policy in its `SubscriberSpec`:

| Policy | Behavior on full queue | Correct for |
|---|---|---|
| `BlockProducer` | producer waits | replay/backfill only — a producer you control |
| `DropNewest` | discard incoming | sampling, non-critical telemetry |
| `DropOldest` | evict head, keep newest | live UI — staleness is worse than gaps |
| `CoalesceByKey` | keep only latest tick per instrument | the WebSocket fan-out; bounded by symbol count, not rate |
| `SpillToDisk` | overflow to a file-backed queue | the persister — must not lose data |

Metrics per subscriber: queue depth, high-water mark, drops, and lag
(`now - tick.recv_ts`).

- **Concept:** policy is a property of the *consumer's semantics*, not of the
  system. There is no global right answer, which is why a single global setting
  is always wrong.
- **Break it:** set the slow consumer to `BlockProducer` against the *live
  exchange* feed. The read loop stalls → you stop reading → the kernel receive
  buffer fills → the TCP window closes → you miss the ping deadline → the
  exchange disconnects you.
- **Lesson (the big one):** *slow-produce is not available to you here.* Applying
  backpressure to a producer you don't control converts a local queueing problem
  into a disconnect and a data gap. Against an exchange feed your only real
  choices are drop, buffer, or spill — and choosing means deciding what the data
  is *for*.

### M6 — Reconnect and recovery

Exponential backoff with jitter, resubscribe, gap detection using exchange
update IDs, and a supervisor that restarts a failed venue connection without
touching the rest of the pipeline.

- **Concept:** idempotency, at-least-once on reconnect, duplicate suppression,
  gap accounting, goroutine lifetime hygiene.
- **Break it:** toxiproxy for latency, packet loss, and mid-frame severing. Then
  force 100 reconnects in a tight loop and check `runtime.NumGoroutine()` and RSS
  before and after — reconnect paths are where goroutine leaks live, and one
  leaked reader per reconnect is invisible until hour six. Also: reconnect and
  confirm you don't double-write the overlapping ticks.
- **Lesson:** reconnection is not "call connect again." It's re-establishing
  subscription state, detecting what you missed, and not corrupting what you
  already have.

### M7 — Read API: HTTP snapshot + WebSocket live

`GET /instruments/{id}` for current state, `GET /healthz`, `/metrics`, and a WS
endpoint pushing live updates.

- **Concept:** snapshot-plus-delta consistency — subscribe *first*, then take the
  snapshot, then drop deltas at or below the snapshot's sequence. Also: your own
  clients are now producers of backpressure.
- **Break it:** connect a WS client and never read from the socket (`nc` to the
  endpoint, then `SIGSTOP` it). Watch your server's memory grow or its write
  block. Fix it with write deadlines, a per-client conflating queue, and a
  documented kick policy for clients that exceed it.
- **Lesson:** the egress side has the identical backpressure problem as ingest,
  and a misbehaving client of *yours* is exactly as dangerous as a slow consumer
  inside the process. Same taxonomy, same fixes.

### M8 — Swap the bus: Redis Streams behind the same interface

Implement `bus/redisstream` with consumer groups. Change one line in
`cmd/mdp`. Run both, compare.

- **Concept:** at-least-once delivery, `XACK`, the pending-entries list,
  `XAUTOCLAIM` for a dead consumer's un-acked work, `MAXLEN` trimming, and the
  cost of serialization per message.
- **Break it:** kill a consumer after it processes but before it ACKs; observe
  redelivery, and verify your consumers really are idempotent (M6's work pays
  off here — if it isn't, you'll see duplicate candles). Then set `MAXLEN` to
  something small and watch data vanish with no error anywhere. Then remove
  `MAXLEN` and watch Redis eat all your RAM.
- **Lesson:** the transport swap changed the *contract*. Delivery went from
  at-most-once to at-least-once, the buffer moved from your heap to someone
  else's, and "the queue is full" turned from an exception into silence.

### M9 — Replay and determinism (optional, high value)

`venue/replay` reads from the store and satisfies the venue interface, at 1× or
Nx speed.

- **Concept:** deterministic testing; time as an injected dependency.
- **Break it:** replay a captured disconnect storm twice and assert byte-identical
  candle output. Any nondeterminism you find is a real bug — usually a map
  iteration or an unsynchronized clock read.
- **Lesson:** a pipeline you can replay is a pipeline you can actually debug.

---

## 5. Decisions that are expensive to reverse

Get these right up front. Everything not on this list, decide fast and move on.

1. **The `Tick` schema and instrument identity.** Include: venue, canonical
   instrument ID, exchange event time, local receive time, ingest sequence,
   price, quantity, side, and the venue's own trade/update ID. Your captured
   history is the one asset you cannot regenerate — a tick you didn't capture,
   or captured wrong, is gone.

2. **Never `float64` for price or quantity.** Scaled `int64` (value + exponent)
   or a decimal type. This is irreversible in the worst way: the error is silent,
   it's tiny, and it's already baked into every row you've written by the time
   you notice.

3. **Persist the raw payload alongside the normalized tick** (compressed, or in
   a separate table/tier). It is the cheapest insurance in the entire system: it
   lets you re-normalize your whole history when you find a decoder bug, which
   you will. Without it, a decoder bug means permanent data loss.

4. **Canonical symbol naming, with a per-venue mapping table.** `BTCUSDT`,
   `BTC-USD`, and `XBT/USD` are the same instrument. Pick your canonical form
   and translate at the venue boundary. Leaking venue-specific symbols into
   storage means rewriting the archive later.

5. **Declare the ordering guarantee now: ordered per `(venue, instrument)`,
   unordered across.** This matches Kafka's partition-by-key semantics exactly,
   so the guarantee survives the migration. If you assume global total order
   anywhere, you've built something that can never be sharded.

6. **Assume at-least-once and make consumers idempotent from day one**, even
   though the in-process bus can't redeliver yet. Give each tick a stable
   identity and make every write an upsert or otherwise replay-safe. Retrofitting
   idempotency into a stateful aggregator is a rewrite, not a patch.

7. **Storage partitioning key: `(venue, instrument, day)`.** Whatever you pick
   determines your query patterns and your migration cost forever. Day-granularity
   partitioning is the safe default for tick data.

8. **Three timestamps, stored as UTC nanoseconds; monotonic clock for all
   durations.** Adding a timestamp column later means the old rows never have it.

9. **Backpressure policy is per-consumer and declared at subscribe time**, never
   a global config value. A global setting bakes in the assumption that all
   consumers want the same thing, which is exactly the assumption M5 disproves.

10. **The four interfaces — `Venue`, `Bus`, `Store`, `Consumer` — and the
    wiring-only-in-`main` rule.** Nearly free today; a structural rewrite once
    there are twenty files on the wrong side of a boundary.

**Cheap to reverse — don't overthink these:** SQLite → Postgres/Parquet/DuckDB;
which metrics or logging library; HTTP router; candle aggregation logic; adding
venues; queue sizes and tuning constants; whether consumers are in-process or
out; the WebSocket library.

---

## Suggested order of attack

M1 → M2 → M3 gets you a system that captures data durably, which means the clock
starts on your dataset — worth doing in the first sitting or two, since every day
without it is a day of history you don't have. M4 and M5 are the core of the
learning goal; budget real time there and do the deliberately-broken versions.
M6 is what makes it survive overnight. M7 makes it usable. M8 is the conceptual
capstone.
