# mdp — real-time market data pipeline

A learning project for backend concurrency, backpressure, and pub/sub.
See [DESIGN.md](DESIGN.md) for the architecture, the milestone plan, and the
reasoning behind the decisions that are expensive to reverse.

**Status: M1 complete.** Connect, normalize, print. No storage, no fan-out,
no reconnect yet — those are M3, M4, and M6.

## Run it

```sh
go run ./cmd/mdp -symbols BTC-USDT,ETH-USDT
```

```
binance BTC-USDT   seq=1      sell 0.00312000 @ 68420.51000000  lag=104ms
binance ETH-USDT   seq=1      buy 1.50000000 @ 3512.44000000    lag=98ms
binance BTC-USDT   seq=2      buy 0.00100000 @ 68420.52000000   lag=87ms
```

Flags worth knowing:

| Flag | Default | Why you'd change it |
|---|---|---|
| `-symbols` | `BTC-USDT,ETH-USDT` | canonical IDs; see `internal/venue/binance/symbols.go` |
| `-endpoint` | Binance public combined stream | point at toxiproxy to inject faults |
| `-ping-interval` | `20s` | **set to `0` for the M1 break-it exercise** |
| `-read-timeout` | `5m` | backstop only; liveness is the ping's job |
| `-duration` | `0` (forever) | bounded runs |
| `-quiet` | `false` | counters every 5s instead of per-tick output |

```sh
go test -race ./...
```

The tests need no network. `internal/venue/binance/e2e_test.go` runs the whole
M1 pipeline against a local websocket server and is the milestone's
acceptance test.

## What M1 is supposed to teach

**A dead TCP connection is indistinguishable from a quiet one.** There is no
read timeout that separates them: any deadline short enough to catch a
failure will also kill a healthy stream on a quiet instrument. Only an
application-level heartbeat can tell the difference. That is why
`internal/venue/binance` runs a ping loop alongside the read loop, and why
the read deadline is a five-minute backstop rather than the primary
mechanism.

### Break it deliberately

1. **Remove the heartbeat.**

   ```sh
   go run ./cmd/mdp -ping-interval 0 -read-timeout 1h -symbols BTC-USDT
   ```

   Now black-hole the traffic rather than resetting it — with
   [toxiproxy](https://github.com/Shopify/toxiproxy):

   ```sh
   toxiproxy-server &
   toxiproxy-cli create --listen localhost:9443 --upstream stream.binance.com:9443 binance
   go run ./cmd/mdp -endpoint ws://localhost:9443/stream -ping-interval 0 -read-timeout 1h
   toxiproxy-cli toxic add --type timeout --attribute timeout=0 binance
   ```

   Output stops. The process does not exit, does not log an error, and looks
   healthy from the outside. Predict how long it would take you to notice in
   production.

2. **Put the heartbeat back** (drop `-ping-interval 0`) and repeat. It now
   fails within `ping-interval + ping-timeout` with `no pong within 10s`.

3. **Reset instead of black-holing** (`toxiproxy-cli toxic add --type reset_peer`)
   and note that this one *is* caught without a heartbeat, which is exactly
   why the failure in step 1 is easy to miss: the common case reports itself.

4. **Watch for leaks.** `TestStreamLeavesNoGoroutines` opens twenty
   connections against a long-lived context and asserts the goroutine count
   stays flat. Delete the `defer cancel()` in `Stream` and watch it catch one
   leaked ping loop per connection — the bug that stays invisible until hour
   six of a run.

## Layout

```
cmd/mdp/            the only place wiring happens
internal/model/     Tick, Decimal, Clock — imports nothing
internal/instrument/  canonical <-> venue symbol mapping
internal/venue/     ingest boundary (transport only)
internal/normalize/ wire formats -> Tick, sequence assignment
```

The rules that keep this cheap to extend are in DESIGN.md §2. The two that
do the most work: `model` imports nothing, and no package outside `cmd/mdp`
imports a concrete implementation.

## Notes from building M1

Two bugs worth remembering, both caught by tests rather than by reading:

- **`encoding/json` matches keys to tags case-insensitively.** Binance sends
  both `m` (buyer-is-maker) and `M` (a deprecated ignore flag). Without a
  field bound to `M`, it also binds to `m` and inverts the side of every
  trade — silently, and in a way no aggregate would reveal. See
  `binanceTrade` in `internal/normalize/binance.go`.
- **Publish the diagnosis before closing the connection.** Closing the socket
  to unblock a stuck read makes that read fail with "use of closed network
  connection", which will win the race and bury the real reason. The ping
  loop sends its error, *then* closes.
