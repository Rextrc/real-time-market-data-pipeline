// Command mdp runs the market data pipeline.
//
// This file is the only place in the program where concrete implementations
// are chosen and connected. Every other package depends on interfaces, which
// is what makes "move this consumer into its own process" a copy-paste
// rather than a refactor.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/api/httpapi"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/api/wsapi"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/bus/inproc"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/candles"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/paper"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/consumer/persist"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/instrument"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/normalize"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/store/sqlite"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/strategy"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/supervise"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue/binance"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "mdp: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	symbols  string
	endpoint string
	dbPath   string
	httpAddr string
	duration time.Duration

	durability   string
	batchSize    int
	flushEvery   time.Duration
	candleEvery  time.Duration
	storeRaw     bool
	logLevel     string
	pingInterval time.Duration

	paperEnabled bool
	paperCash    float64
	feeRate      float64
	slippage     float64
	maxPosition  float64
	strategyName string
	evalEvery    time.Duration
	fillLatency  time.Duration
	paperState   string
	llmModel     string
}

func run() error {
	var c config
	flag.StringVar(&c.symbols, "symbols", "BTC-USDT,ETH-USDT,SOL-USDT", "canonical instrument IDs")
	flag.StringVar(&c.endpoint, "endpoint", binance.DefaultEndpoint, "websocket endpoint")
	flag.StringVar(&c.dbPath, "db", "data/mdp.db", "SQLite database path")
	flag.StringVar(&c.httpAddr, "http", ":8080", "HTTP listen address; empty disables the API")
	flag.DurationVar(&c.duration, "duration", 0, "exit after this long; 0 runs until interrupted")
	flag.StringVar(&c.durability, "durability", "normal", "sqlite fsync policy: full|normal|off")
	flag.IntVar(&c.batchSize, "batch", 500, "ticks per storage transaction")
	flag.DurationVar(&c.flushEvery, "flush", 250*time.Millisecond, "max time a tick waits before being written")
	flag.DurationVar(&c.candleEvery, "candle-interval", time.Minute, "OHLCV bar size")
	flag.BoolVar(&c.storeRaw, "store-raw", true, "also persist undecoded exchange frames")
	flag.StringVar(&c.logLevel, "log-level", "info", "debug|info|warn|error")
	flag.DurationVar(&c.pingInterval, "ping-interval", 20*time.Second, "venue liveness probe; 0 disables")

	flag.BoolVar(&c.paperEnabled, "paper", false, "enable paper trading")
	flag.Float64Var(&c.paperCash, "paper-cash", 10000, "starting balance in quote currency")
	flag.Float64Var(&c.feeRate, "paper-fee", 0.001, "fee per fill as a fraction of notional")
	flag.Float64Var(&c.slippage, "paper-slippage", 0.0005, "spread crossed per fill")
	flag.Float64Var(&c.maxPosition, "paper-max-position", 0.25, "max fraction of equity per position")
	flag.StringVar(&c.strategyName, "strategy", "momentum", "momentum|llm|llm+momentum")
	flag.DurationVar(&c.evalEvery, "eval-interval", 60*time.Second, "how often the strategy runs")
	flag.DurationVar(&c.fillLatency, "fill-latency", 500*time.Millisecond, "simulated execution delay")
	flag.StringVar(&c.paperState, "paper-state", "data/paper.json", "where the paper book is persisted")
	flag.StringVar(&c.llmModel, "llm-model", "", "override the Claude model ID")
	flag.Parse()

	log := newLogger(c.logLevel)

	instruments, err := parseInstruments(c.symbols)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if c.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.duration)
		defer cancel()
	}

	// ---- wiring: the only place concrete types are chosen ---------------
	clock := model.SystemClock{}

	registry := instrument.NewRegistry()
	if err := registry.Register(binance.Symbols()...); err != nil {
		return err
	}

	db, err := sqlite.Open(ctx, c.dbPath, sqlite.Durability(c.durability))
	if err != nil {
		return err
	}
	defer db.Close()

	src := binance.New(registry, clock,
		binance.WithEndpoint(c.endpoint),
		binance.WithPingInterval(c.pingInterval),
	)
	norm := normalize.New(normalize.NewBinanceDecoder(registry))
	b := inproc.New()
	defer b.Close()

	builder := candles.New(c.candleEvery, 1000)
	startedAt := time.Now()
	// ---------------------------------------------------------------------

	g, ctx := errgroup.WithContext(ctx)

	// Ingest, supervised. Each Stream call is a fresh connection; the
	// supervisor owns the retry policy so the venue adapter does not have to.
	raw := make(chan venue.RawMessage, 4096)
	g.Go(func() error {
		return supervise.Run(ctx, "binance", supervise.DefaultPolicy(), log, func(ctx context.Context) error {
			return src.Stream(ctx, instruments, raw)
		})
	})

	// Decode and publish. Sits between ingest and the bus so a decode error
	// is counted and logged without touching either side.
	g.Go(func() error { return pump(ctx, log, norm, b, db, raw, c.storeRaw) })

	// Persister: the lossless consumer. Its queue is deep and its policy is
	// drop-oldest rather than block, because blocking here would push
	// backpressure all the way to a socket the exchange controls — and that
	// converts a local slowdown into a disconnect. A rising drop count is
	// the alert that the disk cannot keep up.
	persistSub, err := b.Subscribe(bus.SubscriberSpec{
		Name: "persist", Capacity: 65536, Policy: bus.PolicyDropOldest,
	})
	if err != nil {
		return err
	}
	persister := persist.New(persistSub, db,
		persist.Config{BatchSize: c.batchSize, FlushInterval: c.flushEvery}, log)
	g.Go(func() error { return persister.Run(ctx) })

	// Candles: moderate speed, drop-oldest. A gap in bars is survivable;
	// unbounded memory is not.
	candleSub, err := b.Subscribe(bus.SubscriberSpec{
		Name: "candles", Capacity: 8192, Policy: bus.PolicyDropOldest,
	})
	if err != nil {
		return err
	}
	g.Go(func() error { return builder.Run(ctx, candleSub) })

	var engine *paper.Engine
	if c.paperEnabled {
		strat, err := buildStrategy(c, log)
		if err != nil {
			return err
		}

		acct := paper.NewAccount(paper.Config{
			StartingCash:        model.FromFloat(c.paperCash, 2),
			FeeRate:             c.feeRate,
			SlippageRate:        c.slippage,
			MaxPositionFraction: c.maxPosition,
		}, log)

		engine = paper.NewEngine(paper.EngineConfig{
			EvalInterval: c.evalEvery,
			FillLatency:  c.fillLatency,
			StatePath:    c.paperState,
		}, acct, strat, builder, binance.ID, instruments, log)

		// The strategy only needs current prices, so it conflates: one tick
		// per instrument is all it can act on anyway.
		paperSub, err := b.Subscribe(bus.SubscriberSpec{
			Name: "paper", Capacity: 1024, Policy: bus.PolicyCoalesce,
		})
		if err != nil {
			return err
		}
		g.Go(func() error { return engine.Run(ctx, paperSub) })

		log.Info("paper trading enabled",
			"strategy", strat.Name(), "cash", c.paperCash,
			"fee_rate", c.feeRate, "eval_interval", c.evalEvery)
	}

	if c.httpAddr != "" {
		ws := wsapi.New(b, log)
		g.Go(func() error { return ws.Run(ctx) })

		api := httpapi.New(httpapi.Deps{
			Store: db, Bus: b, Candles: builder, Paper: engine,
			Persister: persister, Venue: binance.ID,
			Instruments: instruments, StartedAt: startedAt, Log: log,
		})

		mux := http.NewServeMux()
		mux.Handle("/", api.Routes())
		mux.Handle("/v1/stream", ws.Handler())

		g.Go(func() error { return serveHTTP(ctx, log, c.httpAddr, mux) })
	}

	log.Info("started",
		"instruments", instruments, "db", c.dbPath,
		"http", c.httpAddr, "paper", c.paperEnabled)

	err = g.Wait()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}

	if engine != nil {
		snap := engine.Account().Snapshot(time.Now().UTC())
		log.Info("final paper result",
			"equity", snap.Equity.String(), "total_pl", snap.TotalPL.String(),
			"return_pct", fmt.Sprintf("%.2f", snap.ReturnPct),
			"trades", snap.Trades, "wins", snap.Wins, "losses", snap.Losses,
			"fees_paid", snap.FeesPaid.String(),
			"max_drawdown_pct", fmt.Sprintf("%.2f", snap.MaxDrawdownPct))
	}
	return err
}

// pump decodes raw frames and publishes ticks. It also writes the raw frame
// to storage when enabled — the cheapest insurance in the system, since it
// lets the whole history be re-normalized after a decoder fix.
func pump(
	ctx context.Context,
	log *slog.Logger,
	norm *normalize.Normalizer,
	b bus.Bus,
	db *sqlite.DB,
	raw <-chan venue.RawMessage,
	storeRaw bool,
) error {
	var (
		rawBatch  []sqlite.RawFrame
		rawTicker = time.NewTicker(2 * time.Second)
		decodeErr int
	)
	defer rawTicker.Stop()

	flushRaw := func() {
		if len(rawBatch) == 0 {
			return
		}
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		if err := db.AppendRawBatch(wctx, rawBatch); err != nil {
			log.Error("raw frame write failed", "err", err, "frames", len(rawBatch))
		}
		cancel()
		rawBatch = rawBatch[:0]
	}
	defer flushRaw()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-rawTicker.C:
			flushRaw()

		case msg := <-raw:
			if storeRaw {
				rawBatch = append(rawBatch, sqlite.RawFrame{
					Venue: msg.Venue, RecvTime: msg.RecvTime, Payload: msg.Payload,
				})
				if len(rawBatch) >= 500 {
					flushRaw()
				}
			}

			ticks, err := norm.Normalize(msg)
			switch {
			case errors.Is(err, normalize.ErrIgnored):
				continue
			case err != nil:
				// One bad frame is never a reason to stop capturing the
				// other nine thousand, but it must be loud and counted.
				decodeErr++
				if decodeErr <= 10 || decodeErr%100 == 0 {
					log.Warn("decode failed", "err", err, "total", decodeErr)
				}
				continue
			}

			if err := b.Publish(ctx, ticks); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func buildStrategy(c config, log *slog.Logger) (strategy.Strategy, error) {
	momentum := strategy.NewMomentum(9, 27)

	switch strings.ToLower(c.strategyName) {
	case "", "momentum":
		return momentum, nil

	case "llm":
		if os.Getenv("ANTHROPIC_API_KEY") == "" {
			return nil, errors.New("strategy=llm requires ANTHROPIC_API_KEY")
		}
		return strategy.NewLLM(strategy.LLMConfig{Model: c.llmModel}, log), nil

	case "llm+momentum":
		if os.Getenv("ANTHROPIC_API_KEY") == "" {
			return nil, errors.New("strategy=llm+momentum requires ANTHROPIC_API_KEY")
		}
		return strategy.NewLLM(strategy.LLMConfig{
			Model:    c.llmModel,
			Baseline: momentum,
		}, log), nil

	default:
		return nil, fmt.Errorf("unknown strategy %q", c.strategyName)
	}
}

func serveHTTP(ctx context.Context, log *slog.Logger, addr string, h http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: /v1/stream hijacks the connection and manages
		// its own deadlines. A blanket timeout would sever live streams.
		IdleTimeout: 120 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", addr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	}
}

func parseInstruments(s string) ([]model.InstrumentID, error) {
	fields := strings.Split(s, ",")
	out := make([]model.InstrumentID, 0, len(fields))
	for _, f := range fields {
		f = strings.ToUpper(strings.TrimSpace(f))
		if f != "" {
			out = append(out, model.InstrumentID(f))
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no instruments requested")
	}
	return out, nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
