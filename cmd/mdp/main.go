// Command mdp runs the market data pipeline.
//
// This file is the only place in the program where concrete implementations
// are chosen and connected. Every other package depends on interfaces, which
// is what will make "move this consumer into its own process" a copy-paste
// rather than a refactor.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/instrument"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/normalize"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue/binance"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "mdp: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		symbols      = flag.String("symbols", "BTC-USDT,ETH-USDT", "comma-separated canonical instrument IDs")
		endpoint     = flag.String("endpoint", binance.DefaultEndpoint, "websocket endpoint (point at toxiproxy to inject faults)")
		duration     = flag.Duration("duration", 0, "exit after this long; 0 runs until interrupted")
		pingInterval = flag.Duration("ping-interval", 20*time.Second, "liveness probe period; 0 disables pings (M1 break-it exercise)")
		readTimeout  = flag.Duration("read-timeout", 5*time.Minute, "backstop deadline on a single read")
		bufferSize   = flag.Int("buffer", 256, "raw frame channel capacity")
		quiet        = flag.Bool("quiet", false, "suppress per-tick output, print periodic counts instead")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	instruments, err := parseInstruments(*symbols)
	if err != nil {
		return err
	}

	// --- wiring ---------------------------------------------------------
	clock := model.SystemClock{}

	registry := instrument.NewRegistry()
	if err := registry.Register(binance.Symbols()...); err != nil {
		return err
	}

	src := binance.New(registry, clock,
		binance.WithEndpoint(*endpoint),
		binance.WithPingInterval(*pingInterval),
		binance.WithReadTimeout(*readTimeout),
	)

	norm := normalize.New(normalize.NewBinanceDecoder(registry))
	// --------------------------------------------------------------------

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Info("starting",
		"venue", src.ID(),
		"instruments", instruments,
		"endpoint", *endpoint,
		"ping_interval", *pingInterval)

	// One buffered channel between ingest and processing. Note that this is
	// NOT yet a solution to backpressure — it is a fixed-size buffer with no
	// policy. When it fills, Stream blocks, which is the failure mode M5
	// exists to make visible and deliberate.
	raw := make(chan venue.RawMessage, *bufferSize)

	streamDone := make(chan error, 1)
	go func() {
		// Ingest dying must stop the consumer too. Without this cancel, a
		// dial failure at startup would sit unreported behind a consumer
		// happily draining an empty channel until the run timer expired.
		// Until M6 adds a supervisor, a dead connection is a dead process,
		// and it should say so immediately.
		defer cancel()
		streamDone <- src.Stream(ctx, instruments, raw)
	}()

	err = consume(ctx, log, norm, raw, *quiet)

	// Wait for the reader to unwind so we never exit with a live connection
	// or an in-flight goroutine. Counting goroutines before and after a
	// hundred restarts is the M6 exercise; this is what makes the count stay
	// flat.
	streamErr := <-streamDone

	switch {
	case streamErr != nil && !errors.Is(streamErr, context.Canceled) && !errors.Is(streamErr, context.DeadlineExceeded):
		return streamErr
	case err != nil:
		return err
	}
	return nil
}

// consume drains raw frames, normalizes them, and prints. In M4 this becomes
// the bus and its subscribers; for now it is one synchronous consumer, which
// keeps the coupling obvious.
func consume(ctx context.Context, log *slog.Logger, norm *normalize.Normalizer, raw <-chan venue.RawMessage, quiet bool) error {
	var (
		ticks    uint64
		ignored  uint64
		failed   uint64
		lastRept = time.Now()
	)

	report := func() {
		log.Info("counters", "ticks", ticks, "ignored", ignored, "decode_errors", failed)
	}
	defer report()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-raw:
			if !ok {
				return nil
			}

			decoded, err := norm.Normalize(msg)
			switch {
			case errors.Is(err, normalize.ErrIgnored):
				ignored++
				continue
			case err != nil:
				// A decode failure is a bug in our decoder or a change at the
				// exchange. It must be loud and counted, but it must never
				// take down ingest: one bad frame is not a reason to stop
				// capturing the other nine thousand.
				failed++
				log.Warn("decode failed", "err", err, "payload", truncate(msg.Payload, 256))
				continue
			}

			for _, t := range decoded {
				ticks++
				if !quiet {
					fmt.Println(t)
				}
			}

			if quiet && time.Since(lastRept) >= 5*time.Second {
				report()
				lastRept = time.Now()
			}
		}
	}
}

func parseInstruments(s string) ([]model.InstrumentID, error) {
	fields := strings.Split(s, ",")
	out := make([]model.InstrumentID, 0, len(fields))
	for _, f := range fields {
		f = strings.ToUpper(strings.TrimSpace(f))
		if f == "" {
			continue
		}
		out = append(out, model.InstrumentID(f))
	}
	if len(out) == 0 {
		return nil, errors.New("no instruments requested")
	}
	return out, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
