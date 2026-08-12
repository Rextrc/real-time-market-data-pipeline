package binance_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/instrument"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/normalize"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue/binance"
)

type clock struct{ t time.Time }

func (c clock) Now() time.Time { return c.t }

// TestEndToEnd wires the M1 pipeline exactly as cmd/mdp does — venue,
// registry, normalizer — and drives real-shaped Binance frames through it.
// It is the milestone's acceptance test and needs no network.
func TestEndToEnd(t *testing.T) {
	frames := []string{
		// A subscription acknowledgement: must be ignored, not counted as
		// a failure and not counted as a tick.
		`{"result":null,"id":1}`,
		`{"stream":"btcusdt@trade","data":{"e":"trade","E":1710000000123,"s":"BTCUSDT",` +
			`"t":100,"p":"68420.51000000","q":"0.00312000","T":1710000000100,"m":true,"M":true}}`,
		`{"stream":"ethusdt@trade","data":{"e":"trade","E":1710000000200,"s":"ETHUSDT",` +
			`"t":200,"p":"3512.44000000","q":"1.50000000","T":1710000000190,"m":false,"M":true}}`,
		`{"stream":"btcusdt@trade","data":{"e":"trade","E":1710000000300,"s":"BTCUSDT",` +
			`"t":101,"p":"68420.52000000","q":"0.00100000","T":1710000000290,"m":false,"M":true}}`,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()

		// Assert the subscription actually asked for what we wanted.
		if got := r.URL.RawQuery; got != "streams=btcusdt@trade/ethusdt@trade" {
			t.Errorf("subscription query = %q", got)
		}
		for _, f := range frames {
			if err := c.Write(r.Context(), websocket.MessageText, []byte(f)); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	reg := instrument.NewRegistry()
	if err := reg.Register(binance.Symbols()...); err != nil {
		t.Fatalf("register: %v", err)
	}

	recv := time.UnixMilli(1710000000400).UTC()
	src := binance.New(reg, clock{recv},
		binance.WithEndpoint("ws"+strings.TrimPrefix(srv.URL, "http")))
	norm := normalize.New(normalize.NewBinanceDecoder(reg))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	raw := make(chan venue.RawMessage, len(frames))
	go func() { _ = src.Stream(ctx, []model.InstrumentID{"BTC-USDT", "ETH-USDT"}, raw) }()

	var (
		ticks   []model.Tick
		ignored int
	)
	for len(ticks) < 3 {
		select {
		case msg := <-raw:
			decoded, err := norm.Normalize(msg)
			if err == normalize.ErrIgnored {
				ignored++
				continue
			}
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			ticks = append(ticks, decoded...)
		case <-ctx.Done():
			t.Fatalf("timed out with %d ticks", len(ticks))
		}
	}

	if ignored != 1 {
		t.Errorf("ignored = %d, want 1", ignored)
	}

	want := []struct {
		inst  model.InstrumentID
		seq   model.Seq
		price string
		side  model.Side
	}{
		{"BTC-USDT", 1, "68420.51000000", model.SideSell},
		{"ETH-USDT", 1, "3512.44000000", model.SideBuy},
		{"BTC-USDT", 2, "68420.52000000", model.SideBuy},
	}
	for i, w := range want {
		got := ticks[i]
		if got.Instrument != w.inst || got.Seq != w.seq ||
			got.Price.String() != w.price || got.Side != w.side {
			t.Errorf("tick %d = %+v, want %s seq=%d %s %s",
				i, got, w.inst, w.seq, w.side, w.price)
		}
		if got.RecvTime != recv {
			t.Errorf("tick %d RecvTime = %v, want %v", i, got.RecvTime, recv)
		}
	}
}
