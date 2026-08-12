package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/instrument"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue"
)

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func testRegistry(t *testing.T) *instrument.Registry {
	t.Helper()
	reg := instrument.NewRegistry()
	if err := reg.Register(Symbols()...); err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg
}

// wsServer starts a websocket server running handle, and returns its ws:// URL.
func wsServer(t *testing.T, handle func(ctx context.Context, c *websocket.Conn)) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		handle(r.Context(), c)
	}))
	t.Cleanup(srv.Close)

	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func TestStreamURL(t *testing.T) {
	v := New(testRegistry(t), fixedClock{}, WithEndpoint("wss://example.test/stream"))

	got, err := v.streamURL([]model.InstrumentID{"BTC-USDT", "ETH-USDT"})
	if err != nil {
		t.Fatalf("streamURL: %v", err)
	}
	// The separators must survive literally; percent-encoding them yields a
	// URL Binance rejects.
	want := "wss://example.test/stream?streams=btcusdt@trade/ethusdt@trade"
	if got != want {
		t.Errorf("streamURL = %q, want %q", got, want)
	}
}

func TestStreamURLRejectsUnmappedInstrument(t *testing.T) {
	v := New(testRegistry(t), fixedClock{})

	if _, err := v.streamURL([]model.InstrumentID{"NOPE-USDT"}); err == nil {
		t.Error("expected an error for an instrument with no venue symbol")
	}
	if _, err := v.streamURL(nil); err == nil {
		t.Error("expected an error for an empty instrument list")
	}
}

func TestStreamForwardsFrames(t *testing.T) {
	const frames = 3
	url := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		for i := 0; i < frames; i++ {
			if err := c.Write(ctx, websocket.MessageText, []byte(`{"stream":"btcusdt@trade","data":{}}`)); err != nil {
				return
			}
		}
		<-ctx.Done()
	})

	now := time.UnixMilli(1710000000000).UTC()
	v := New(testRegistry(t), fixedClock{now}, WithEndpoint(url))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := make(chan venue.RawMessage, frames)
	errc := make(chan error, 1)
	go func() { errc <- v.Stream(ctx, []model.InstrumentID{"BTC-USDT"}, out) }()

	for i := 0; i < frames; i++ {
		select {
		case msg := <-out:
			if msg.Venue != model.VenueBinance {
				t.Errorf("Venue = %q, want binance", msg.Venue)
			}
			// RecvTime comes from the injected clock, not time.Now.
			if !msg.RecvTime.Equal(now) {
				t.Errorf("RecvTime = %v, want %v", msg.RecvTime, now)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for frame %d", i)
		}
	}

	cancel()
	if err := <-errc; err != context.Canceled {
		t.Errorf("Stream returned %v, want context.Canceled", err)
	}
}

// TestStreamDetectsUnresponsivePeer is the M1 lesson as a regression test.
// The server accepts the connection and then stops reading, so it never
// answers a ping. The socket stays open and looks perfectly healthy at the
// TCP layer; only the application-level heartbeat notices.
func TestStreamDetectsUnresponsivePeer(t *testing.T) {
	url := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		<-ctx.Done() // never reads, so never pongs
	})

	v := New(testRegistry(t), fixedClock{time.Now()},
		WithEndpoint(url),
		WithPingInterval(50*time.Millisecond),
		WithPingTimeout(150*time.Millisecond),
		// Deliberately far longer than the test: if the read backstop were
		// what caught this, the test would hang instead of failing fast.
		WithReadTimeout(time.Hour),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := make(chan venue.RawMessage, 1)
	err := v.Stream(ctx, []model.InstrumentID{"BTC-USDT"}, out)

	if err == nil {
		t.Fatal("Stream returned nil, want a heartbeat failure")
	}
	if !strings.Contains(err.Error(), "no pong") {
		t.Errorf("Stream error = %v, want a heartbeat failure", err)
	}
}

// TestStreamLeavesNoGoroutines guards the bug M6 goes looking for: one
// leaked reader per reconnect is invisible until the process has been up for
// hours.
func TestStreamLeavesNoGoroutines(t *testing.T) {
	// This server reads as well as writes, so its handler goroutine unwinds
	// when the client hangs up. A handler parked on the request context
	// would leak on its own — httptest does not cancel that context for a
	// hijacked connection — and would be indistinguishable from a leak in
	// Stream.
	// The server sends one frame and hangs up, simulating the exchange-side
	// disconnect that M6's supervisor will have to survive. It returns
	// promptly rather than parking on the request context, which httptest
	// never cancels for a hijacked connection — a handler blocked there
	// would leak on its own and be indistinguishable from a leak in Stream.
	url := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"stream":"s","data":{}}`))
	})

	v := New(testRegistry(t), fixedClock{time.Now()}, WithEndpoint(url))

	// The parent context stays alive across every connection, which is what
	// makes this test meaningful. A supervisor's context outlives the
	// individual connections it restarts, so any goroutine Stream parks on
	// that context rather than on its own derived one survives forever —
	// one leak per reconnect. Cancelling the parent between attempts would
	// tidy those up and hide exactly the bug being hunted.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// connectOnce dials and runs until the server hangs up, the way a
	// reconnect loop will thousands of times over a long session.
	connectOnce := func() {
		out := make(chan venue.RawMessage, 4)
		if err := v.Stream(ctx, []model.InstrumentID{"BTC-USDT"}, out); err == nil {
			t.Error("expected an error when the server closes the connection")
		}
	}

	settle := func() int {
		var n int
		for i := 0; i < 40; i++ {
			time.Sleep(25 * time.Millisecond)
			n = runtime.NumGoroutine()
		}
		return n
	}

	connectOnce() // warm up: first connection allocates pooled machinery
	before := settle()

	for i := 0; i < 20; i++ {
		connectOnce()
	}
	after := settle()

	if after > before+2 {
		t.Errorf("goroutine count grew from %d to %d across 20 connections", before, after)
	}
}
