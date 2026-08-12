// Command fakevenue serves a Binance-shaped trade stream locally.
//
// Two uses. First, developing without the network or a Binance-reachable
// region. Second, and more importantly, it is a load generator with a knob:
// point it at the pipeline at 50,000 ticks/sec and the backpressure policies
// stop being theoretical. Waiting for a volatile market to produce that
// pressure is not a test, it is a hope.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

func main() {
	addr := flag.String("addr", ":9443", "listen address")
	rate := flag.Int("rate", 50, "trades per second per symbol")
	drop := flag.Duration("drop-after", 0, "sever every connection after this long; 0 never")
	stall := flag.Duration("stall-after", 0, "stop sending but hold the socket open after this long (tests heartbeats)")
	flag.Parse()

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		symbols := parseStreams(r.URL.RawQuery)
		if len(symbols) == 0 {
			http.Error(w, "no streams requested", http.StatusBadRequest)
			return
		}

		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		log.Printf("client connected: %v at %d/s per symbol", symbols, *rate)
		serve(r, conn, symbols, *rate, *drop, *stall)
		log.Printf("client disconnected")
	})

	log.Printf("fake venue listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func serve(r *http.Request, conn *websocket.Conn, symbols []string, rate int, drop, stall time.Duration) {
	ctx := r.Context()

	// Drain reads so pings are answered and a client close is noticed.
	//
	// readCtx is separate from ctx so that -stall-after can stop the reader
	// too. That distinction is the whole point of the flag: a server that
	// goes quiet but still answers pings is a healthy idle connection and
	// must NOT be dropped, whereas one that answers nothing is dead and
	// must be. Only the second is a black hole.
	readCtx, stopReading := context.WithCancel(ctx)
	defer stopReading()
	go func() {
		for {
			if _, _, err := conn.Read(readCtx); err != nil {
				return
			}
		}
	}()

	interval := time.Second / time.Duration(max(rate, 1))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	started := time.Now()
	prices := make(map[string]float64, len(symbols))
	for _, s := range symbols {
		prices[s] = seedPrice(s)
	}

	var tradeID int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		elapsed := time.Since(started)
		if drop > 0 && elapsed > drop {
			log.Printf("severing connection (drop-after %s)", drop)
			return
		}
		if stall > 0 && elapsed > stall {
			// Hold the socket open, send nothing, and stop answering pings.
			// A pipeline without an application-level heartbeat will sit
			// here forever looking perfectly healthy.
			stopReading()
			continue
		}

		for _, sym := range symbols {
			tradeID++

			// Random walk with a slight mean reversion, so prices stay in a
			// plausible band over a long run instead of drifting to zero.
			p := prices[sym]
			p *= 1 + (rand.Float64()-0.5)*0.0008
			p += (seedPrice(sym) - p) * 0.0001
			prices[sym] = p

			maker := "false"
			if rand.Intn(2) == 0 {
				maker = "true"
			}
			now := time.Now().UnixMilli()

			frame := fmt.Sprintf(
				`{"stream":"%s@trade","data":{"e":"trade","E":%d,"s":"%s","t":%d,`+
					`"p":"%.8f","q":"%.8f","T":%d,"m":%s,"M":true}}`,
				strings.ToLower(sym), now, strings.ToUpper(sym), tradeID,
				p, 0.0001+rand.Float64()*2, now, maker)

			if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
				return
			}
		}
	}
}

func parseStreams(query string) []string {
	const prefix = "streams="
	i := strings.Index(query, prefix)
	if i < 0 {
		return nil
	}

	var out []string
	for _, part := range strings.Split(query[i+len(prefix):], "/") {
		if s, _, ok := strings.Cut(part, "@"); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

// seedPrice gives each symbol a plausible starting level so the output looks
// like a market rather than a random number generator.
func seedPrice(symbol string) float64 {
	switch strings.ToUpper(symbol) {
	case "BTCUSDT":
		return 68000
	case "ETHUSDT":
		return 3500
	case "SOLUSDT":
		return 170
	case "BNBUSDT":
		return 600
	case "LTCUSDT":
		return 85
	case "XRPUSDT":
		return 0.55
	case "ADAUSDT":
		return 0.45
	case "DOGEUSDT":
		return 0.16
	default:
		return 100
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
