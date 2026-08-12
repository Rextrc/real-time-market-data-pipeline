package alpaca

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// fakeAlpaca stands in for Alpaca's REST API over the exact wire shape the
// real SDK expects, so Client's request construction, response parsing, and
// polling logic all run against real HTTP — the only thing that differs
// from a live run is which server answers. This is what lets the safety
// interlock and the fill logic be verified in an environment that cannot
// reach Alpaca's actual hosts.
type fakeAlpaca struct {
	*httptest.Server
	cash      string
	positions map[string]string // symbol -> qty
	orderSeq  int
}

func newFakeAlpaca(t *testing.T) *fakeAlpaca {
	t.Helper()
	f := &fakeAlpaca{cash: "10000", positions: map[string]string{}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/account", f.handleAccount)
	mux.HandleFunc("GET /v2/positions", f.handlePositions)
	mux.HandleFunc("POST /v2/orders", f.handlePlaceOrder)
	mux.HandleFunc("GET /v2/orders/{id}", f.handleGetOrder)

	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

func (f *fakeAlpaca) handleAccount(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"id": "test-account", "status": "ACTIVE", "crypto_status": "ACTIVE",
		"cash": f.cash, "equity": f.cash, "last_equity": f.cash,
	})
}

func (f *fakeAlpaca) handlePositions(w http.ResponseWriter, r *http.Request) {
	out := []map[string]any{}
	for symbol, qty := range f.positions {
		out = append(out, map[string]any{
			"asset_id": symbol, "symbol": symbol, "exchange": "CRYPTO",
			"asset_class": "crypto", "qty": qty, "qty_available": qty,
			"avg_entry_price": "100", "side": "long", "cost_basis": "100",
		})
	}
	writeJSON(w, out)
}

func (f *fakeAlpaca) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Symbol        string `json:"symbol"`
		Side          string `json:"side"`
		Notional      string `json:"notional"`
		Qty           string `json:"qty"`
		TimeInForce   string `json:"time_in_force"`
		ClientOrderID string `json:"client_order_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// A market order fills instantly in this fake, at a fixed price — real
	// Alpaca is asynchronous, which is exactly why Client polls GetOrder
	// rather than trusting this response; that polling path is exercised
	// here too, since GetOrder is what actually reports "filled".
	f.orderSeq++
	id := "order-" + itoa(f.orderSeq)

	const price = "100.00"
	var filledQty string
	switch req.Side {
	case "buy":
		filledQty = divide(req.Notional, price)
		f.positions[req.Symbol] = add(f.positions[req.Symbol], filledQty)
		f.cash = sub(f.cash, req.Notional)
	case "sell":
		filledQty = req.Qty
		f.cash = add(f.cash, multiply(req.Qty, price))
		delete(f.positions, req.Symbol)
	}

	body := map[string]any{
		"id": id, "client_order_id": req.ClientOrderID, "symbol": req.Symbol,
		"asset_class": "crypto", "side": req.Side, "type": "market",
		"time_in_force": req.TimeInForce, "status": "accepted",
		"filled_qty":   "0",
		"submitted_at": "2026-01-01T00:00:00Z", "created_at": "2026-01-01T00:00:00Z",
	}
	// A real Alpaca response omits qty/notional entirely (null) rather than
	// sending an empty string for whichever sizing field the order didn't
	// use — the SDK's decimal type accepts null but not "". Mirror that so
	// this fixture actually validates the client's decoding.
	if req.Qty != "" {
		body["qty"] = req.Qty
	}
	if req.Notional != "" {
		body["notional"] = req.Notional
	}
	writeJSON(w, body)
	// Stash the terminal state for GetOrder to report a moment later.
	f.terminal(id, req.Symbol, req.Side, filledQty, price)
}

// pending simulates "accepted but not yet filled" for exactly one GetOrder
// poll, then "filled" thereafter — this is what forces Client.awaitFill's
// polling loop to actually loop rather than succeed on the first check.
var pending = map[string]int{}

func (f *fakeAlpaca) terminal(id, symbol, side, qty, price string) {
	pending[id] = 1
	body := map[string]any{
		"id": id, "symbol": symbol, "side": side, "status": "filled",
		"filled_qty":   qty,
		"submitted_at": "2026-01-01T00:00:00Z",
		"filled_at":    "2026-01-01T00:00:01Z",
	}
	if qty != "" {
		body["qty"] = qty
	}
	if price != "" {
		body["filled_avg_price"] = price
	}
	terminalOrders[id] = body
}

var terminalOrders = map[string]map[string]any{}

func (f *fakeAlpaca) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if pending[id] > 0 {
		pending[id]--
		writeJSON(w, map[string]any{
			"id": id, "status": "accepted", "filled_qty": "0",
			"submitted_at": "2026-01-01T00:00:00Z",
		})
		return
	}
	if order, ok := terminalOrders[id]; ok {
		writeJSON(w, order)
		return
	}
	http.NotFound(w, r)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// Minimal decimal string arithmetic so this file has no dependency beyond
// the standard library — precision is not the point of a test fixture.
func add(a, b string) string      { return decOp(a, b, func(x, y float64) float64 { return x + y }) }
func sub(a, b string) string      { return decOp(a, b, func(x, y float64) float64 { return x - y }) }
func multiply(a, b string) string { return decOp(a, b, func(x, y float64) float64 { return x * y }) }
func divide(a, b string) string   { return decOp(a, b, func(x, y float64) float64 { return x / y }) }

func decOp(a, b string, op func(x, y float64) float64) string {
	x, _ := model.ParseDecimal(orZero(a))
	y, _ := model.ParseDecimal(orZero(b))
	return model.FromFloat(op(x.Float(), y.Float()), 8).String()
}

func orZero(s string) string {
	if s == "" {
		return "0"
	}
	return s
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
