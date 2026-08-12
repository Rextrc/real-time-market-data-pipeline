// Package broker is the order-execution boundary.
//
// This is where the system stops simulating and starts sending instructions
// to something outside itself. That makes it the one package where a bug has
// consequences beyond a wrong number in a report, so the interface is kept
// deliberately narrow: market orders only, one instrument at a time, no
// leverage, no margin, no order types that rest on the book.
package broker

import (
	"context"
	"errors"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Side is an order direction.
type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

// Order is a market order request.
//
// Exactly one of QuoteAmount and Quantity is set: buys are sized in quote
// currency ("spend 250 USDT") because that is how position sizing naturally
// works, and sells are sized in base currency ("sell 0.0036 BTC") because
// that is what you hold.
type Order struct {
	Instrument  model.InstrumentID
	Side        Side
	QuoteAmount model.Decimal // buys
	Quantity    model.Decimal // sells
	// ClientID makes submission idempotent across a retry or a reconnect.
	// Without it, a timeout that actually succeeded turns into a duplicate
	// position the next time the strategy fires.
	ClientID string
	Reason   string
}

// Fill is what an exchange reports back after execution.
type Fill struct {
	Instrument    model.InstrumentID `json:"instrument"`
	Side          Side               `json:"side"`
	Price         model.Decimal      `json:"price"` // volume-weighted across partial fills
	Quantity      model.Decimal      `json:"quantity"`
	QuoteSpent    model.Decimal      `json:"quote_spent"`
	Commission    model.Decimal      `json:"commission"`
	CommissionCcy string             `json:"commission_currency"`
	OrderID       string             `json:"order_id"`
	ClientID      string             `json:"client_order_id"`
	Time          time.Time          `json:"time"`
	Reason        string             `json:"reason"`
	// Partial reports whether the exchange filled less than requested.
	// A market order on a liquid pair rarely partials, but "rarely" is not
	// "never" and silently treating a partial as complete corrupts the
	// position accounting.
	Partial bool `json:"partial"`
}

// Balance is a holding at the exchange.
type Balance struct {
	Asset  string        `json:"asset"`
	Free   model.Decimal `json:"free"`
	Locked model.Decimal `json:"locked"`
}

// Errors a caller is expected to distinguish.
var (
	// ErrRejected means the exchange refused the order — bad size, below
	// the minimum notional, insufficient balance. Retrying unchanged will
	// fail again.
	ErrRejected = errors.New("broker: order rejected")
	// ErrBelowMinimum means the order was too small to submit. Common and
	// benign: it means the position size the strategy asked for is under
	// the exchange's floor.
	ErrBelowMinimum = errors.New("broker: order below exchange minimum")
	// ErrInsufficientFunds means the account cannot cover the order.
	ErrInsufficientFunds = errors.New("broker: insufficient balance")
)

// Broker executes orders somewhere real.
type Broker interface {
	// Name identifies the venue in logs and in the API.
	Name() string
	// Submit places a market order and blocks until the exchange reports
	// the outcome.
	Submit(ctx context.Context, o Order) (*Fill, error)
	// Balances returns current holdings.
	Balances(ctx context.Context) ([]Balance, error)
	// Ping verifies connectivity and credentials.
	Ping(ctx context.Context) error
}
