// Package alpaca executes orders against Alpaca's paper trading API.
//
// Alpaca runs one account against simulated fills for paper trading and a
// separate account against real fills for live trading, distinguished only
// by which base URL a request goes to. Fills, balances, and positions are
// Alpaca's own, not locally simulated — this is a step up in realism from
// the in-process paper engine (internal/consumer/paper), not a replacement
// for it: real order acceptance/rejection, real timing, a real account
// state machine, and (for crypto) continuous execution with no market hours
// to reason about.
//
// # Safety
//
// The underlying Alpaca SDK defaults to the **live** base URL when none is
// configured — see alpacahq/alpaca-trade-api-go's rest.go, which falls back
// to https://api.alpaca.markets unless APCA_API_BASE_URL happens to be set.
// That default is backwards for this package's purpose, so it is inverted
// here: New defaults to paper and requires an explicit, named opt-in to
// reach anything else. There is no flag or environment variable that flips
// it by accident — see AllowLive.
package alpaca

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	alpacasdk "github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	marketdata "github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/shopspring/decimal"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// PaperBaseURL is Alpaca's simulated-fill endpoint. This is the default and,
// unless AllowLive is set, the only endpoint this package will use.
const PaperBaseURL = "https://paper-api.alpaca.markets"

// LiveBaseURL is Alpaca's real-money endpoint. Reaching it requires setting
// Config.AllowLive to true in addition to supplying it as BaseURL — a single
// mistyped or copy-pasted environment variable cannot enable it alone.
const LiveBaseURL = "https://api.alpaca.markets"

// Config configures the client.
type Config struct {
	APIKey    string
	APISecret string
	// BaseURL selects the endpoint. Empty means PaperBaseURL. Any other
	// value is rejected unless AllowLive is also true — see AllowLive.
	BaseURL string
	// AllowLive must be explicitly true for BaseURL to be permitted to be
	// anything other than the paper endpoint. Default false. This exists so
	// that going live is a decision made in code, reviewed like any other
	// change, rather than a side effect of an environment variable.
	AllowLive bool
	Timeout   time.Duration
}

// ErrLiveNotAllowed is returned when Config names a non-paper endpoint
// without explicitly setting AllowLive.
var ErrLiveNotAllowed = errors.New(
	"alpaca: a non-paper base URL was given but AllowLive is not set; " +
		"this package defaults to Alpaca's paper endpoint and requires an explicit opt-in to reach anything else")

// Client is an Alpaca paper (or, if explicitly allowed, live) broker.
type Client struct {
	sdk      *alpacasdk.Client
	data     *marketdata.Client
	baseURL  string
	registry symbolMapper
	log      *slog.Logger
}

// symbolMapper is the subset of instrument.Registry this package needs, kept
// as an interface so tests don't need the real registry.
type symbolMapper interface {
	Symbol(venue model.VenueID, id model.InstrumentID) (string, bool)
}

func New(cfg Config, reg symbolMapper, log *slog.Logger) (*Client, error) {
	base := cfg.BaseURL
	if base == "" {
		base = PaperBaseURL
	}
	if err := checkBaseURL(base, cfg.AllowLive); err != nil {
		return nil, err
	}
	if cfg.APIKey == "" || cfg.APISecret == "" {
		return nil, errors.New("alpaca: API key and secret are required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}

	sdk := alpacasdk.NewClient(alpacasdk.ClientOpts{
		APIKey:    cfg.APIKey,
		APISecret: cfg.APISecret,
		BaseURL:   base,
	})
	// Market data lives on its own host and isn't part of the paper/live
	// split above — the same feed serves both account types, keyed only by
	// the API key's own entitlement. Quotes are read-only, so there's
	// nothing for checkBaseURL to guard here.
	data := marketdata.NewClient(marketdata.ClientOpts{
		APIKey:    cfg.APIKey,
		APISecret: cfg.APISecret,
	})

	c := &Client{sdk: sdk, data: data, baseURL: base, registry: reg, log: log}
	if u, err := url.Parse(base); err == nil && base != PaperBaseURL && !isLoopback(u.Hostname()) {
		log.Warn("alpaca client configured against a non-paper endpoint",
			"base_url", base, "note", "orders placed through this client will use real funds")
	}
	return c, nil
}

// checkBaseURL is the safety interlock. Only PaperBaseURL is permitted
// without AllowLive; anything else — including a typo of the paper URL that
// happens to resolve, or the live URL itself — is refused.
func checkBaseURL(base string, allowLive bool) error {
	u, err := url.Parse(base)
	if err != nil {
		return fmt.Errorf("alpaca: bad base URL %q: %w", base, err)
	}
	host := strings.ToLower(u.Hostname())

	if host == "paper-api.alpaca.markets" {
		return nil
	}
	// Loopback is permitted unconditionally so the test suite can exercise
	// the real request path against a local fake server. It grants nothing
	// in production: nobody can make Alpaca's hostname resolve to loopback,
	// so this is not a route past the interlock for a real deployment.
	if isLoopback(host) {
		return nil
	}
	if !allowLive {
		return ErrLiveNotAllowed
	}
	// Even with AllowLive, only Alpaca's own live host is accepted — the
	// flag grants "trade for real on Alpaca," not "trust any URL."
	if host != "api.alpaca.markets" {
		return fmt.Errorf("alpaca: unrecognized base URL host %q", host)
	}
	return nil
}

func isLoopback(host string) bool {
	switch host {
	case "127.0.0.1", "localhost", "::1", "[::1]":
		return true
	}
	return false
}

func (c *Client) Name() string {
	if c.baseURL == PaperBaseURL {
		return "alpaca-paper"
	}
	if u, err := url.Parse(c.baseURL); err == nil && isLoopback(u.Hostname()) {
		return "alpaca-test"
	}
	return "alpaca-live"
}

// Ping verifies credentials and connectivity by fetching the account.
func (c *Client) Ping(ctx context.Context) error {
	acct, err := c.sdk.GetAccount()
	if err != nil {
		return fmt.Errorf("alpaca: account check failed: %w", err)
	}
	if acct.Status != "ACTIVE" {
		c.log.Warn("alpaca account is not ACTIVE", "status", acct.Status)
	}
	return nil
}

// Balances reports cash plus the market value of every open position, so the
// engine sees the same "what do I hold" view Alpaca itself would show.
func (c *Client) Balances(ctx context.Context) ([]broker.Balance, error) {
	acct, err := c.sdk.GetAccount()
	if err != nil {
		return nil, fmt.Errorf("alpaca: get account: %w", err)
	}
	balances := []broker.Balance{
		{Asset: "USD", Free: fromSDKDecimal(acct.Cash)},
	}

	positions, err := c.sdk.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("alpaca: get positions: %w", err)
	}
	for _, p := range positions {
		balances = append(balances, broker.Balance{
			// broker.Balance.Asset is the base asset alone (e.g. "BTC"),
			// matching how model.InstrumentID splits as BASE-QUOTE — not
			// Alpaca's own "BTC/USD" spelling. Reporting the raw symbol
			// here would silently break every caller that matches a
			// balance back to an instrument, including this package's own
			// sell path a few lines down.
			Asset: baseAssetOf(p.Symbol),
			Free:  fromSDKDecimal(p.Qty),
		})
	}
	return balances, nil
}

// baseAssetOf strips Alpaca's quote-asset suffix, accepting both its
// slash-delimited crypto spelling ("BTC/USD") and a bare symbol with no
// separator, which some Alpaca responses use.
func baseAssetOf(symbol string) string {
	if base, _, ok := strings.Cut(symbol, "/"); ok {
		return base
	}
	return symbol
}

// Submit places a market order and polls until Alpaca reports it filled.
//
// Alpaca's PlaceOrder call returns as soon as the order is *accepted*, not
// once it is filled — for crypto, fills are typically near-instant but are
// not guaranteed synchronous with the HTTP response. Treating "accepted" as
// "filled" would report a position and a price the account doesn't actually
// have yet, so this polls GetOrder until a terminal status.
func (c *Client) Submit(ctx context.Context, o broker.Order) (*broker.Fill, error) {
	symbol, ok := c.registry.Symbol(model.VenueBinance, o.Instrument)
	if !ok {
		return nil, fmt.Errorf("alpaca: no venue symbol for %s", o.Instrument)
	}
	// Alpaca's crypto symbols are BASE/USD, e.g. BTC/USD — not Binance's
	// BTCUSDT. Translate at this boundary so nothing upstream needs to know
	// Alpaca's spelling.
	alpacaSymbol, err := toAlpacaCryptoSymbol(symbol)
	if err != nil {
		return nil, err
	}

	req := alpacasdk.PlaceOrderRequest{
		Symbol: alpacaSymbol,
		Type:   alpacasdk.Market,
		// Crypto does not support "day" (there is no close to expire
		// against); GTC is Alpaca's documented choice for crypto market
		// orders.
		TimeInForce:   alpacasdk.GTC,
		ClientOrderID: o.ClientID,
	}

	switch o.Side {
	case broker.Buy:
		req.Side = alpacasdk.Buy
		amt := toSDKDecimal(o.QuoteAmount)
		req.Notional = &amt
	case broker.Sell:
		req.Side = alpacasdk.Sell
		qty := toSDKDecimal(o.Quantity)
		req.Qty = &qty
	default:
		return nil, fmt.Errorf("alpaca: unsupported side %q", o.Side)
	}

	placed, err := c.sdk.PlaceOrder(req)
	if err != nil {
		return nil, apiError(err)
	}

	final, err := c.awaitFill(ctx, placed.ID)
	if err != nil {
		return nil, err
	}
	return c.toFill(o, final)
}

// terminal order statuses. Anything not in this set means "still working" —
// keep polling. See Alpaca's order lifecycle documentation for the full set;
// these are the ones a market order can actually reach.
var terminalStatuses = map[string]bool{
	"filled":           true,
	"partially_filled": false, // can still progress to filled or canceled
	"canceled":         true,
	"expired":          true,
	"rejected":         true,
	"done_for_day":     true,
}

func (c *Client) awaitFill(ctx context.Context, orderID string) (*alpacasdk.Order, error) {
	const pollInterval = 250 * time.Millisecond

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		o, err := c.sdk.GetOrder(orderID)
		if err != nil {
			return nil, fmt.Errorf("alpaca: poll order %s: %w", orderID, err)
		}
		if terminalStatuses[o.Status] {
			return o, nil
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			// The order is still open at Alpaca even though we're giving up
			// on waiting for it here — say so rather than returning a
			// generic timeout, since the caller needs to know the order may
			// still fill or may need to be checked later, not retried.
			return nil, fmt.Errorf(
				"alpaca: order %s did not reach a terminal status within the poll window "+
					"(last status %q); it may still be open at the exchange — check before retrying: %w",
				orderID, o.Status, ctx.Err())
		}
	}
}

func (c *Client) toFill(o broker.Order, r *alpacasdk.Order) (*broker.Fill, error) {
	if r.Status == "rejected" || r.Status == "expired" || r.Status == "canceled" {
		return nil, fmt.Errorf("%w: status %s", broker.ErrRejected, r.Status)
	}
	if r.FilledQty.IsZero() {
		return nil, fmt.Errorf("%w: nothing filled (status %s)", broker.ErrRejected, r.Status)
	}

	filled := fromSDKDecimal(r.FilledQty)
	var price model.Decimal
	if r.FilledAvgPrice != nil {
		price = fromSDKDecimal(*r.FilledAvgPrice)
	}
	quoteSpent := price.Mul(filled).Rescale(8)

	var origQty decimal.Decimal
	if r.Qty != nil {
		origQty = *r.Qty
	}
	partial := r.Status == "partially_filled" ||
		(!origQty.IsZero() && r.FilledQty.LessThan(origQty))

	filledAt := r.SubmittedAt
	if r.FilledAt != nil {
		filledAt = *r.FilledAt
	}

	return &broker.Fill{
		Instrument: o.Instrument,
		Side:       o.Side,
		Price:      price,
		Quantity:   filled,
		QuoteSpent: quoteSpent,
		// Alpaca crypto orders as of this integration are commission-free;
		// left zero rather than guessed. If that changes, the fee needs to
		// come from Alpaca's response, not be assumed here.
		Commission:    model.Decimal{},
		CommissionCcy: "USD",
		OrderID:       r.ID,
		ClientID:      r.ClientOrderID,
		Time:          filledAt.UTC(),
		Reason:        o.Reason,
		Partial:       partial,
	}, nil
}

// toAlpacaCryptoSymbol converts a Binance-style symbol (BTCUSDT) to Alpaca's
// crypto spelling (BTC/USD). Only the USDT and USD quote assets used
// elsewhere in this pipeline are handled; anything else is an error rather
// than a guess, because a wrong symbol here submits an order for the wrong
// instrument.
func toAlpacaCryptoSymbol(binanceSymbol string) (string, error) {
	s := strings.ToUpper(binanceSymbol)
	for _, quote := range []string{"USDT", "USD"} {
		if strings.HasSuffix(s, quote) && len(s) > len(quote) {
			base := strings.TrimSuffix(s, quote)
			// Alpaca crypto is USD-quoted; a Binance USDT pair maps to the
			// same USD pair on Alpaca (e.g. BTCUSDT -> BTC/USD).
			return base + "/USD", nil
		}
	}
	return "", fmt.Errorf("alpaca: cannot derive a crypto symbol from %q", binanceSymbol)
}

func toSDKDecimal(d model.Decimal) decimal.Decimal {
	return decimal.New(d.Unscaled, -int32(d.Scale))
}

// fromSDKDecimal converts shopspring's arbitrary-precision decimal to this
// system's fixed-point Decimal by round-tripping through its exact decimal
// string. shopspring's internal representation (arbitrary-precision
// coefficient plus exponent, which can be positive) doesn't map onto our
// int64-mantissa/uint8-scale layout by simple field copies, and Alpaca's
// prices are always well within the range ParseDecimal handles — going
// through the string form reuses that logic instead of duplicating it.
func fromSDKDecimal(d decimal.Decimal) model.Decimal {
	parsed, err := model.ParseDecimal(d.String())
	if err != nil {
		// Alpaca's own decimals are always well-formed; a failure here means
		// a value this system genuinely cannot represent (absurdly many
		// fractional digits). Zero is a safer failure than a panic in the
		// broker's response path, and callers already treat a zero fill
		// price as a sign to look closer.
		return model.Decimal{}
	}
	return parsed
}

var _ broker.Broker = (*Client)(nil)

func apiError(err error) error {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "insufficient"):
		return fmt.Errorf("%w: %v", broker.ErrInsufficientFunds, err)
	case strings.Contains(msg, "notional") || strings.Contains(msg, "qty") || strings.Contains(msg, "minimum"):
		return fmt.Errorf("%w: %v", broker.ErrBelowMinimum, err)
	default:
		return fmt.Errorf("%w: %v", broker.ErrRejected, err)
	}
}
