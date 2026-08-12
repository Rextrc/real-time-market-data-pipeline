package alpaca

// This file is the manual-order path: any Alpaca-tradable equity symbol, not
// just the crypto pairs the automated pipeline strategies trade, with
// optional bracket take-profit/stop-loss legs. It exists for
// internal/telegram, which lets a person type "buy 2 shares of AAPL with a
// take profit of 160 and stop loss of 140" and have it become a real order —
// see that package for the command grammar. Everything here still goes
// through the same Client, so the paper/live safety interlock in
// checkBaseURL applies unchanged.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	alpacasdk "github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	marketdata "github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// EquityOrder is a manually-issued order for a specific Alpaca symbol.
// Exactly one of Qty and Notional is set — the same convention as
// broker.Order, for the same reason: a buy is naturally sized in dollars,
// a sell in shares you already hold.
type EquityOrder struct {
	Symbol   string
	Side     broker.Side
	Qty      model.Decimal
	Notional model.Decimal
	// TakeProfit and StopLoss, if non-zero, attach a bracket order: Alpaca
	// manages both exit legs itself and cancels whichever one didn't fire —
	// this process does not need to stay running for the exit to happen.
	TakeProfit model.Decimal
	StopLoss   model.Decimal
	ClientID   string
	Reason     string
}

// PlaceEquityOrder submits a market order for any equity Alpaca lists,
// optionally as a bracket order when TakeProfit and/or StopLoss are set.
func (c *Client) PlaceEquityOrder(ctx context.Context, o EquityOrder) (*broker.Fill, error) {
	if o.Symbol == "" {
		return nil, errors.New("alpaca: order needs a symbol")
	}
	if o.Qty.IsZero() && o.Notional.IsZero() {
		return nil, errors.New("alpaca: order needs a quantity or a dollar amount")
	}
	if !o.Qty.IsZero() && !o.Notional.IsZero() {
		return nil, errors.New("alpaca: order cannot set both a quantity and a dollar amount")
	}
	// Alpaca rejects a notional bracket order outright — the exit legs need
	// a share count to attach a limit/stop price to. Catch it here with an
	// explanation instead of forwarding Alpaca's less obvious rejection.
	if !o.Notional.IsZero() && (!o.TakeProfit.IsZero() || !o.StopLoss.IsZero()) {
		return nil, errors.New("alpaca: a bracket order (take-profit/stop-loss) needs a share quantity, not a dollar amount")
	}

	symbol := strings.ToUpper(strings.TrimSpace(o.Symbol))

	req := alpacasdk.PlaceOrderRequest{
		Symbol:        symbol,
		Type:          alpacasdk.Market,
		TimeInForce:   alpacasdk.Day,
		ClientOrderID: o.ClientID,
	}
	switch o.Side {
	case broker.Buy:
		req.Side = alpacasdk.Buy
	case broker.Sell:
		req.Side = alpacasdk.Sell
	default:
		return nil, fmt.Errorf("alpaca: unsupported side %q", o.Side)
	}
	if !o.Qty.IsZero() {
		q := toSDKDecimal(o.Qty)
		req.Qty = &q
	} else {
		n := toSDKDecimal(o.Notional)
		req.Notional = &n
	}
	if !o.TakeProfit.IsZero() || !o.StopLoss.IsZero() {
		req.OrderClass = alpacasdk.Bracket
		if !o.TakeProfit.IsZero() {
			tp := toSDKDecimal(o.TakeProfit)
			req.TakeProfit = &alpacasdk.TakeProfit{LimitPrice: &tp}
		}
		if !o.StopLoss.IsZero() {
			sl := toSDKDecimal(o.StopLoss)
			req.StopLoss = &alpacasdk.StopLoss{StopPrice: &sl}
		}
	}

	placed, err := c.sdk.PlaceOrder(req)
	if err != nil {
		return nil, apiError(err)
	}

	// The entry leg of a bracket, or a plain market order outside market
	// hours, may not fill within the poll window awaitFill gives it — that
	// returns a descriptive "still open, check later" error rather than a
	// generic timeout, which is exactly what should reach the Telegram
	// reply in that case.
	final, err := c.awaitFill(ctx, placed.ID)
	if err != nil {
		return nil, err
	}

	// toFill only reads Instrument/Side/Reason off the broker.Order it's
	// given — reusing it here (with the equity symbol standing in for the
	// crypto InstrumentID) avoids duplicating the fill-conversion logic for
	// a broker.Order shape this path doesn't otherwise use.
	return c.toFill(broker.Order{
		Instrument: model.InstrumentID(symbol),
		Side:       o.Side,
		Reason:     o.Reason,
	}, final)
}

// PositionInfo is a richer view of one open equity position than
// Balances gives — Balances only reports free quantity, because that is all
// the automated strategies need.
type PositionInfo struct {
	Symbol         string
	Qty            model.Decimal
	AvgEntryPrice  model.Decimal
	CurrentPrice   model.Decimal
	MarketValue    model.Decimal
	UnrealizedPL   model.Decimal
	UnrealizedPLPc float64
}

// Positions lists every open equity position on the account.
func (c *Client) Positions(ctx context.Context) ([]PositionInfo, error) {
	positions, err := c.sdk.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("alpaca: get positions: %w", err)
	}
	out := make([]PositionInfo, 0, len(positions))
	for _, p := range positions {
		info := PositionInfo{
			Symbol:        p.Symbol,
			Qty:           fromSDKDecimal(p.Qty),
			AvgEntryPrice: fromSDKDecimal(p.AvgEntryPrice),
		}
		// current_price, market_value, and unrealized P&L come back null
		// when Alpaca has no fresh mark for the symbol yet (e.g. right after
		// the market opens) — zero is a reasonable "unknown" here, this is a
		// status report, not money being moved.
		if p.CurrentPrice != nil {
			info.CurrentPrice = fromSDKDecimal(*p.CurrentPrice)
		}
		if p.MarketValue != nil {
			info.MarketValue = fromSDKDecimal(*p.MarketValue)
		}
		if p.UnrealizedPL != nil {
			info.UnrealizedPL = fromSDKDecimal(*p.UnrealizedPL)
		}
		if p.UnrealizedPLPC != nil {
			pct, _ := p.UnrealizedPLPC.Float64()
			info.UnrealizedPLPc = pct * 100
		}
		out = append(out, info)
	}
	return out, nil
}

// AccountSummary is the numbers a "status"/"balance" reply needs.
type AccountSummary struct {
	Cash           model.Decimal
	PortfolioValue model.Decimal
	BuyingPower    model.Decimal
}

func (c *Client) AccountSummary(ctx context.Context) (*AccountSummary, error) {
	acct, err := c.sdk.GetAccount()
	if err != nil {
		return nil, fmt.Errorf("alpaca: get account: %w", err)
	}
	return &AccountSummary{
		Cash:           fromSDKDecimal(acct.Cash),
		PortfolioValue: fromSDKDecimal(acct.PortfolioValue),
		BuyingPower:    fromSDKDecimal(acct.BuyingPower),
	}, nil
}

// Quote returns the latest trade price for a symbol. Market data is served
// from a separate Alpaca host (data.alpaca.markets) that isn't gated by the
// paper/live base URL interlock — it's read-only and the same feed serves
// both account types.
func (c *Client) Quote(ctx context.Context, symbol string) (model.Decimal, error) {
	if c.data == nil {
		return model.Decimal{}, errors.New("alpaca: market data client not configured")
	}
	trade, err := c.data.GetLatestTrade(strings.ToUpper(strings.TrimSpace(symbol)), marketdata.GetLatestTradeRequest{})
	if err != nil {
		return model.Decimal{}, fmt.Errorf("alpaca: quote %s: %w", symbol, err)
	}
	return model.FromFloat(trade.Price, 4), nil
}
