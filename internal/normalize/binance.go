package normalize

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/instrument"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue"
)

// binanceEnvelope is the combined-stream wrapper: {"stream":..,"data":{..}}.
// Control replies ({"result":null,"id":1}) have neither field.
type binanceEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// binanceTrade is Binance's raw trade event.
//
// Prices and quantities arrive as strings and stay strings until
// model.ParseDecimal turns them into exact fixed-point values. They must
// never touch a float on the way.
type binanceTrade struct {
	EventType string      `json:"e"`
	EventTime int64       `json:"E"` // ms since epoch
	Symbol    string      `json:"s"`
	TradeID   json.Number `json:"t"`
	Price     string      `json:"p"`
	Quantity  string      `json:"q"`
	TradeTime int64       `json:"T"` // ms since epoch
	// BuyerIsMaker inverts to the aggressor's side. If the buyer was the
	// maker, the seller crossed the spread, so the trade is a sell.
	BuyerIsMaker bool `json:"m"`

	// Binance also sends "M" (a deprecated "ignore" flag) in every trade
	// frame. It must be declared even though it is unused: encoding/json
	// matches keys to tags case-insensitively when there is no exact match,
	// so without this field "M":true also binds to BuyerIsMaker and
	// silently inverts the side of every trade. Declaring both gives each
	// key an exact match, which takes priority. It has to be exported:
	// encoding/json ignores unexported fields outright, so an unexported
	// one would not absorb the key and the bug would persist.
	IgnoreFlag bool `json:"M"`
}

// BinanceDecoder decodes Binance combined-stream trade frames.
type BinanceDecoder struct {
	registry *instrument.Registry
}

func NewBinanceDecoder(reg *instrument.Registry) *BinanceDecoder {
	return &BinanceDecoder{registry: reg}
}

func (d *BinanceDecoder) Venue() model.VenueID { return model.VenueBinance }

func (d *BinanceDecoder) Decode(msg venue.RawMessage) ([]model.Tick, error) {
	var env binanceEnvelope
	if err := json.Unmarshal(msg.Payload, &env); err != nil {
		return nil, fmt.Errorf("binance decode: envelope: %w", err)
	}
	if len(env.Data) == 0 {
		return nil, nil // subscription ack or similar; not a tick, not a failure
	}

	var t binanceTrade
	if err := json.Unmarshal(env.Data, &t); err != nil {
		return nil, fmt.Errorf("binance decode: data: %w", err)
	}
	if t.EventType != "trade" {
		return nil, nil
	}

	if t.Symbol == "" {
		return nil, fmt.Errorf("binance decode: trade frame has no symbol")
	}
	id, ok := d.registry.Canonical(model.VenueBinance, t.Symbol)
	if !ok {
		return nil, fmt.Errorf("binance decode: unmapped symbol %q", t.Symbol)
	}

	price, err := model.ParseDecimal(t.Price)
	if err != nil {
		return nil, fmt.Errorf("binance decode: price %q: %w", t.Price, err)
	}
	qty, err := model.ParseDecimal(t.Quantity)
	if err != nil {
		return nil, fmt.Errorf("binance decode: quantity %q: %w", t.Quantity, err)
	}

	side := model.SideBuy
	if t.BuyerIsMaker {
		side = model.SideSell
	}

	// Prefer the trade timestamp over the event timestamp: T is when the
	// match happened, E is when the server emitted the notification.
	eventMillis := t.TradeTime
	if eventMillis == 0 {
		eventMillis = t.EventTime
	}

	return []model.Tick{{
		Venue:        model.VenueBinance,
		Instrument:   id,
		EventTime:    time.UnixMilli(eventMillis).UTC(),
		RecvTime:     msg.RecvTime.UTC(),
		Price:        price,
		Quantity:     qty,
		Side:         side,
		VenueTradeID: t.TradeID.String(),
	}}, nil
}
