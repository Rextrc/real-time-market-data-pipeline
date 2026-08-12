package binance

import (
	"github.com/Rextrc/real-time-market-data-pipeline/internal/instrument"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Symbols is the venue's contribution to the instrument registry. cmd/mdp
// registers it at startup; nothing imports this package to get at the table.
//
// The canonical IDs are BASE-QUOTE. Note that Binance's "USDT" is a
// genuinely different instrument from Coinbase's "USD" and must not be
// collapsed into one canonical ID, however tempting it looks in a chart.
func Symbols() []instrument.Mapping {
	pairs := map[model.InstrumentID]string{
		"BTC-USDT":  "BTCUSDT",
		"ETH-USDT":  "ETHUSDT",
		"SOL-USDT":  "SOLUSDT",
		"XRP-USDT":  "XRPUSDT",
		"ADA-USDT":  "ADAUSDT",
		"DOGE-USDT": "DOGEUSDT",
		"BNB-USDT":  "BNBUSDT",
		"LTC-USDT":  "LTCUSDT",
	}

	out := make([]instrument.Mapping, 0, len(pairs))
	for id, sym := range pairs {
		out = append(out, instrument.Mapping{Venue: ID, Instrument: id, Symbol: sym})
	}
	return out
}
