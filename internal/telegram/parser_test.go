package telegram

import (
	"testing"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
)

func TestParse_BuyWithBracket(t *testing.T) {
	in, err := Parse("buy 2 shares of AAPL with a take profit of 160 and stop loss of 140")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if in.Kind != KindOrder || in.Side != broker.Buy {
		t.Fatalf("got %+v", in)
	}
	if in.Symbol != "AAPL" {
		t.Fatalf("symbol = %q, want AAPL", in.Symbol)
	}
	if in.Qty.Float() != 2 {
		t.Fatalf("qty = %v, want 2", in.Qty.Float())
	}
	if in.TakeProfitPrice.Float() != 160 {
		t.Fatalf("take-profit = %v, want 160", in.TakeProfitPrice.Float())
	}
	if in.StopLossPrice.Float() != 140 {
		t.Fatalf("stop-loss = %v, want 140", in.StopLossPrice.Float())
	}
}

func TestParse_BuyCompanyNameNoOf(t *testing.T) {
	in, err := Parse("buy 3 tesla tp 5% sl 3%")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if in.Symbol != "TSLA" {
		t.Fatalf("symbol = %q, want TSLA", in.Symbol)
	}
	if in.Qty.Float() != 3 {
		t.Fatalf("qty = %v, want 3", in.Qty.Float())
	}
	if in.TakeProfitPct != 5 || in.StopLossPct != 3 {
		t.Fatalf("tp/sl pct = %v/%v, want 5/3", in.TakeProfitPct, in.StopLossPct)
	}
}

func TestParse_NotionalBuy(t *testing.T) {
	in, err := Parse("buy $500 of AAPL")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if in.Notional.Float() != 500 {
		t.Fatalf("notional = %v, want 500", in.Notional.Float())
	}
	if !in.Qty.IsZero() {
		t.Fatalf("expected zero qty for a notional buy, got %v", in.Qty.Float())
	}
}

func TestParse_Sell(t *testing.T) {
	in, err := Parse("sell 2 shares of AAPL")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if in.Kind != KindOrder || in.Side != broker.Sell || in.Symbol != "AAPL" {
		t.Fatalf("got %+v", in)
	}
}

func TestParse_SellNotionalRejected(t *testing.T) {
	if _, err := Parse("sell $500 of AAPL"); err == nil {
		t.Fatal("expected an error selling a dollar amount")
	}
}

func TestParse_Close(t *testing.T) {
	in, err := Parse("close AAPL")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if in.Kind != KindClose || in.Symbol != "AAPL" {
		t.Fatalf("got %+v", in)
	}
}

func TestParse_StatusPositionsHelp(t *testing.T) {
	for text, want := range map[string]Kind{
		"status":    KindStatus,
		"positions": KindPositions,
		"help":      KindHelp,
	} {
		in, err := Parse(text)
		if err != nil {
			t.Fatalf("Parse(%q): %v", text, err)
		}
		if in.Kind != want {
			t.Fatalf("Parse(%q).Kind = %v, want %v", text, in.Kind, want)
		}
	}
}

func TestParse_Quote(t *testing.T) {
	in, err := Parse("price aapl")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if in.Kind != KindQuote || in.Symbol != "AAPL" {
		t.Fatalf("got %+v", in)
	}
}

func TestParse_Gibberish(t *testing.T) {
	if _, err := Parse("what's up"); err == nil {
		t.Fatal("expected an error for an unrecognized message")
	}
}

func TestParse_MissingQuantity(t *testing.T) {
	if _, err := Parse("buy AAPL"); err == nil {
		t.Fatal("expected an error when no quantity is given")
	}
}

func TestParse_MissingSymbol(t *testing.T) {
	if _, err := Parse("buy 2 shares"); err == nil {
		t.Fatal("expected an error when no symbol is given")
	}
}
