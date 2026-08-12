package telegram

// Command parsing lives in its own file, separate from execution (bot.go),
// so the grammar can be tested without a live Alpaca connection.
//
// The grammar is deliberately small and regex-based rather than handed to an
// LLM: these messages place real orders, and a rule that either parses
// cleanly or says exactly what it didn't understand is worth more here than
// one that guesses at intent. Supported shapes:
//
//	buy 2 shares of AAPL
//	buy 2 shares of AAPL with a take profit of 160 and stop loss of 140
//	buy 2 AAPL tp 5% sl 3%
//	buy $500 of tesla
//	sell 2 shares of AAPL
//	close AAPL
//	positions / status / price AAPL / help

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Kind is what the message asked for.
type Kind string

const (
	KindOrder     Kind = "order"
	KindClose     Kind = "close" // sell everything held in one symbol
	KindPositions Kind = "positions"
	KindStatus    Kind = "status"
	KindQuote     Kind = "quote"
	KindHelp      Kind = "help"
)

// Intent is one parsed instruction, ready to execute.
type Intent struct {
	Kind   Kind
	Side   broker.Side // KindOrder only
	Symbol string      // KindOrder, KindClose, KindQuote

	// Exactly one of Qty/Notional is set for a buy; sells and closes always
	// use Qty (you sell what you hold, not a dollar amount of it).
	Qty      model.Decimal
	Notional model.Decimal

	// At most one of each pair is non-zero. A Pct is resolved against the
	// live quote at execution time, since the parser has no market access.
	TakeProfitPrice model.Decimal
	TakeProfitPct   float64
	StopLossPrice   model.Decimal
	StopLossPct     float64
}

var (
	buyRe   = regexp.MustCompile(`(?i)^\s*buy\b`)
	sellRe  = regexp.MustCompile(`(?i)^\s*sell\b`)
	closeRe = regexp.MustCompile(`(?i)^\s*close(?:\s+position)?\s+([A-Za-z][A-Za-z.]{0,9})\b`)
	priceRe = regexp.MustCompile(`(?i)^\s*(?:price|quote)\s+([A-Za-z][A-Za-z.]{0,9})\b`)

	tpRe = regexp.MustCompile(`(?i)(?:take[- ]?profit(?:\s+(?:of|at))?|\btp\b)\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(%)?`)
	slRe = regexp.MustCompile(`(?i)(?:stop[- ]?loss(?:\s+(?:of|at))?|\bsl\b)\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(%)?`)

	notionalRe = regexp.MustCompile(`(?i)\$\s*([\d,]+(?:\.\d+)?)`)
	shareQtyRe = regexp.MustCompile(`(?i)([\d,]+(?:\.\d+)?)\s*shares?\b`)
	bareQtyRe  = regexp.MustCompile(`(?i)^(?:buy|sell)\s+([\d,]+(?:\.\d+)?)\b`)

	// Matches the symbol whether or not "shares"/"of" are present:
	// "buy 2 shares of AAPL", "buy 2 AAPL", "buy $500 of AAPL" all capture
	// AAPL. Anchored to the start since it walks through the sentence
	// structure from "buy"/"sell" onward.
	// The \b after (?:shares?) matters: without it, the engine can satisfy
	// the pattern by consuming only "share" from "shares" and handing the
	// leftover "s" to the symbol capture group — a valid but wrong parse of
	// "buy 2 shares" with no symbol. Forcing a word boundary means the
	// optional group only matches a whole "share"/"shares" token, never a
	// fragment of one.
	symbolRe = regexp.MustCompile(`(?i)^(?:buy|sell)\s+\$?[\d,.]+\s*(?:shares?\b)?\s*(?:of\s+)?([A-Za-z][A-Za-z.]{0,9})\b`)
)

// companyAliases resolves a handful of common household names to their
// ticker. It only ever matches a single word, so "coca cola" won't resolve —
// the fallback is always to type the ticker itself.
var companyAliases = map[string]string{
	"apple": "AAPL", "tesla": "TSLA", "google": "GOOGL", "alphabet": "GOOGL",
	"amazon": "AMZN", "microsoft": "MSFT", "nvidia": "NVDA", "meta": "META",
	"facebook": "META", "netflix": "NFLX", "disney": "DIS", "boeing": "BA",
	"nike": "NKE", "starbucks": "SBUX", "walmart": "WMT", "target": "TGT",
	"intel": "INTC", "amd": "AMD", "ibm": "IBM", "oracle": "ORCL",
	"salesforce": "CRM", "uber": "UBER", "airbnb": "ABNB", "paypal": "PYPL",
	"visa": "V", "mastercard": "MA", "coinbase": "COIN", "robinhood": "HOOD",
	"gamestop": "GME", "amc": "AMC", "ford": "F", "gm": "GM",
	"pfizer": "PFE", "moderna": "MRNA", "berkshire": "BRK.B",
	"jpmorgan": "JPM", "goldman": "GS", "spy": "SPY", "qqq": "QQQ",
}

// symbolStopwords catches symbolRe capturing one of its own optional
// keywords instead of an actual symbol (see the comment where it's used).
var symbolStopwords = map[string]bool{
	"shares": true, "share": true, "of": true, "with": true, "and": true,
}

func resolveSymbol(tok string) string {
	key := strings.ToLower(strings.TrimSpace(tok))
	if t, ok := companyAliases[key]; ok {
		return t
	}
	return strings.ToUpper(strings.TrimSpace(tok))
}

func parseNum(s string) (float64, error) {
	return strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
}

// Parse turns one message into an Intent, or an error describing exactly
// what was ambiguous or missing — that error is sent back to the chat
// verbatim, so it needs to be something a person can act on.
func Parse(raw string) (Intent, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return Intent{}, fmt.Errorf("empty message")
	}
	lower := strings.ToLower(text)

	switch lower {
	case "/start", "help", "/help":
		return Intent{Kind: KindHelp}, nil
	case "positions", "/positions", "position":
		return Intent{Kind: KindPositions}, nil
	case "status", "/status", "balance", "account":
		return Intent{Kind: KindStatus}, nil
	}

	if m := priceRe.FindStringSubmatch(text); m != nil {
		return Intent{Kind: KindQuote, Symbol: resolveSymbol(m[1])}, nil
	}
	if m := closeRe.FindStringSubmatch(text); m != nil {
		return Intent{Kind: KindClose, Symbol: resolveSymbol(m[1])}, nil
	}

	var side broker.Side
	switch {
	case buyRe.MatchString(text):
		side = broker.Buy
	case sellRe.MatchString(text):
		side = broker.Sell
	default:
		return Intent{}, fmt.Errorf(
			`didn't understand %q — try "buy 2 shares of AAPL with a take profit of 160 and stop loss of 140", or send "help"`, text)
	}

	// Strip take-profit/stop-loss clauses before looking for the quantity
	// and symbol: both clauses commonly contain their own "of <number>",
	// which would otherwise be mistaken for "of <symbol>".
	work := text
	var tpPrice, slPrice model.Decimal
	var tpPct, slPct float64
	if m := tpRe.FindStringSubmatch(work); m != nil {
		v, err := parseNum(m[1])
		if err != nil {
			return Intent{}, fmt.Errorf("couldn't read the take-profit number in %q", m[0])
		}
		if m[2] == "%" {
			tpPct = v
		} else {
			tpPrice = model.FromFloat(v, 4)
		}
		work = tpRe.ReplaceAllString(work, " ")
	}
	if m := slRe.FindStringSubmatch(work); m != nil {
		v, err := parseNum(m[1])
		if err != nil {
			return Intent{}, fmt.Errorf("couldn't read the stop-loss number in %q", m[0])
		}
		if m[2] == "%" {
			slPct = v
		} else {
			slPrice = model.FromFloat(v, 4)
		}
		work = slRe.ReplaceAllString(work, " ")
	}

	var qty, notional model.Decimal
	switch {
	case notionalRe.MatchString(work):
		v, err := parseNum(notionalRe.FindStringSubmatch(work)[1])
		if err != nil {
			return Intent{}, fmt.Errorf("couldn't read the dollar amount in %q", text)
		}
		notional = model.FromFloat(v, 2)
	case shareQtyRe.MatchString(work):
		v, err := parseNum(shareQtyRe.FindStringSubmatch(work)[1])
		if err != nil {
			return Intent{}, fmt.Errorf("couldn't read the share quantity in %q", text)
		}
		qty = model.FromFloat(v, 6)
	case bareQtyRe.MatchString(work):
		v, err := parseNum(bareQtyRe.FindStringSubmatch(work)[1])
		if err != nil {
			return Intent{}, fmt.Errorf("couldn't read the quantity in %q", text)
		}
		qty = model.FromFloat(v, 6)
	default:
		return Intent{}, fmt.Errorf(
			`couldn't find a quantity in %q — say how many shares or a dollar amount, e.g. "buy 2 shares of AAPL" or "buy $500 of AAPL"`, text)
	}

	if side == broker.Sell && !notional.IsZero() {
		return Intent{}, fmt.Errorf("a sell needs a share quantity, not a dollar amount — you can only sell what you hold")
	}

	m := symbolRe.FindStringSubmatch(work)
	// The optional "shares"/"of" groups in symbolRe can leave one of those
	// words themselves unconsumed and captured as the symbol when nothing
	// real follows them (e.g. "buy 2 shares" with nothing after it) — treat
	// that the same as no match at all rather than trying to trade "SHARES".
	if m == nil || symbolStopwords[strings.ToLower(m[1])] {
		return Intent{}, fmt.Errorf(`couldn't find a stock symbol in %q — try "buy 2 shares of AAPL"`, text)
	}

	return Intent{
		Kind: KindOrder, Side: side, Symbol: resolveSymbol(m[1]),
		Qty: qty, Notional: notional,
		TakeProfitPrice: tpPrice, TakeProfitPct: tpPct,
		StopLossPrice: slPrice, StopLossPct: slPct,
	}, nil
}
