package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// LLM asks Claude for trading signals from recent candle data.
//
// Read this before running it with any expectation of profit:
//
// A language model is not a forecasting model. It has no training signal for
// "what does BTC do in the next fifteen minutes", it cannot see order flow,
// funding, or positioning, and its notion of a chart is a few hundred numbers
// in a prompt. What it is genuinely good at is stating a rationale in
// English, which makes it a legible strategy — every trade in the log carries
// a reason you can read and argue with. That is the reason to run it: to have
// a second, differently-wrong opinion next to the momentum baseline, and to
// find out how the two compare on identical data.
//
// The honest prior is that it will underperform the momentum baseline after
// fees. Run both; let the P&L settle it.
//
// The engine, not this strategy, enforces every risk limit. The model's
// entire authority is one of three words per instrument.
type LLM struct {
	client   anthropic.Client
	modelID  string
	bars     int
	log      *slog.Logger
	baseline Strategy
}

// LLMConfig configures the strategy.
type LLMConfig struct {
	// APIKey is the Anthropic API key. Empty falls back to the
	// ANTHROPIC_API_KEY environment variable.
	APIKey string
	// Model defaults to Claude Opus 5.
	Model string
	// Bars is how many recent candles to include per instrument. More
	// context costs more per call and rarely helps past a few dozen.
	Bars int
	// Baseline, if set, has its signals included in the prompt as a
	// reference opinion. Useful for seeing whether the model agrees with a
	// mechanical rule or contradicts it.
	Baseline Strategy
}

const defaultLLMModel = "claude-opus-5"

func NewLLM(cfg LLMConfig, log *slog.Logger) *LLM {
	opts := []option.RequestOption{}
	if cfg.APIKey != "" {
		opts = append(opts, option.WithAPIKey(cfg.APIKey))
	}
	if cfg.Model == "" {
		cfg.Model = defaultLLMModel
	}
	if cfg.Bars <= 0 {
		cfg.Bars = 40
	}

	return &LLM{
		client:   anthropic.NewClient(opts...),
		modelID:  cfg.Model,
		bars:     cfg.Bars,
		log:      log,
		baseline: cfg.Baseline,
	}
}

func (l *LLM) Name() string { return "llm(" + l.modelID + ")" }

const llmSystemPrompt = `You are evaluating short-term price action (crypto or
equities — check the instrument names) for a trading account. Positions may
be real orders against a broker's paper-trading account; treat every
decision as if it matters.

You will receive recent OHLCV candles for one or more instruments, plus the
account's current positions.

For each instrument, decide one action:
  buy  - open a long position (only meaningful if not already held)
  sell - close an existing long position (only meaningful if held)
  hold - do nothing

Rules:
- The account is long-only. There is no shorting and no leverage.
- One position per instrument, sized by the engine. You do not choose size.
- Every fill pays a fee and crosses the spread, so a trade needs a real
  expected move to be worth making. Churning loses money by construction.
- "hold" is the correct answer most of the time. A signal on every evaluation
  is a sign you are reading noise.
- Base your reasoning only on the data provided. You cannot see news, order
  books, or funding rates. If the data does not support a view, say so and
  hold.

Respond with JSON only, no prose outside it, in exactly this shape:

{"signals":[{"instrument":"BTC-USDT","action":"hold","strength":0.0,"reason":"..."}]}

strength is 0..1 and expresses conviction; the engine scales position size by
it. Keep reason under 200 characters and make it specific to the data.`

func (l *LLM) Evaluate(ctx context.Context, mk Market) ([]Signal, error) {
	prompt, err := l.buildPrompt(ctx, mk)
	if err != nil {
		return nil, err
	}
	if prompt == "" {
		return nil, nil // not enough data yet
	}

	// A bounded timeout matters here: this call sits inside the trading
	// loop, and a hung request would stall evaluation indefinitely.
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	resp, err := l.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.Model(l.modelID),
		MaxTokens: 4096,
		System: []anthropic.TextBlockParam{{
			Text: llmSystemPrompt,
			// The system prompt is byte-stable across every call, so
			// caching it turns most of the per-call input cost into a
			// cache read. Over a week of evaluations that is the
			// difference between a trivial bill and an annoying one.
			CacheControl: anthropic.NewCacheControlEphemeralParam(),
		}},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(prompt)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("llm: request failed: %w", err)
	}

	// A refusal is a successful HTTP response with no usable content.
	// Treating it as "hold" is the right call: the strategy declines to act
	// rather than the engine crashing mid-run.
	if resp.StopReason == anthropic.StopReasonRefusal {
		l.log.Warn("llm declined to answer", "category", resp.StopDetails.Category)
		return nil, nil
	}

	var text strings.Builder
	for _, block := range resp.Content {
		if tb, ok := block.AsAny().(anthropic.TextBlock); ok {
			text.WriteString(tb.Text)
		}
	}

	signals, err := parseSignals(text.String(), mk.Instruments())
	if err != nil {
		l.log.Warn("llm response unparseable", "err", err, "response", truncate(text.String(), 500))
		return nil, nil // a bad response means no trade, never a guessed trade
	}

	l.log.Info("llm evaluation",
		"signals", len(signals),
		"input_tokens", resp.Usage.InputTokens,
		"cache_read_tokens", resp.Usage.CacheReadInputTokens,
		"output_tokens", resp.Usage.OutputTokens)

	return signals, nil
}

func (l *LLM) buildPrompt(ctx context.Context, mk Market) (string, error) {
	var b strings.Builder
	b.WriteString("Current time (UTC): ")
	b.WriteString(time.Now().UTC().Format(time.RFC3339))
	b.WriteString("\n\n")

	var included int
	for _, id := range mk.Instruments() {
		bars := mk.Candles(id, l.bars)
		if len(bars) < 5 {
			continue
		}
		included++

		fmt.Fprintf(&b, "## %s\n", id)
		if last, ok := mk.Last(id); ok {
			fmt.Fprintf(&b, "Last trade: %s\n", last.Price)
		}
		b.WriteString("open_time,open,high,low,close,volume,trades\n")
		for _, c := range bars {
			fmt.Fprintf(&b, "%s,%s,%s,%s,%s,%s,%d\n",
				c.OpenTime.Format("15:04:05"),
				c.Open, c.High, c.Low, c.Close, c.Volume, c.Trades)
		}
		b.WriteString("\n")
	}

	if included == 0 {
		return "", nil
	}

	if l.baseline != nil {
		base, err := l.baseline.Evaluate(ctx, mk)
		if err == nil && len(base) > 0 {
			b.WriteString("## Reference: mechanical baseline signals\n")
			for _, s := range base {
				fmt.Fprintf(&b, "%s: %s (%s)\n", s.Instrument, s.Action, s.Reason)
			}
			b.WriteString("\nYou may agree or disagree with the baseline; say which and why.\n\n")
		}
	}

	b.WriteString("Respond with the JSON object described in your instructions.")
	return b.String(), nil
}

// parseSignals extracts the JSON object from the response and validates every
// field. Anything unrecognized becomes a hold rather than a guess — an
// ambiguous model response must never turn into a trade.
func parseSignals(text string, known []model.InstrumentID) ([]Signal, error) {
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("no JSON object in response")
	}

	var payload struct {
		Signals []struct {
			Instrument string  `json:"instrument"`
			Action     string  `json:"action"`
			Strength   float64 `json:"strength"`
			Reason     string  `json:"reason"`
		} `json:"signals"`
	}
	if err := json.Unmarshal([]byte(text[start:end+1]), &payload); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	valid := make(map[model.InstrumentID]struct{}, len(known))
	for _, id := range known {
		valid[id] = struct{}{}
	}

	now := time.Now().UTC()
	out := make([]Signal, 0, len(payload.Signals))
	for _, s := range payload.Signals {
		id := model.InstrumentID(strings.ToUpper(strings.TrimSpace(s.Instrument)))
		if _, ok := valid[id]; !ok {
			// The model named an instrument we do not trade. Ignoring it
			// is the only safe response.
			continue
		}

		var action Action
		switch strings.ToLower(strings.TrimSpace(s.Action)) {
		case "buy":
			action = ActionBuy
		case "sell":
			action = ActionSell
		case "hold", "":
			continue
		default:
			continue
		}

		strength := s.Strength
		if strength <= 0 || strength > 1 {
			strength = 1
		}

		out = append(out, Signal{
			Instrument: id,
			Action:     action,
			Strength:   strength,
			Reason:     truncate(s.Reason, 200),
			Time:       now,
		})
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

var _ Strategy = (*LLM)(nil)
