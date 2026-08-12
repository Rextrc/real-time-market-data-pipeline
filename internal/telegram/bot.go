package telegram

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/broker/alpaca"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

// Trader is the subset of *alpaca.Client the bot drives, kept as an
// interface so the command grammar and safety checks below can be tested
// without a real Alpaca connection.
type Trader interface {
	Name() string
	PlaceEquityOrder(ctx context.Context, o alpaca.EquityOrder) (*broker.Fill, error)
	Positions(ctx context.Context) ([]alpaca.PositionInfo, error)
	AccountSummary(ctx context.Context) (*alpaca.AccountSummary, error)
	Quote(ctx context.Context, symbol string) (model.Decimal, error)
}

// BotConfig tunes the bot.
type BotConfig struct {
	// AllowedChatID is the only chat the bot acts on. Telegram bots are
	// discoverable by username, so without this a stranger who finds the
	// bot could place real orders on your account — this is the entire
	// access control, and there is deliberately no way to add a second chat
	// short of restarting with a new value.
	AllowedChatID int64
	// MaxOrderNotional caps one order's dollar size, independent of
	// whatever Alpaca itself would allow. Same reasoning as
	// live.Config.MaxPositionFraction: it's the one number that determines
	// how bad a typo or a misparse can get.
	MaxOrderNotional float64
	// PollTimeout is Telegram's long-poll window per request.
	PollTimeout time.Duration
}

func DefaultBotConfig() BotConfig {
	return BotConfig{MaxOrderNotional: 1000, PollTimeout: 25 * time.Second}
}

// Bot connects one Telegram chat to a Trader.
type Bot struct {
	cfg    BotConfig
	tg     *Client
	trader Trader
	log    *slog.Logger
}

func NewBot(cfg BotConfig, tg *Client, trader Trader, log *slog.Logger) *Bot {
	d := DefaultBotConfig()
	if cfg.MaxOrderNotional <= 0 {
		cfg.MaxOrderNotional = d.MaxOrderNotional
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = d.PollTimeout
	}
	return &Bot{cfg: cfg, tg: tg, trader: trader, log: log}
}

// Run long-polls for messages until ctx is cancelled.
func (b *Bot) Run(ctx context.Context) error {
	var offset int64
	b.log.Warn("TELEGRAM TRADING ENABLED — messages from the allowed chat place real orders",
		"broker", b.trader.Name(), "max_order_notional", b.cfg.MaxOrderNotional)

	for {
		if ctx.Err() != nil {
			return nil
		}

		updates, err := b.tg.GetUpdates(ctx, offset, int(b.cfg.PollTimeout.Seconds()))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			b.log.Warn("telegram getUpdates failed; retrying", "err", err)
			select {
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
				return nil
			}
			continue
		}

		for _, u := range updates {
			offset = u.UpdateID + 1
			if u.Message == nil || strings.TrimSpace(u.Message.Text) == "" {
				continue
			}
			b.handle(ctx, *u.Message)
		}
	}
}

func (b *Bot) handle(ctx context.Context, msg Message) {
	if msg.Chat.ID != b.cfg.AllowedChatID {
		// Deliberately silent: replying "not authorized" to a chat that
		// isn't yours confirms the bot exists and is worth probing further.
		b.log.Warn("ignored message from unauthorized chat", "chat_id", msg.Chat.ID, "from", usernameOf(msg))
		return
	}

	b.log.Info("telegram command", "text", msg.Text)
	reply := b.process(ctx, msg.Text)
	if err := b.tg.SendMessage(ctx, msg.Chat.ID, reply); err != nil {
		b.log.Error("telegram sendMessage failed", "err", err)
	}
}

func usernameOf(msg Message) string {
	if msg.From == nil {
		return ""
	}
	return msg.From.Username
}

func (b *Bot) process(ctx context.Context, text string) string {
	intent, err := Parse(text)
	if err != nil {
		return "⚠️ " + err.Error()
	}

	switch intent.Kind {
	case KindHelp:
		return helpText
	case KindStatus:
		return b.status(ctx)
	case KindPositions:
		return b.positions(ctx)
	case KindQuote:
		price, err := b.trader.Quote(ctx, intent.Symbol)
		if err != nil {
			return "⚠️ " + err.Error()
		}
		return fmt.Sprintf("%s last: $%s", intent.Symbol, price.String())
	case KindClose:
		return b.close(ctx, intent.Symbol)
	case KindOrder:
		return b.order(ctx, intent)
	default:
		return "⚠️ didn't understand that — send \"help\""
	}
}

const helpText = `Commands:
buy 2 shares of AAPL
buy 2 shares of AAPL with a take profit of 160 and stop loss of 140
buy 2 AAPL tp 5% sl 3%
buy $500 of tesla
sell 2 shares of AAPL
close AAPL   (sells the whole position)
price AAPL
positions
status

Orders execute immediately against Alpaca's paper account (real money only
if this deployment was explicitly configured for it). A sell only ever
closes or trims a position this bot can see — it will not open a short.`

func (b *Bot) status(ctx context.Context) string {
	summary, err := b.trader.AccountSummary(ctx)
	if err != nil {
		return "⚠️ " + err.Error()
	}
	return fmt.Sprintf("💼 %s\ncash: $%s\nportfolio value: $%s\nbuying power: $%s",
		b.trader.Name(), summary.Cash.String(), summary.PortfolioValue.String(), summary.BuyingPower.String())
}

func (b *Bot) positions(ctx context.Context) string {
	positions, err := b.trader.Positions(ctx)
	if err != nil {
		return "⚠️ " + err.Error()
	}
	if len(positions) == 0 {
		return "no open positions"
	}
	var sb strings.Builder
	sb.WriteString("open positions:\n")
	for _, p := range positions {
		fmt.Fprintf(&sb, "%s: %s sh @ $%s avg, now $%s (%+.2f%%, %+.2f$)\n",
			p.Symbol, p.Qty.String(), p.AvgEntryPrice.String(), p.CurrentPrice.String(),
			p.UnrealizedPLPc, p.UnrealizedPL.Float())
	}
	return sb.String()
}

func (b *Bot) close(ctx context.Context, symbol string) string {
	held, err := b.heldQty(ctx, symbol)
	if err != nil {
		return "⚠️ " + err.Error()
	}
	if held.IsZero() {
		return fmt.Sprintf("no open position in %s — nothing done", symbol)
	}

	fill, err := b.trader.PlaceEquityOrder(ctx, alpaca.EquityOrder{
		Symbol: symbol, Side: broker.Sell, Qty: held,
		ClientID: clientOrderID(), Reason: "telegram: close",
	})
	if err != nil {
		return "⚠️ order failed: " + err.Error()
	}
	return fmt.Sprintf("✅ closed %s: sold %s @ $%s", symbol, fill.Quantity.String(), fill.Price.String())
}

func (b *Bot) heldQty(ctx context.Context, symbol string) (model.Decimal, error) {
	positions, err := b.trader.Positions(ctx)
	if err != nil {
		return model.Decimal{}, err
	}
	for _, p := range positions {
		if strings.EqualFold(p.Symbol, symbol) {
			return p.Qty, nil
		}
	}
	return model.Decimal{}, nil
}

func (b *Bot) order(ctx context.Context, intent Intent) string {
	// Selling opens a short if nothing is held. This bot only ever closes
	// or trims a long position it can see for itself, never opens a short
	// on a typo or a misparsed quantity — that is a materially different
	// (unbounded) risk than anything else this bot does.
	if intent.Side == broker.Sell {
		held, err := b.heldQty(ctx, intent.Symbol)
		if err != nil {
			return "⚠️ " + err.Error()
		}
		if held.IsZero() {
			return fmt.Sprintf("no open position in %s to sell — nothing done (this bot won't open a short)", intent.Symbol)
		}
		if intent.Qty.Float() > held.Float() {
			return fmt.Sprintf("you asked to sell %s shares of %s but only hold %s — nothing done",
				intent.Qty.String(), intent.Symbol, held.String())
		}
	}

	price, err := b.trader.Quote(ctx, intent.Symbol)
	if err != nil {
		return "⚠️ couldn't get a quote for " + intent.Symbol + ": " + err.Error()
	}
	if price.IsZero() {
		return "⚠️ got a zero quote for " + intent.Symbol + " — refusing to size an order off it"
	}

	order := alpaca.EquityOrder{
		Symbol: intent.Symbol, Side: intent.Side,
		Qty: intent.Qty, Notional: intent.Notional,
		ClientID: clientOrderID(), Reason: "telegram",
	}

	// Resolve a percentage take-profit/stop-loss against the live quote —
	// the parser has no market access, so it hands back a percentage and
	// this is where it becomes a price. Brackets only make sense on the
	// entry (buy) leg; a sell/close is already an exit.
	if intent.Side == broker.Buy {
		order.TakeProfit = intent.TakeProfitPrice
		if intent.TakeProfitPct > 0 {
			order.TakeProfit = model.FromFloat(price.Float()*(1+intent.TakeProfitPct/100), 2)
		}
		order.StopLoss = intent.StopLossPrice
		if intent.StopLossPct > 0 {
			order.StopLoss = model.FromFloat(price.Float()*(1-intent.StopLossPct/100), 2)
		}
	}

	notional := intent.Notional.Float()
	if notional == 0 {
		notional = intent.Qty.Float() * price.Float()
	}
	if notional > b.cfg.MaxOrderNotional {
		return fmt.Sprintf("⚠️ that's ~$%.2f, above the $%.2f cap (MDP_TELEGRAM_MAX_NOTIONAL) — nothing done",
			notional, b.cfg.MaxOrderNotional)
	}

	// A bracket order needs a share count, not a dollar amount — Alpaca
	// rejects a notional bracket outright. Convert here rather than making
	// the person do the division themselves in the chat.
	if (!order.TakeProfit.IsZero() || !order.StopLoss.IsZero()) && order.Qty.IsZero() {
		order.Qty = model.FromFloat(order.Notional.Float()/price.Float(), 6)
		order.Notional = model.Decimal{}
	}

	fill, err := b.trader.PlaceEquityOrder(ctx, order)
	if err != nil {
		return "⚠️ order failed: " + err.Error()
	}

	msg := fmt.Sprintf("✅ %s %s %s @ $%s (~$%.2f)",
		strings.ToLower(string(fill.Side)), fill.Quantity.String(), intent.Symbol, fill.Price.String(),
		fill.Quantity.Float()*fill.Price.Float())
	if !order.TakeProfit.IsZero() {
		msg += fmt.Sprintf("\n  take-profit @ $%s", order.TakeProfit.String())
	}
	if !order.StopLoss.IsZero() {
		msg += fmt.Sprintf("\n  stop-loss @ $%s", order.StopLoss.String())
	}
	return msg
}

func clientOrderID() string {
	var buf [12]byte
	_, _ = crand.Read(buf[:])
	return "tg-" + hex.EncodeToString(buf[:])
}
