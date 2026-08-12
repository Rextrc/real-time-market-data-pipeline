// Package binance is a transport-only adapter for Binance's public combined
// stream. It knows how to hold a connection open and hand frames upward; it
// does not know what the frames mean.
package binance

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/instrument"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
	"github.com/Rextrc/real-time-market-data-pipeline/internal/venue"
)

// ID is the canonical venue name used in every Tick and every stored row.
// It is defined in model so that decoders and stores can name this venue
// without importing this package.
const ID = model.VenueBinance

// DefaultEndpoint is the public combined-stream endpoint. Point this at a
// toxiproxy listener to run the M1 and M6 fault-injection exercises.
const DefaultEndpoint = "wss://stream.binance.com:9443/stream"

const (
	// defaultPingInterval is how often we prove the connection is alive.
	//
	// This is the M1 lesson in code. A read deadline alone does not detect a
	// dead connection, because on a quiet instrument there is nothing to
	// read even when everything is healthy — so any read timeout short
	// enough to catch a failure is also short enough to kill a working
	// stream. An application-level ping separates "quiet" from "dead".
	defaultPingInterval = 20 * time.Second

	// defaultPingTimeout is how long a pong may take before we treat the
	// connection as gone.
	defaultPingTimeout = 10 * time.Second

	// defaultReadTimeout is a backstop only. Liveness is the ping's job.
	defaultReadTimeout = 5 * time.Minute

	// readLimit caps a single frame. Trade frames are a few hundred bytes;
	// this is set well above that so a malformed or hostile frame cannot
	// balloon memory.
	readLimit = 1 << 20
)

// Venue streams raw frames from Binance.
type Venue struct {
	endpoint     string
	registry     *instrument.Registry
	clock        model.Clock
	pingInterval time.Duration
	pingTimeout  time.Duration
	readTimeout  time.Duration
}

// Option customizes a Venue. Everything with a sensible default is an option
// so that tests and fault injection do not need their own constructor.
type Option func(*Venue)

// WithEndpoint overrides the websocket URL, e.g. to insert a proxy.
func WithEndpoint(u string) Option { return func(v *Venue) { v.endpoint = u } }

// WithPingInterval sets the liveness probe period. Set it to zero to disable
// pings entirely and watch a black-holed connection hang forever.
func WithPingInterval(d time.Duration) Option { return func(v *Venue) { v.pingInterval = d } }

// WithPingTimeout sets how long a pong may take before the connection is
// declared dead.
func WithPingTimeout(d time.Duration) Option { return func(v *Venue) { v.pingTimeout = d } }

// WithReadTimeout sets the backstop deadline on an individual read.
func WithReadTimeout(d time.Duration) Option { return func(v *Venue) { v.readTimeout = d } }

func New(reg *instrument.Registry, clock model.Clock, opts ...Option) *Venue {
	v := &Venue{
		endpoint:     DefaultEndpoint,
		registry:     reg,
		clock:        clock,
		pingInterval: defaultPingInterval,
		pingTimeout:  defaultPingTimeout,
		readTimeout:  defaultReadTimeout,
	}
	for _, o := range opts {
		o(v)
	}
	return v
}

func (v *Venue) ID() model.VenueID { return ID }

// Stream connects, subscribes to the trade stream for each instrument, and
// forwards every frame to out until ctx is cancelled or the connection fails.
//
// Note what this method does NOT do: it does not reconnect, and it does not
// buffer. Sending to out blocks. In M1 that is fine because the only consumer
// is a printer, but it is precisely the coupling M4 and M5 exist to break —
// today, a slow consumer stalls this read loop, and stalling this read loop
// eventually gets us disconnected by the exchange.
func (v *Venue) Stream(ctx context.Context, instruments []model.InstrumentID, out chan<- venue.RawMessage) error {
	endpoint, err := v.streamURL(instruments)
	if err != nil {
		return err
	}

	conn, _, err := websocket.Dial(ctx, endpoint, nil)
	if err != nil {
		return fmt.Errorf("binance: dial: %w", err)
	}
	conn.SetReadLimit(readLimit)
	defer conn.CloseNow()

	// The ping loop runs against a context that the read loop's exit
	// cancels, so neither goroutine can outlive Stream. A leaked reader per
	// reconnect is invisible until hour six; the structure here is what
	// makes that impossible rather than merely unlikely.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pingErr := make(chan error, 1)
	go func() {
		err := v.pingLoop(ctx, conn)
		// Publish the diagnosis BEFORE tearing down the connection.
		// Closing first would unblock the read with "use of closed network
		// connection", and that generic error would win the race and bury
		// the real reason.
		pingErr <- err
		if err != nil {
			conn.CloseNow()
		}
	}()

	for {
		msg, err := v.read(ctx, conn)
		if err != nil {
			// A ping failure and a read failure race on a dead connection.
			// Prefer the ping's diagnosis: it says "the peer stopped
			// answering", which is more useful than "read was cancelled".
			select {
			case perr := <-pingErr:
				if perr != nil {
					return perr
				}
			default:
			}
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return ctx.Err()
			}
			return err
		}

		select {
		case out <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (v *Venue) read(ctx context.Context, conn *websocket.Conn) (venue.RawMessage, error) {
	readCtx, cancel := context.WithTimeout(ctx, v.readTimeout)
	defer cancel()

	typ, payload, err := conn.Read(readCtx)
	if err != nil {
		if ctx.Err() == nil && readCtx.Err() != nil {
			return venue.RawMessage{}, fmt.Errorf("binance: no frame within %s: %w", v.readTimeout, err)
		}
		return venue.RawMessage{}, fmt.Errorf("binance: read: %w", err)
	}
	if typ != websocket.MessageText {
		return venue.RawMessage{}, fmt.Errorf("binance: unexpected frame type %v", typ)
	}

	return venue.RawMessage{
		Venue:    ID,
		RecvTime: v.clock.Now(),
		Payload:  payload,
	}, nil
}

func (v *Venue) pingLoop(ctx context.Context, conn *websocket.Conn) error {
	if v.pingInterval <= 0 {
		<-ctx.Done()
		return nil
	}

	t := time.NewTicker(v.pingInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			pingCtx, cancel := context.WithTimeout(ctx, v.pingTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				// The caller closes the connection once it has this error,
				// which is what unblocks the read.
				return fmt.Errorf("binance: no pong within %s: %w", v.pingTimeout, err)
			}
		}
	}
}

// streamURL builds the combined-stream URL. Binance wants lowercase venue
// symbols in the path and returns uppercase ones in the payload, which is
// exactly the sort of detail that must not leak past this package.
func (v *Venue) streamURL(instruments []model.InstrumentID) (string, error) {
	if len(instruments) == 0 {
		return "", errors.New("binance: no instruments requested")
	}

	streams := make([]string, 0, len(instruments))
	for _, id := range instruments {
		sym, ok := v.registry.Symbol(ID, id)
		if !ok {
			return "", fmt.Errorf("binance: no venue symbol registered for %s", id)
		}
		streams = append(streams, strings.ToLower(sym)+"@trade")
	}

	u, err := url.Parse(v.endpoint)
	if err != nil {
		return "", fmt.Errorf("binance: bad endpoint %q: %w", v.endpoint, err)
	}
	// Built by hand rather than with url.Values.Encode: Binance expects the
	// "/" separators and "@" suffixes literally, and Encode would
	// percent-escape both.
	u.RawQuery = "streams=" + strings.Join(streams, "/")
	return u.String(), nil
}
