package alpaca

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/Rextrc/real-time-market-data-pipeline/internal/model"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestDefaultsToPaper is the load-bearing test in this package: an empty
// config must land on the paper endpoint, not on whatever the underlying SDK
// happens to default to (which is Alpaca's live URL).
func TestDefaultsToPaper(t *testing.T) {
	c, err := New(Config{APIKey: "k", APISecret: "s"}, fakeRegistry{}, quietLog())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.baseURL != PaperBaseURL {
		t.Errorf("baseURL = %q, want the paper endpoint %q", c.baseURL, PaperBaseURL)
	}
	if c.Name() != "alpaca-paper" {
		t.Errorf("Name() = %q, want alpaca-paper", c.Name())
	}
}

// TestLiveRequiresExplicitOptIn is the safety interlock itself: naming the
// live URL without AllowLive must fail closed, not fail open.
func TestLiveRequiresExplicitOptIn(t *testing.T) {
	_, err := New(Config{APIKey: "k", APISecret: "s", BaseURL: LiveBaseURL}, fakeRegistry{}, quietLog())
	if !errors.Is(err, ErrLiveNotAllowed) {
		t.Fatalf("err = %v, want ErrLiveNotAllowed", err)
	}
}

// TestLiveWithOptInIsAccepted confirms the interlock is a real gate, not a
// permanent lock — the documented way through must actually work.
func TestLiveWithOptInIsAccepted(t *testing.T) {
	c, err := New(Config{
		APIKey: "k", APISecret: "s", BaseURL: LiveBaseURL, AllowLive: true,
	}, fakeRegistry{}, quietLog())
	if err != nil {
		t.Fatalf("New with AllowLive: %v", err)
	}
	if c.Name() != "alpaca-live" {
		t.Errorf("Name() = %q, want alpaca-live", c.Name())
	}
}

// TestAllowLiveDoesNotAcceptArbitraryHosts guards against AllowLive being
// read as "trust any URL" rather than "trust Alpaca's live URL specifically".
// A vault typo or a copy-paste of the wrong host must not be able to submit
// orders to it just because AllowLive happened to be set for a legitimate
// live deployment elsewhere.
func TestAllowLiveDoesNotAcceptArbitraryHosts(t *testing.T) {
	_, err := New(Config{
		APIKey: "k", APISecret: "s",
		BaseURL: "https://evil.example.com", AllowLive: true,
	}, fakeRegistry{}, quietLog())
	if err == nil {
		t.Fatal("expected an error for a non-Alpaca host even with AllowLive set")
	}
	if errors.Is(err, ErrLiveNotAllowed) {
		t.Error("wrong error: an arbitrary host should be rejected on its own terms, not as a missing opt-in")
	}
}

func TestPaperURLIsCaseInsensitive(t *testing.T) {
	c, err := New(Config{
		APIKey: "k", APISecret: "s", BaseURL: "HTTPS://PAPER-API.ALPACA.MARKETS",
	}, fakeRegistry{}, quietLog())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.baseURL != "HTTPS://PAPER-API.ALPACA.MARKETS" {
		t.Errorf("baseURL was altered: %q", c.baseURL)
	}
}

func TestMissingCredentialsRejected(t *testing.T) {
	if _, err := New(Config{APISecret: "s"}, fakeRegistry{}, quietLog()); err == nil {
		t.Error("expected an error with no API key")
	}
	if _, err := New(Config{APIKey: "k"}, fakeRegistry{}, quietLog()); err == nil {
		t.Error("expected an error with no API secret")
	}
}

func TestSymbolTranslation(t *testing.T) {
	cases := map[string]string{
		"BTCUSDT": "BTC/USD",
		"ethusdt": "ETH/USD",
		"SOLUSD":  "SOL/USD",
	}
	for in, want := range cases {
		got, err := toAlpacaCryptoSymbol(in)
		if err != nil {
			t.Errorf("toAlpacaCryptoSymbol(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("toAlpacaCryptoSymbol(%q) = %q, want %q", in, got, want)
		}
	}

	if _, err := toAlpacaCryptoSymbol("EURJPY"); err == nil {
		t.Error("expected an error for a symbol with no recognized quote asset")
	}
}

type fakeRegistry struct{}

func (fakeRegistry) Symbol(venue model.VenueID, id model.InstrumentID) (string, bool) {
	return "BTCUSDT", true
}
