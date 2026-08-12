// Package telegram is a minimal client for the Telegram Bot HTTP API — just
// enough to long-poll for messages and send replies.
//
// This intentionally does not pull in a third-party Telegram SDK. The
// surface actually used here is two JSON endpoints, and a hand-rolled client
// is smaller and easier to audit than a general-purpose bot framework for
// something that only ever polls and replies.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const apiBase = "https://api.telegram.org"

// Client talks to one bot (identified by its token).
type Client struct {
	token string
	http  *http.Client
}

func New(token string) *Client {
	return &Client{
		token: token,
		// Longer than the long-poll timeout GetUpdates asks Telegram for, so
		// a slow-but-empty poll isn't mistaken for a hung connection.
		http: &http.Client{Timeout: 40 * time.Second},
	}
}

// Update is one item from getUpdates. Only the fields this bot's grammar
// needs are decoded — Telegram's payload has many more.
type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

// Message is an incoming chat message.
type Message struct {
	MessageID int64  `json:"message_id"`
	Text      string `json:"text"`
	Chat      Chat   `json:"chat"`
	From      *User  `json:"from"`
}

type Chat struct {
	ID int64 `json:"id"`
}

type User struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	Result      T      `json:"result"`
}

// GetUpdates long-polls for messages received since the last acknowledged
// update. offset should be the highest update_id seen so far, plus one —
// Telegram treats that as "and mark everything before this as delivered."
// timeoutSec is Telegram's own long-poll window on its side of the
// connection; ctx still governs whether this call is cancelled from ours.
func (c *Client) GetUpdates(ctx context.Context, offset int64, timeoutSec int) ([]Update, error) {
	u := fmt.Sprintf("%s/bot%s/getUpdates?offset=%d&timeout=%d", apiBase, c.token, offset, timeoutSec)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out apiResponse[[]Update]
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("telegram: decode getUpdates: %w", err)
	}
	if !out.OK {
		return nil, fmt.Errorf("telegram: getUpdates failed: %s", out.Description)
	}
	return out.Result, nil
}

// SendMessage replies into a chat. Kept as a simple best-effort call: the
// poll loop logs a failed send and moves on to the next update rather than
// treating it as fatal — a reply Telegram couldn't deliver should not stop
// the bot from processing the next command.
func (c *Client) SendMessage(ctx context.Context, chatID int64, text string) error {
	body, err := json.Marshal(map[string]any{
		"chat_id": chatID,
		"text":    text,
	})
	if err != nil {
		return err
	}

	u := apiBase + "/bot" + url.PathEscape(c.token) + "/sendMessage"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var out apiResponse[json.RawMessage]
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("telegram: decode sendMessage: %w", err)
	}
	if !out.OK {
		return fmt.Errorf("telegram: sendMessage failed: %s", out.Description)
	}
	return nil
}
