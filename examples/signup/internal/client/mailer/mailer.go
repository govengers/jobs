// Package mailer is the example's mail client (ARC-005). It writes each mail as one JSON line
// instead of talking to a provider; a real client would send the idempotency key as a header
// the provider deduplicates on.
package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/govengers/jobs/examples/signup/internal/model"
)

// Client sends mail; it is safe for concurrent use.
type Client struct {
	mutex  sync.Mutex
	writer io.Writer
}

// Options configures a Client.
type Options struct {
	Writer io.Writer // required; receives one JSON line per mail
}

// New returns a Client.
func New(options *Options) (*Client, error) {
	if options == nil || options.Writer == nil {
		return nil, errors.New("mailer: Options.Writer is required")
	}
	return &Client{writer: options.Writer}, nil
}

// Send writes the mail.
func (c *Client) Send(ctx context.Context, mail *model.Mail) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	line, err := json.Marshal(mail)
	if err != nil {
		return fmt.Errorf("encode mail %s: %w", mail.IdempotencyKey, err)
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, err := c.writer.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("send mail %s: %w", mail.IdempotencyKey, err)
	}
	return nil
}
