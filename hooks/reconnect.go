package hooks

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/loghole/dbhook"
)

type ReconnectHook struct {
	config *Config
}

var ErrCanRetry = errors.New("connection reconnect")

func NewReconnectHook(config *Config) *ReconnectHook {
	return &ReconnectHook{
		config: config,
	}
}

func (rh *ReconnectHook) Error(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	if input.Error != nil && isReconnectError(input.Error) {
		if err := rh.config.ReconnectFn(); err != nil {
			return ctx, fmt.Errorf("reconnect error: %w", err)
		}

		return ctx, fmt.Errorf("%w: %s", ErrCanRetry, input.Error.Error())
	}

	return ctx, input.Error
}

func isReconnectError(err error) bool {
	msg := err.Error()

	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "connection timed out") ||
		strings.Contains(msg, "unexpected EOF")
}
