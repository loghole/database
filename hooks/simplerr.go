package hooks

import (
	"context"
	"errors"
	"strings"

	"github.com/lissteron/simplerr"
	"github.com/loghole/dbhook"
)

type SimplerrHook struct{}

func NewSimplerrHook() *SimplerrHook {
	return &SimplerrHook{}
}

func (h *SimplerrHook) Error(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	if input.Error == nil {
		return ctx, nil
	}

	if simplerr.GetCode(input.Error).Int() != 0 {
		return ctx, input.Error
	}

	if errors.Is(input.Error, ErrCanRetry) {
		return ctx, simplerr.WrapWithCode(input.Error, Reconnected, "reconnected, try again")
	}

	msg := input.Error.Error()

	if strings.HasSuffix(msg, "server is not accepting clients") {
		return ctx, simplerr.WrapWithCode(input.Error, BadConnection, "connection refused, try later")
	}

	if strings.HasSuffix(msg, "connection refused") {
		return ctx, simplerr.WrapWithCode(input.Error, BadConnection, "connection refused, try later")
	}

	return ctx, simplerr.WrapWithCode(input.Error, DatabaseError, "database error")
}
