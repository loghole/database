package hooks

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/loghole/dbhook"

	"github.com/loghole/database/internal/helpers"
)

type MetricCollector interface {
	SerializationFailureInc(dbType, dbAddr, dbName string)
	QueryDurationObserve(dbType, dbAddr, dbName, operation string, isError bool, since time.Duration)
}

type MetricsHook struct {
	startedAtContextKey struct{}

	config    *Config
	collector MetricCollector
}

func NewMetricsHook(config *Config, collector MetricCollector) *MetricsHook {
	return &MetricsHook{
		config:    config,
		collector: collector,
	}
}

func (h *MetricsHook) Before(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	ctx = context.WithValue(ctx, h.startedAtContextKey, time.Now())

	return ctx, input.Error
}

func (h *MetricsHook) After(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	return h.finish(ctx, input)
}

func (h *MetricsHook) Error(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	if helpers.IsSerialisationFailureErr(input.Error) {
		h.collector.SerializationFailureInc(h.config.Type, h.config.Addr, h.config.Database)
	}

	return h.finish(ctx, input)
}

func (h *MetricsHook) finish(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	if startedAt, ok := ctx.Value(h.startedAtContextKey).(time.Time); ok {
		h.collector.QueryDurationObserve(
			h.config.Type,
			h.config.Addr,
			h.config.Database,
			h.parseOperation(input),
			h.isError(input.Error),
			time.Since(startedAt),
		)
	}

	return ctx, input.Error
}

func (h *MetricsHook) isError(err error) bool {
	return err != nil &&
		!errors.Is(err, sql.ErrNoRows) &&
		!helpers.IsSerialisationFailureErr(err)
}

func (h *MetricsHook) parseOperation(input *dbhook.HookInput) string {
	switch input.Caller { // nolint:exhaustive // not need other types.
	case dbhook.CallerBegin, dbhook.CallerCommit, dbhook.CallerRollback:
		return "tx." + string(input.Caller)
	}

	scan := bufio.NewScanner(strings.NewReader(input.Query))
	scan.Split(scanSQLToken)

	for scan.Scan() {
		switch txt := strings.ToLower(scan.Text()); txt {
		case "select",
			"insert",
			"update",
			"delete",
			"call",
			"exec",
			"execute":
			return txt
		}
	}

	return "unknown"
}

func scanSQLToken(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Skip leading spaces.
	var start int

	for width := 0; start < len(data); start += width { // nolint:wastedassign // width used
		var r rune

		r, width = utf8.DecodeRune(data[start:])

		if !isDelimiter(r) {
			break
		}
	}

	// Scan until space, marking end of word.
	for width, i := 0, start; i < len(data); i += width { // nolint:wastedassign // width used
		var r rune

		r, width = utf8.DecodeRune(data[i:])

		if isDelimiter(r) {
			return i + width, data[start:i], nil
		}
	}

	// If we're at EOF, we have a final, non-empty, non-terminated word. Return it.
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}

	// Request more data.
	return start, nil, nil
}

func isDelimiter(r rune) bool {
	// High-valued ones.
	if '\u2000' <= r && r <= '\u200a' {
		return true
	}

	switch r {
	case ' ', '\t', '\n', '\v', '\f', '\r', ';', '(', ')', '.', ',':
		return true
	case '\u0085', '\u00A0':
		return true
	case '\u1680', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000':
		return true
	default:
		return false
	}
}
