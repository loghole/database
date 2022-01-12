package hooks

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

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
	return err != nil && !errors.Is(err, sql.ErrNoRows)
}

func (h *MetricsHook) parseOperation(input *dbhook.HookInput) string {
	switch input.Caller { // nolint:exhaustive // not need other types.
	case dbhook.CallerBegin, dbhook.CallerCommit, dbhook.CallerRollback:
		return "tx." + string(input.Caller)
	}

	scan := bufio.NewScanner(strings.NewReader(input.Query))
	scan.Split(bufio.ScanWords)

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
