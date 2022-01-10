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
	ActiveTxInc(dbType, dbAddr, dbName string)
	ActiveTxDec(dbType, dbAddr, dbName string)
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
	if input.Caller == dbhook.CallerBegin {
		h.collector.ActiveTxInc(h.config.Type, h.config.Addr, h.config.Database)
	}

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
	if input.Caller == dbhook.CallerCommit {
		h.collector.ActiveTxDec(h.config.Type, h.config.Addr, h.config.Database)
	}

	if startedAt, ok := ctx.Value(h.startedAtContextKey).(time.Time); ok {
		h.collector.QueryDurationObserve(
			h.config.Type,
			h.config.Addr,
			h.config.Database,
			h.parseOperation(input.Query),
			h.isError(input.Error),
			time.Since(startedAt),
		)
	}

	return ctx, input.Error
}

func (h *MetricsHook) isError(err error) bool {
	return err != nil && !errors.Is(err, sql.ErrNoRows)
}

func (h *MetricsHook) parseOperation(query string) string {
	scan := bufio.NewScanner(strings.NewReader(query))
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
