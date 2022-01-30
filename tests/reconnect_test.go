//go:build integration
// +build integration

package tests

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/loghole/database"
	"github.com/loghole/database/hooks"
)

func TestReconnectHook(t *testing.T) {
	const dbName = "base"

	initDatabase(t, dbName)

	trace, closer := tracer(t)
	defer closer()

	db, err := database.New(&database.Config{
		Addr:     "haproxy:12757",
		User:     "root",
		Type:     database.PostgresDatabase,
		Database: dbName,
	}, database.WithReconnectHook(), database.WithTracingHook(trace))
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	ctx, span := trace.Start(context.TODO(), t.Name())
	defer span.End()

	err = db.RunTxx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return sql.ErrNoRows
	})

	assert.ErrorIs(t, err, sql.ErrNoRows)

	var val string

	err = db.GetContext(ctx, &val, `SELECT name FROM test LIMIT 1`)
	assert.NoError(t, err)

	time.Sleep(time.Second * 5) // nolint:gomnd // it's ok.

	err = db.GetContext(ctx, &val, `SELECT name FROM test LIMIT 1`)
	assert.ErrorIs(t, err, hooks.ErrCanRetry)

	err = db.GetContext(ctx, &val, `SELECT name FROM test LIMIT 1`)
	assert.NoError(t, err)
}

func TestReconnectHook_WithRetryFunc(t *testing.T) {
	const dbName = "with_retry"

	initDatabase(t, dbName)

	trace, closer := tracer(t)
	defer closer()

	db, err := database.New(&database.Config{
		Addr:     "haproxy:12757",
		User:     "root",
		Type:     database.PostgresDatabase,
		Database: dbName,
	}, database.WithReconnectHook(), database.WithCockroachRetryFunc(), database.WithTracingHook(trace))
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	ctx, span := trace.Start(context.TODO(), t.Name())
	defer span.End()

	err = db.RunTxx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return sql.ErrNoRows
	})

	assert.ErrorIs(t, err, sql.ErrNoRows)

	var val string

	err = db.GetContext(ctx, &val, `SELECT name FROM test LIMIT 1`)
	assert.NoError(t, err)

	time.Sleep(time.Second * 5) // nolint:gomnd // it's ok.

	err = db.GetContext(ctx, &val, `SELECT name FROM test LIMIT 1`)
	assert.NoError(t, err)
}

type testInsert struct {
	Name string `db:"name"`
}

func initDatabase(t *testing.T, name string) {
	ctx := context.TODO()

	db, err := database.New(&database.Config{
		Addr: "haproxy:12757",
		User: "root",
		Type: database.PostgresDatabase,
	})
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE DATABASE IF NOT EXISTS `+name)
	assert.NoError(t, err)

	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+name+`.test (
		id UUID NOT NULL DEFAULT gen_random_uuid(),
		name STRING NOT NULL
	)`)
	assert.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO `+name+`.test (name) VALUES('test')`)
	assert.NoError(t, err)

	_, err = db.NamedExecContext(ctx, `INSERT INTO `+name+`.test (name) VALUES(:name)`, &testInsert{Name: "asd"})
	assert.NoError(t, err)
}

func tracer(t *testing.T) (trace.Tracer, func() error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithAgentEndpoint(jaeger.WithAgentHost("jaeger")))
	require.NoError(t, err)

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSpanProcessor(tracesdk.NewBatchSpanProcessor(exp)),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("database-test"),
		)),
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)

	closer := func() error {
		return tp.Shutdown(context.Background())
	}

	return tp.Tracer("default"), closer
}
