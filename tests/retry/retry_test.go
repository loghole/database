//go:build integration
// +build integration

package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

	"github.com/loghole/database"
)

type collector struct {
	serializationFailureInc int64
	queryDurationObserve    int64
}

func (c *collector) SerializationFailureInc(dbType, dbAddr, dbName string) {
	atomic.AddInt64(&c.serializationFailureInc, 1)
}

func (c *collector) QueryDurationObserve(dbType, dbAddr, dbName, operation string, isError bool, since time.Duration) {
	atomic.AddInt64(&c.queryDurationObserve, 1)
}

func (c *collector) check(t *testing.T, serializationFailureInc, queryDurationObserve int64) {
	t.Helper()

	assert.Equal(t, serializationFailureInc, c.serializationFailureInc, "serializationFailureInc not equal")
	assert.Equal(t, queryDurationObserve, c.queryDurationObserve, "queryDurationObserve not equal")
}

func (c *collector) reset() {
	atomic.StoreInt64(&c.serializationFailureInc, 0)
	atomic.StoreInt64(&c.queryDurationObserve, 0)
}

func TestMetrics(t *testing.T) {
	metric := &collector{}

	db, err := database.New(&database.Config{
		Addr:     "postgres:5432",
		User:     "postgres:password",
		Database: "postgres",
		Type:     database.PostgresDatabase,
	}, database.WithMetricsHook(metric))
	if err != nil {
		t.Error(err)
		return
	}

	initDB(t, db)
	metric.reset()
	metric.check(t, 0, 0)

	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `INSERT INTO test_metric (id, name) VALUES ($1, $2)`, 3, "3"); err != nil {
		t.Error(err)
		return
	}

	metric.check(t, 0, 1)
	metric.reset()

	if _, err := db.ExecContext(ctx, `INSERT INTO test_metric (id, name) VALUES ($1, $2)`, 3, "3"); err == nil {
		t.Error("NO CONFLICT ERROR")
		return
	}

	metric.check(t, 0, 1)
	metric.reset()

	if err := db.RunTxx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO test_metric (id, name) VALUES ($1, $2)`, 4, "4"); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx, `INSERT INTO test_metric (id, name) VALUES ($1, $2)`, 5, "5"); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Error(err)
		return
	}

	metric.check(t, 0, 4)
	metric.reset()

	var count int

	if err := db.GetContext(ctx, &count, `SELECT count(id) AS count FROM test_metric`); err != nil {
		t.Error(err)
		return
	}

	metric.check(t, 0, 1)
	metric.reset()

	if count != 5 {
		t.Errorf("count rows not equal, expected %v, got %v", 5, count)
	}
}

func initDB(t *testing.T, db *database.DB) {
	t.Helper()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS test_metric(
			id INTEGER NOT NULL PRIMARY KEY,
			name TEXT NOT NULL
		)`,
		`TRUNCATE TABLE test_metric`,
		`ALTER DATABASE postgres SET DEFAULT_TRANSACTION_ISOLATION TO SERIALIZABLE`,
		`INSERT INTO test_metric (id, name) VALUES (1, '1')`,
		`INSERT INTO test_metric (id, name) VALUES (2, '2')`,
	}

	for _, query := range queries {
		if _, err := db.ExecContext(context.TODO(), query); err != nil {
			t.Errorf("do query '%s': %v", query, err)
			return
		}
	}
}
