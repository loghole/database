package pool

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
)

const dbPingTimeout = 3 * time.Second

const (
	isPending int32 = iota
	isDead
	isLive
)

type DBNode struct {
	config *DBNodeConfig

	addr       string
	driverName string
	priority   int32
	weight     int32

	status      int32
	activeReq   int32
	lastUseTime int64

	db *sqlx.DB
}

func NewDBNode(driverName string, config *DBNodeConfig) (*DBNode, error) {
	if config.Weight == 0 {
		config.Weight = 1
	}

	if config.Priority == 0 {
		config.Priority = 1
	}

	client := &DBNode{
		config:     config,
		addr:       config.DSN(),
		driverName: driverName,
		priority:   int32(config.Priority),
		weight:     int32(config.Weight),
		status:     isPending,
	}

	return client, nil
}

func (db *DBNode) Connect() error {
	log.Println(db.addr)

	stdDB, err := sql.Open(db.driverName, db.addr)
	if err != nil {
		return fmt.Errorf("can't open db: %w", err)
	}

	db.db = sqlx.NewDb(stdDB, strings.Split(db.driverName, "-")[0])

	ctx, cancel := context.WithTimeout(context.TODO(), dbPingTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return err
	}

	if db.config.Type == CockroachDatabase {
		db.config.NodeID = cockroachDBInstans(ctx, db.db)
	}

	return nil
}

func (db *DBNode) Addr() string {
	return db.addr
}

func (db *DBNode) Close() error {
	if st := atomic.LoadInt32(&db.status); st != isPending && st != isDead {
		return ErrIsNotPending
	}

	if db.db == nil {
		return nil
	}

	defer func() { db.db = nil }()

	if err := db.db.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	return nil
}

func (db *DBNode) PingContext(ctx context.Context) error {
	db.setLastUsedTime()

	return db.db.PingContext(ctx)
}

// BindNamed binds a query using the DB driver's bindvar type.
func (db *DBNode) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	db.setLastUsedTime()

	return db.db.BindNamed(query, arg)
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DBNode) Beginx() (*sqlx.Tx, error) {
	db.setLastUsedTime()

	return db.db.Beginx()
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (db *DBNode) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	db.setLastUsedTime()

	return db.db.BeginTxx(ctx, opts)
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DBNode) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	db.setLastUsedTime()

	return db.db.GetContext(ctx, dest, query, args...)
}

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DBNode) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	db.setLastUsedTime()

	return db.db.SelectContext(ctx, dest, query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DBNode) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	db.setLastUsedTime()

	return db.db.ExecContext(ctx, query, args...)
}

// NamedExecContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DBNode) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	db.setLastUsedTime()

	return db.db.NamedExecContext(ctx, query, arg)
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DBNode) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	db.setLastUsedTime()

	return db.db.QueryxContext(ctx, query, args...)
}

// NamedQueryContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DBNode) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	db.setLastUsedTime()

	return db.db.NamedQueryContext(ctx, query, arg)
}

// PreparexContext returns an sqlx.Stmt instead of a sqlx.Stmt.
func (db *DBNode) PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error) {
	db.setLastUsedTime()

	return db.db.PreparexContext(ctx, query)
}

// PrepareNamedContext returns an sqlx.NamedStmt.
func (db *DBNode) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	db.setLastUsedTime()

	return db.db.PrepareNamedContext(ctx, query)
}

// LastUseTime returns time of last started request.
func (db *DBNode) loadLastUseTime() int64 {
	return atomic.LoadInt64(&db.lastUseTime)
}

// ActiveRequests returns all active request of node client.
func (db *DBNode) loadActiveRequests() int32 {
	return atomic.LoadInt32(&db.activeReq)
}

func (db *DBNode) addActiveReq() int32 {
	return atomic.AddInt32(&db.activeReq, 1)
}

func (db *DBNode) subActiveReq() int32 {
	return atomic.AddInt32(&db.activeReq, -1)
}

func (db *DBNode) setLastUsedTime() {
	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())
}

func (db *DBNode) loadStatus() int32 {
	return atomic.LoadInt32(&db.status)
}

func (db *DBNode) setDead() {
	atomic.StoreInt32(&db.status, isDead)
}

func (db *DBNode) setLive() {
	atomic.StoreInt32(&db.status, isLive)
}

func (db *DBNode) setPending() {
	atomic.StoreInt32(&db.status, isPending)
}

func (db *DBNode) copyWithoutDB() *DBNode {
	return &DBNode{ // TODO: add linter!
		config:      db.config,
		addr:        db.addr,
		driverName:  db.driverName,
		priority:    db.priority,
		weight:      db.weight,
		status:      isPending,
		activeReq:   0,
		lastUseTime: 0,
		db:          nil,
	}
}

func cockroachDBInstans(ctx context.Context, db *sqlx.DB) string {
	var nodeID int

	_ = db.GetContext(ctx, &nodeID, `SHOW node_id`)

	return strconv.Itoa(nodeID)
}
