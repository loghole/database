package addrlist

import (
	"context"
	"database/sql"
	"fmt"
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

type NodeConfig struct {
	Host      string
	AuthToken string
}

type NodeDB struct {
	addr       string
	driverName string
	priority   uint32
	weight     int32

	status      int32
	activeReq   int32
	lastUseTime int64

	db *sqlx.DB
}

func NewNodeDB(addr *DBAddr) (*NodeDB, error) {
	client := &NodeDB{
		addr:       addr.Addr,
		driverName: addr.DriverName,
		priority:   uint32(addr.Priority),
		weight:     int32(addr.Weight),
		status:     isPending,
	}

	return client, nil
}

func (db *NodeDB) Connect() error {
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

	db.SetIsLive()

	return nil
}

func (db *NodeDB) Addr() string {
	return db.addr
}

func (db *NodeDB) Close() error {
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

func (db *NodeDB) PingContext(ctx context.Context) error {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	if err := db.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping context: %w", err)
	}

	return nil
}

// BindNamed binds a query using the DB driver's bindvar type.
func (db *NodeDB) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.BindNamed(query, arg)
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *NodeDB) Beginx() (*sqlx.Tx, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.Beginx()
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (db *NodeDB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.BeginTxx(ctx, opts)
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *NodeDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.GetContext(ctx, dest, query, args...)
}

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *NodeDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.SelectContext(ctx, dest, query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *NodeDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.ExecContext(ctx, query, args...)
}

// NamedExecContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *NodeDB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.NamedExecContext(ctx, query, arg)
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *NodeDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.QueryxContext(ctx, query, args...)
}

// NamedQueryContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *NodeDB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.NamedQueryContext(ctx, query, arg)
}

// PreparexContext returns an sqlx.Stmt instead of a sqlx.Stmt.
func (db *NodeDB) PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.PreparexContext(ctx, query)
}

// PrepareNamedContext returns an sqlx.NamedStmt.
func (db *NodeDB) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	atomic.AddInt32(&db.activeReq, 1)
	defer atomic.AddInt32(&db.activeReq, -1)

	atomic.StoreInt64(&db.lastUseTime, time.Now().UnixNano())

	return db.db.PrepareNamedContext(ctx, query)
}

// ActiveRequests returns all active request of node client.
func (db *NodeDB) ActiveRequests() int32 {
	return atomic.LoadInt32(&db.activeReq)
}

// LastUseTime returns time of last started request.
func (db *NodeDB) LastUseTime() int64 {
	return atomic.LoadInt64(&db.lastUseTime)
}

func (db *NodeDB) SetIsPending() {
	atomic.StoreInt32(&db.status, isPending)
}

func (db *NodeDB) SetIsDead() {
	atomic.StoreInt32(&db.status, isDead)
}

func (db *NodeDB) SetIsLive() {
	atomic.StoreInt32(&db.status, isLive)
}
