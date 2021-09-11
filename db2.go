package database

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/loghole/database/hooks"
	"github.com/loghole/database/internal/addrlist"
	"github.com/loghole/database/internal/signal"
)

type DB2 struct {
	pool        addrlist.Pool
	mu          sync.RWMutex
	pendingDBMU sync.Mutex

	retryFunc RetryFunc
	hooksCfg  *hooks.Config
	baseCfg   *Config

	deadCh signal.Signal

	pingInterval time.Duration
}

func NewDB2(cfg *Config, options ...Option) (db *DB2, err error) {
	// TODO validate config

	var (
		hooksCfg = cfg.hookConfig()
		builder  = applyOptions(hooksCfg, options...)
	)

	hooksCfg.DriverName, err = wrapDriver(cfg.driverName(), builder.hook())
	if err != nil {
		return nil, fmt.Errorf("wrap driver: %w", err)
	}

	db = &DB2{
		retryFunc:    builder.retryFunc,
		hooksCfg:     hooksCfg,
		baseCfg:      cfg,
		deadCh:       make(signal.Signal, 1),
		pingInterval: time.Second, // TODO fix
	}

	db.pool, err = addrlist.NewPool(2, hooksCfg.DriverName, cfg.GetAddrList())
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	go db.pingDeadNodes()

	return db, nil
}

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB2) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.runQuery(ctx, func(ctx context.Context, db *addrlist.NodeDB) error {
		return db.SelectContext(ctx, dest, query, args...)
	})
}

func (db *DB2) runQuery(ctx context.Context, fn func(ctx context.Context, db *addrlist.NodeDB) error) error {
	for {
		nodeDB, err := db.next(ctx)
		if err != nil {
			return err
		}

		if err := fn(ctx, nodeDB); err != nil {
			if isReconnectError(err) {
				db.errorf(ctx, "select: %v", err)

				nodeDB.SetIsDead()
				db.deadCh.Send()

				continue
			}

			return err
		}

		break
	}

	return nil
}

func (db *DB2) next(ctx context.Context) (*addrlist.NodeDB, error) {
	nodeDB, err := db.pool.NextLive()
	if err == nil {
		return nodeDB, nil
	}

	if !errors.Is(err, addrlist.ErrNoAvailableClients) {
		db.errorf(ctx, "next live: %v", err)
	}

	db.pendingDBMU.Lock()
	defer db.pendingDBMU.Unlock()

	nodeDB, err = db.pool.NextLive()
	if err == nil {
		return nodeDB, nil
	}

	return db.nextPending(ctx)
}

func (db *DB2) nextPending(ctx context.Context) (*addrlist.NodeDB, error) {
	for {
		log.Println("next pending")

		pendingDB, err := db.pool.NextPending()
		if err != nil {
			return nil, err
		}

		if err := pendingDB.Connect(); err != nil {
			db.errorf(ctx, "connect: %v", err)
			pendingDB.SetIsDead()

			continue
		}

		return pendingDB, nil
	}

	return nil, addrlist.ErrNoAvailableClients
}

// TODO make cool logger.
func (db *DB2) errorf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[database] "+format, args...)
}

func (db *DB2) pingDeadNodes() {
	var (
		client      *addrlist.NodeDB
		err         error
		lastLogTime time.Time
	)

	for {
		client, err = db.pool.NextDead()
		if err != nil {
			<-db.deadCh

			continue
		}

		if err := client.Connect(); err == nil {
			continue
		}

		if time.Now().Add(-time.Minute).After(lastLogTime) {
			lastLogTime = time.Now()

			db.errorf(context.TODO(), "dead node connect %s: %v", client.Addr(), err)
		}

		timer := time.NewTimer(db.pingInterval)

		select {
		case <-timer.C:
		case <-db.deadCh:
		}

		timer.Stop()
	}
}

func isReconnectError(err error) bool {
	msg := err.Error()

	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "connection timed out") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "try another node")
}
