package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loghole/database/internal/dbsqlx"
	"github.com/loghole/database/internal/signal"
)

type Pool interface {
	DoQuery(ctx context.Context, cb func(ctx context.Context, db dbsqlx.Database) error) error
}

const (
	_reqMultiplier     int32 = 10_000
	_pingInterval            = time.Second
	_drainWaitInterval       = time.Second
)

var (
	ErrIsNotPending       = errors.New("is not pending")
	ErrNoAvailableClients = errors.New("no available clients")
)

// TODO нужна валидация конфига
func NewClusterPool(activeCount int, configs []DBNodeConfig) (Pool, error) {
	var (
		index      = make(map[uint][]DBNodeConfig)
		priorities = make([]uint, 0)
	)

	for _, config := range configs {
		var exists bool

		for _, p := range priorities {
			if p == config.Priority {
				exists = true

				break
			}
		}

		if !exists {
			priorities = append(priorities, config.Priority)
		}

		index[config.Priority] = append(index[config.Priority], config)
	}

	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i] < priorities[j]
	})

	pool := &ClusterPool{
		activeTarget: int32(activeCount),
		clients:      make([][]*DBNode, len(priorities)),
		deadSignal:   signal.New(),
		liveSignal:   signal.New(),
	}

	for idx, priority := range priorities {
		for _, config := range index[priority] {
			node, err := NewDBNode(config)
			if err != nil {
				return nil, err
			}

			if pool.activeCurrent < pool.activeTarget {
				log.Println("connect ", idx, config.Addr)

				if err := node.Connect(); err != nil {
					return nil, err
				}

				pool.setLive(node)
			}

			pool.clients[idx] = append(pool.clients[idx], node)
		}
	}

	go pool.connectWorker()
	go pool.closeWorker()

	return pool, nil
}

type ClusterPool struct {
	clients [][]*DBNode

	activeTarget  int32
	activeCurrent int32

	drainMu    sync.RWMutex
	pendingMu  sync.Mutex
	deadSignal signal.Signal
	liveSignal signal.Signal
}

func copyNodeWithoutDB(node *DBNode) *DBNode {
	return &DBNode{ // TODO add linter?
		addr:        node.addr,
		driverName:  node.driverName,
		priority:    node.priority,
		weight:      node.weight,
		status:      isPending,
		activeReq:   0,
		lastUseTime: 0,
		db:          nil,
	}
}

func (p *ClusterPool) DoQuery(ctx context.Context, cb func(ctx context.Context, db dbsqlx.Database) error) error {
	for {
		node, err := p.next(ctx)
		if err != nil {
			return err
		}

		if err := cb(ctx, node); err != nil {
			if isReconnectError(err) {
				p.errorf(ctx, "select: %v", err)
				p.setDead(node)

				continue
			}

			return err
		}

		break
	}

	return nil
}

func (p *ClusterPool) next(ctx context.Context) (*DBNode, error) {
	liveNode, err := p.nextByStatus(isLive)
	if err == nil {
		return liveNode, nil
	}

	if !errors.Is(err, ErrNoAvailableClients) {
		p.errorf(ctx, "next live: %v", err)
	}

	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()

	liveNode, err = p.nextByStatus(isLive)
	if err == nil {
		return liveNode, nil
	}

	// Если закончились живые ноды - пробуем подключиться к ожидающим.
	for {
		log.Println("get next from pending nodes") // TODO remove

		pendingNode, err := p.nextByStatus(isPending)
		if err != nil {
			return nil, fmt.Errorf("next pending: %w", err)
		}

		if err := pendingNode.Connect(); err != nil {
			p.setDead(pendingNode)
			p.errorf(ctx, "connect: %v", err)

			continue
		}

		p.setLive(pendingNode)

		return pendingNode, nil
	}
}

func (p *ClusterPool) setDead(node *DBNode) {
	atomic.StoreInt32(&node.status, isDead)
	atomic.AddInt32(&p.activeCurrent, -1)

	p.deadSignal.Send()
}

func (p *ClusterPool) setLive(node *DBNode) {
	atomic.StoreInt32(&node.status, isLive)
	atomic.AddInt32(&p.activeCurrent, 1)

	p.liveSignal.Send()
}

func (p *ClusterPool) setPending(node *DBNode) {
	atomic.StoreInt32(&node.status, isPending)
	atomic.AddInt32(&p.activeCurrent, -1)
}

func (p *ClusterPool) nextByStatus(status int32) (*DBNode, error) {
	p.drainMu.RLock()
	defer p.drainMu.RUnlock()

	clients := p.clients // TODO зачем?

	for _, list := range clients {
		var (
			minClient *DBNode

			minWeight int32 = math.MaxInt32
			minTime   int64 = math.MaxInt64
		)

		for _, client := range list {
			if atomic.LoadInt32(&client.status) != status {
				continue
			}

			var (
				weight  = client.ActiveRequests() * _reqMultiplier / client.weight
				useTime = client.LastUseTime()
			)

			if weight < minWeight || (weight == minWeight && useTime < minTime) {
				minClient = client
				minWeight = weight
				minTime = useTime
			}
		}

		if minClient == nil {
			continue
		}

		// TODO добавить активности + перепроверить статус

		return minClient, nil
	}

	return nil, ErrNoAvailableClients
}

func (p *ClusterPool) connectWorker() {
	var (
		node        *DBNode
		err         error
		lastLogTime time.Time
	)

	for {
		node, err = p.nextByStatus(isDead)
		if err != nil {
			<-p.deadSignal

			continue
		}

		if err = node.Connect(); err == nil {
			p.setLive(node)

			continue
		}

		if time.Now().Add(-time.Minute).After(lastLogTime) {
			lastLogTime = time.Now()

			p.errorf(context.TODO(), "dead node reconnect %s: %v", node.Addr(), err)
		}

		timer := time.NewTimer(_pingInterval)

		select {
		case <-timer.C:
		case <-p.deadSignal:
		}

		if !timer.Stop() {
			<-timer.C
		}
	}
}

// TODO: выглядит сложновато...
//  также будут работать n нод из разных приоритетов, но если все слить на одну ноду,
//  она может отвалиться под нагрузкой что также не круто.
//
// Кажется есть проблемы с концепцией. Возможно стоит в пуле держать только активные коннекты, а не активные убирать из пула.
// Иначе очень сложно решить проблемы гонки данных.
func (p *ClusterPool) closeWorker() { // TODO pendingWorker?
LIVE:
	for range p.liveSignal {
		for i := len(p.clients); i >= 0; i-- {
			for j := len(p.clients[i]); j >= 0; j-- {
				if atomic.LoadInt32(&p.activeCurrent) <= p.activeTarget {
					continue LIVE
				}

				node := p.getNode(i, j)

				if atomic.LoadInt32(&node.status) != isLive {
					continue
				}

				p.toDrain(i, j)

				// Встаем ждать в отдельном потоке
				go func() {
					// Встаем в ожидании завершения активных запросов.
					for atomic.LoadInt32(&node.activeReq) != 0 {
						time.Sleep(_drainWaitInterval)
					}

					if err := node.Close(); err != nil {
						p.errorf(context.Background(), "close node conn %s: %v", node.Addr(), err)
					}
				}()
			}
		}
	}
}

func (p *ClusterPool) getNode(i, j int) *DBNode {
	p.drainMu.RLock()
	defer p.drainMu.RUnlock()

	return p.clients[i][j]
}

func (p *ClusterPool) toDrain(i, j int) {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()

	p.clients[i][j] = copyNodeWithoutDB(p.clients[i][j])
	p.setPending(p.clients[i][j])
}

// TODO: add cool logger.
func (p *ClusterPool) errorf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[database] "+format, args...)
}

func isReconnectError(err error) bool {
	msg := err.Error()

	// TODO отсортировать по частоте получения ошибки
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "connection timed out") || // TODO: а таймаут с контекстом тут не заретраится случайно?
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "try another node")
}
