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
	ErrGoToRetry          = errors.New("go to retry")
	ErrIsNotPending       = errors.New("is not pending")
	ErrNoAvailableClients = errors.New("no available clients")
)

type ClusterPool struct {
	clients []*DBNode

	canUseOtherLevel bool
	activeTarget     int32
	activeCurrent    int32

	drainMu       sync.RWMutex
	pendingMu     sync.Mutex
	deadSignal    signal.Signal
	liveSignal    signal.Signal
	pendingSignal signal.Signal
}

func NewClusterPool(
	driverName string,
	activeCount int,
	canUseOtherLevel bool,
	configs []*DBNodeConfig,
) (Pool, error) {
	if activeCount <= 0 {
		activeCount = 1
	}

	var (
		index      = make(map[uint][]*DBNodeConfig)
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
		activeTarget:     int32(activeCount),
		clients:          make([]*DBNode, 0, len(configs)),
		deadSignal:       signal.New(),
		liveSignal:       signal.New(),
		pendingSignal:    signal.New(),
		canUseOtherLevel: canUseOtherLevel,
	}

	// TODO можем конектится если есть мертвые хосты
	for idx, priority := range priorities {
		for _, config := range index[priority] {
			node, err := NewDBNode(driverName, config)
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

			pool.clients = append(pool.clients, node)
		}
	}

	log.Println(pool.clients)

	go pool.connectWorker()
	go pool.pendingWorker()
	go pool.liveWorker()

	return pool, nil
}

func (p *ClusterPool) DoQuery(ctx context.Context, cb func(ctx context.Context, db dbsqlx.Database) error) error {
	for {
		if err := p.doQuery(ctx, cb); err != nil {
			if errors.Is(err, ErrGoToRetry) {
				continue
			}
		}

		break
	}

	return nil
}

func (p *ClusterPool) doQuery(ctx context.Context, cb func(ctx context.Context, db dbsqlx.Database) error) error {
	node, err := p.next(ctx)
	if err != nil {
		return err
	}

	defer node.subActiveReq()

	if err := cb(DBNodeConfigToContext(ctx, node.config), node); err != nil {
		if isReconnectError(err) {
			p.errorf(ctx, "select: %v", err)
			p.setDead(node)

			return ErrGoToRetry
		}

		return err
	}

	return nil
}

func (p *ClusterPool) next(ctx context.Context) (*DBNode, error) {
	var (
		liveNode *DBNode
		err      error
	)

	for {
		liveNode, err = p.nextByStatus(isLive)
		if err == nil {
			//  Вычитаем в самой ноде после завершения запроса.
			liveNode.addActiveReq()

			if liveNode.loadStatus() != isLive {
				liveNode.subActiveReq()

				continue
			}

			return liveNode, nil
		}

		break
	}

	// ЭТО НАДО!!!
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()

	// И ЭТО ТОЖЕ!
	liveNode, err = p.nextByStatus(isLive)
	if err == nil {
		return liveNode, nil
	}

	if !errors.Is(err, ErrNoAvailableClients) {
		p.errorf(ctx, "next live: %v", err)
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

func (p *ClusterPool) nextByStatus(status int32) (*DBNode, error) {
	p.drainMu.RLock()
	defer p.drainMu.RUnlock()

	var (
		minClient *DBNode

		minWeight int32 = math.MaxInt32
		minTime   int64 = math.MaxInt64

		checkedCounter  int32
		priorityChanged bool
	)

	for _, client := range p.clients {
		if client.loadStatus() != status {
			continue
		}

		if !priorityChanged && minClient != nil && client.priority != minClient.priority {
			priorityChanged = true

			if !p.canUseOtherLevel {
				break
			}
		}

		if priorityChanged && p.canUseOtherLevel && checkedCounter >= p.activeTarget {
			break
		}

		checkedCounter++

		var (
			weight  = client.loadActiveRequests() * _reqMultiplier / client.weight
			useTime = client.loadLastUseTime()
		)

		if weight < minWeight || (weight == minWeight && useTime < minTime) {
			minClient = client
			minWeight = weight
			minTime = useTime
		}
	}

	if minClient == nil {
		return nil, ErrNoAvailableClients
	}

	return minClient, nil
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

		timer.Stop()
	}
}

func (p *ClusterPool) pendingWorker() {
LIVE:
	for range p.liveSignal {
		for i := len(p.clients) - 1; i >= 0; i-- {
			if p.loadActiveCurrent() <= p.activeTarget {
				continue LIVE
			}

			node := p.getNode(i)

			if node.loadStatus() != isLive {
				continue
			}

			p.toDrain(i)

			go func() {
				// Встаем в ожидании завершения активных запросов.
				for node.loadActiveRequests() != 0 {
					time.Sleep(_drainWaitInterval)
				}

				if err := node.Close(); err != nil {
					p.errorf(context.Background(), "close node conn %s: %v", node.Addr(), err)
				}
			}()
		}
	}
}

func (p *ClusterPool) liveWorker() {
PENDING:
	for range p.pendingSignal {
		for i := 0; i < len(p.clients); i++ {
			if p.loadActiveCurrent() >= p.activeTarget {
				continue PENDING
			}

			node := p.getNode(i)

			if node.loadStatus() != isPending {
				continue
			}

			if err := node.Connect(); err != nil {
				p.errorf(context.Background(), "pending to live: %v", err)
				p.setDead(node)

				continue
			}

			p.setLive(node)
		}
	}
}

func (p *ClusterPool) getNode(i int) *DBNode {
	p.drainMu.RLock()
	defer p.drainMu.RUnlock()

	return p.clients[i]
}

func (p *ClusterPool) toDrain(i int) {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	p.clients[i] = p.clients[i].copyWithoutDB()
	p.setPending(p.clients[i])
}

// TODO: add cool logger.
func (p *ClusterPool) errorf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[database] "+format, args...)
}

func (p *ClusterPool) setDead(node *DBNode) {
	node.setDead()
	p.subActiveCurrent()

	p.pendingSignal.Send()
	p.deadSignal.Send()
}

func (p *ClusterPool) setLive(node *DBNode) {
	log.Println("active: ", p.addActiveCurrent())
	node.setLive()

	p.liveSignal.Send()
}

func (p *ClusterPool) setPending(node *DBNode) {
	node.setPending()
	p.subActiveCurrent()
}

func (p *ClusterPool) addActiveCurrent() int32 {
	return atomic.AddInt32(&p.activeCurrent, 1)
}

func (p *ClusterPool) subActiveCurrent() int32 {
	return atomic.AddInt32(&p.activeCurrent, -1)
}

func (p *ClusterPool) loadActiveCurrent() int32 {
	return atomic.LoadInt32(&p.activeCurrent)
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
