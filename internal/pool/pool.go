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
		activeTarget:  int32(activeCount),
		clients:       make([][]*DBNode, len(priorities)),
		deadSignal:    signal.New(),
		liveSignal:    signal.New(),
		pendingSignal: signal.New(),
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

	log.Println(pool.clients)

	go pool.connectWorker()
	go pool.pendingWorker()
	go pool.liveWorker()

	return pool, nil
}

type ClusterPool struct {
	clients [][]*DBNode

	activeTarget  int32
	activeCurrent int32

	drainMu       sync.RWMutex
	pendingMu     sync.Mutex
	deadSignal    signal.Signal
	liveSignal    signal.Signal
	pendingSignal signal.Signal
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

func (p *ClusterPool) nextByStatus(status int32) (*DBNode, error) {
	p.drainMu.RLock()
	defer p.drainMu.RUnlock()

	for _, list := range p.clients {
		var (
			minClient *DBNode

			minWeight int32 = math.MaxInt32
			minTime   int64 = math.MaxInt64
		)

	RETRY:
		for _, client := range list {
			if client.loadStatus() != status {
				continue
			}

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
			continue
		}

		// Вычитаем на уровень выше
		minClient.addActiveReq()

		if minClient.loadStatus() != status {
			minClient.subActiveReq()

			goto RETRY
		}

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

// TODO: выглядит сложновато... НО НУЖНО УСЛОЖНИТЬ (сказал Виталик.)
// Добавить минимальное кол-во активных нод.
// Например если сдохли 2 ноды из 3 на первом уровне,
// а у нас минимально активные 2, то надо подрубить одну с уровня 2.
// Но есть и ожидаемое кол-во нод, которое при нормальной работе кластера надо стараться поддержать.
func (p *ClusterPool) pendingWorker() {
LIVE:
	for range p.liveSignal {
		for i := len(p.clients) - 1; i >= 0; i-- {
			for j := len(p.clients[i]) - 1; j >= 0; j-- {
				if p.loadActiveCurrent() <= p.activeTarget {
					continue LIVE
				}

				node := p.getNode(i, j)

				if node.loadStatus() != isLive {
					continue
				}

				p.toDrain(i, j)

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
}

func (p *ClusterPool) getNode(i, j int) *DBNode {
	p.drainMu.RLock()
	defer p.drainMu.RUnlock()

	return p.clients[i][j]
}

func (p *ClusterPool) toDrain(i, j int) {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()

	p.clients[i][j] = p.clients[i][j].copyWithoutDB()
	p.setPending(p.clients[i][j])
}

// TODO: add cool logger.
func (p *ClusterPool) errorf(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[database] "+format, args...)
}

func (p *ClusterPool) liveWorker() {
PENDING:
	for range p.pendingSignal {
		log.Println("[PENDING SIGNAL] run loop")

		for i := 0; i < len(p.clients); i++ {
			for j := 0; j < len(p.clients[i]); j++ {
				if p.loadActiveCurrent() >= p.activeTarget {
					log.Println("[PENDING SIGNAL] stop loop")

					continue PENDING
				}

				node := p.getNode(i, j)

				log.Println("[PENDING SIGNAL] node status: ", node.loadStatus(), node.addr)

				if node.loadStatus() != isPending {
					log.Println("[PENDING SIGNAL] get next node")

					continue
				}

				if err := node.Connect(); err != nil {
					p.errorf(context.Background(), "pending to live: %v", err)
					node.setDead()

					log.Println("[PENDING SIGNAL] node failed to connect")

					continue
				}

				log.Println("[PENDING SIGNAL] node connected", node.addr)

				node.setLive()
			}
		}

		log.Println("[PENDING SIGNAL] end loop")
	}
}

func (p *ClusterPool) setDead(node *DBNode) {
	node.setDead()
	p.subActiveCurrent()

	p.pendingSignal.Send()
	p.deadSignal.Send()
}

func (p *ClusterPool) setLive(node *DBNode) {
	p.addActiveCurrent()
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
