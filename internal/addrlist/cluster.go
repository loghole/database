package addrlist

import (
	"errors"
	"log"
	"math"
	"sort"
	"sync/atomic"
)

type Pool interface {
	NextLive() (*NodeDB, error)
	NextDead() (*NodeDB, error)
	NextPending() (*NodeDB, error)
}

const reqMultiplier int32 = 10_000

var (
	ErrIsNotPending       = errors.New("is not pending")
	ErrNoAvailableClients = errors.New("no available clients")
	ErrNoAvailableServers = errors.New("no servers available for connection")
)

func NewPool(activeCount int, driverName string, addrList *AddrList) (Pool, error) {
	var (
		addrIndex    = make(map[uint][]*DBAddr)
		priorityList = make([]uint, 0)
	)

	log.Println(addrList)

	for _, addr := range addrList.list {
		var exists bool

		for _, p := range priorityList {
			if p == addr.Priority {
				exists = true

				break
			}
		}

		if !exists {
			addrIndex[addr.Priority] = []*DBAddr{addr}

			priorityList = append(priorityList, addr.Priority)
		} else {
			addrIndex[addr.Priority] = append(addrIndex[addr.Priority], addr)
		}
	}

	sort.Slice(priorityList, func(i, j int) bool {
		return priorityList[i] < priorityList[j]
	})

	pool := &ClusterPool{
		clients: make([][]*NodeDB, len(priorityList)),
	}

	var connected int

	for idx, p := range priorityList {
		for _, addr := range addrIndex[p] {
			db, err := NewNodeDB(driverName, addr)
			if err != nil {
				return nil, err
			}

			if connected < activeCount {
				log.Println("connect ", idx, addr.Addr)

				if err := db.Connect(); err != nil {
					return nil, err
				}

				connected++
			}

			pool.clients[idx] = append(pool.clients[idx], db)
		}
	}

	return pool, nil
}

type ClusterPool struct {
	clients [][]*NodeDB
}

func (p *ClusterPool) NextLive() (*NodeDB, error) {
	return p.next(isLive)
}

func (p *ClusterPool) NextDead() (*NodeDB, error) {
	return p.next(isDead)
}

func (p *ClusterPool) NextPending() (*NodeDB, error) {
	return p.next(isPending)
}

func (p *ClusterPool) next(status int32) (*NodeDB, error) {
	clients := p.clients

	for _, list := range clients {
		var (
			minClient *NodeDB

			minWeight int32 = math.MaxInt32
			minTime   int64 = math.MaxInt64
		)

		for _, client := range list {
			if atomic.LoadInt32(&client.status) != status {
				continue
			}

			var (
				weight  = client.ActiveRequests() * reqMultiplier / client.weight
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

		return minClient, nil
	}

	return nil, ErrNoAvailableClients
}
