package addrlist

import (
	"errors"
	"math"
	"sync/atomic"
)

type Pool interface {
	NextLive() (*NodeDB, error)
	NextDead() (*NodeDB, error)
	NextPending() (*NodeDB, error)
}

const reqMultiplier int32 = 10000

var (
	ErrIsNotPending       = errors.New("is not pending")
	ErrNoAvailableClients = errors.New("no available clients")
	ErrNoAvailableServers = errors.New("no servers available for connection")
)

func NewPool(driverName string, addrList AddrList) (Pool, error) {

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