package addrlist

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var ErrAddrAlreadyExists = errors.New("addr already exists")

type DBAddr struct {
	Addr     string
	Priority uint
	Weight   uint
}

type AddrList struct {
	mu   sync.Mutex
	list []*DBAddr
}

func (a *AddrList) Add(priority, weight uint, addrs ...string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, addr := range addrs {
		for _, target := range a.list {
			if strings.EqualFold(addr, target.Addr) {
				return fmt.Errorf("%s: %w", addr, ErrAddrAlreadyExists)
			}
		}

		a.list = append(a.list, &DBAddr{
			Addr:     addr,
			Priority: priority,
			Weight:   weight,
		})
	}

	return nil
}

func (a *AddrList) All() []*DBAddr {
	return a.list
}
