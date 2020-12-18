package hooks

import (
	"net/http"
)

const (
	DatabaseError Code = 2000
	BadConnection Code = 2001
	Reconnected   Code = 2002
)

type Code int

func (c Code) Int() int {
	return int(c)
}

func (c Code) HTTP() int {
	switch c {
	case DatabaseError:
		return http.StatusInternalServerError
	case BadConnection, Reconnected:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

func (c Code) GRPC() int {
	const (
		grpcInternal    = 13
		grpcUnavailable = 14
	)

	switch c {
	case DatabaseError:
		return grpcInternal
	case BadConnection, Reconnected:
		return grpcUnavailable
	default:
		return grpcInternal
	}
}
