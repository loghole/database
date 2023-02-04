package helpers

import (
	"errors"

	"github.com/lib/pq"
)

const (
	_pqSerializationFailureCode = "40001"
)

func IsSerialisationFailureErr(err error) bool {
	var pqErrPtr *pq.Error

	switch {
	case errors.As(err, &pqErrPtr):
		return pqErrPtr.Code == _pqSerializationFailureCode
	default:
		return false
	}
}
