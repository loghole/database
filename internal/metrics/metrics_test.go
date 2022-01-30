package metrics

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics(t *testing.T) {
	got1, err := NewMetrics()
	require.NoError(t, err)

	got2, err := NewMetrics()
	require.NoError(t, err)

	assert.Equal(t, fmt.Sprintf("%p", got1), fmt.Sprintf("%p", got2), "addr not equal")
}

func TestMetrics_QueryDurationObserve(t *testing.T) {
	type fields struct {
		queryDuration        *prometheus.SummaryVec
		serializationFailure *prometheus.CounterVec
	}
	type args struct {
		dbType    string
		dbAddr    string
		dbName    string
		operation string
		isError   bool
		since     time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "pass",
			fields: fields{
				queryDuration: queryDurationSummaryVec(),
			},
			args: args{
				dbType:    "1",
				dbAddr:    "2",
				dbName:    "3",
				operation: "4",
				isError:   true,
				since:     time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metrics{
				queryDuration: tt.fields.queryDuration,
			}
			m.QueryDurationObserve(tt.args.dbType, tt.args.dbAddr, tt.args.dbName, tt.args.operation, tt.args.isError, tt.args.since)
		})
	}
}

func TestMetrics_SerializationFailureInc(t *testing.T) {
	type fields struct {
		queryDuration        *prometheus.SummaryVec
		serializationFailure *prometheus.CounterVec
	}
	type args struct {
		dbType string
		dbAddr string
		dbName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "pass",
			fields: fields{
				serializationFailure: serializationFailureCounterVec(),
			},
			args: args{
				dbType: "1",
				dbAddr: "2",
				dbName: "3",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metrics{
				queryDuration:        tt.fields.queryDuration,
				serializationFailure: tt.fields.serializationFailure,
			}
			m.SerializationFailureInc(tt.args.dbType, tt.args.dbAddr, tt.args.dbName)
		})
	}
}

func TestNewMetrics1(t *testing.T) {
	tests := []struct {
		name    string
		want    *Metrics
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMetrics()
			if !tt.wantErr(t, err, fmt.Sprintf("NewMetrics()")) {
				return
			}
			assert.Equalf(t, tt.want, got, "NewMetrics()")
		})
	}
}
