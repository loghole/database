package metrics

import (
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	queryDuration *prometheus.SummaryVec
	activeTxCount *prometheus.GaugeVec
	retryTxCount  *prometheus.CounterVec
}

// nolint:promlinter // skip milliseconds.
func NewMetrics() (*Metrics, error) {
	metrics := &Metrics{
		queryDuration: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name:       "sql_query_duration_milliseconds",
				Help:       "Summary of response time for SQL queries (milliseconds)",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, // nolint:gomnd // it's ok
			},
			[]string{"db_type", "db_addr", "db_name", "is_error", "operation"},
		),
		activeTxCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "sql_active_transactions_count",
				Help: "Active transactions count to SQL database",
			},
			[]string{"db_type", "db_addr", "db_name"},
		),
		retryTxCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sql_serialization_failure_errors_total",
				Help: "SQL transaction serialization failure count",
			},
			[]string{"db_type", "db_addr", "db_name"},
		),
	}

	if err := prometheus.Register(metrics.queryDuration); err != nil {
		return nil, fmt.Errorf("register 'response time': %w", err)
	}

	if err := prometheus.Register(metrics.activeTxCount); err != nil {
		return nil, fmt.Errorf("register 'active tx count': %w", err)
	}

	if err := prometheus.Register(metrics.retryTxCount); err != nil {
		return nil, fmt.Errorf("register 'retry tx count': %w", err)
	}

	return metrics, nil
}

func (m *Metrics) ActiveTxInc(dbType, dbAddr, dbName string) {
	m.activeTxCount.With(prometheus.Labels{
		"db_type": dbType,
		"db_addr": dbAddr,
		"db_name": dbName,
	}).Inc()
}

func (m *Metrics) ActiveTxDec(dbType, dbAddr, dbName string) {
	m.activeTxCount.With(prometheus.Labels{
		"db_type": dbType,
		"db_addr": dbAddr,
		"db_name": dbName,
	}).Dec()
}

func (m *Metrics) SerializationFailureInc(dbType, dbAddr, dbName string) {
	m.retryTxCount.With(prometheus.Labels{
		"db_type": dbType,
		"db_addr": dbAddr,
		"db_name": dbName,
	}).Inc()
}

func (m *Metrics) QueryDurationObserve(dbType, dbAddr, dbName, operation string, isError bool, since time.Duration) {
	m.queryDuration.With(prometheus.Labels{
		"db_type":   dbType,
		"db_addr":   dbAddr,
		"db_name":   dbName,
		"is_error":  strconv.FormatBool(isError),
		"operation": operation,
	}).Observe(float64(since) / float64(time.Millisecond))
}
