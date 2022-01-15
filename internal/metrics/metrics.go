package metrics

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// nolint:gochecknoglobals // singleton object.
var (
	_metrics     *Metrics
	_metricsMu   sync.Mutex
	_metricsOnce sync.Once
)

type Metrics struct {
	queryDuration        *prometheus.SummaryVec
	serializationFailure *prometheus.CounterVec
}

// nolint:promlinter // skip milliseconds.
func NewMetrics() (metrics *Metrics, err error) {
	_metricsMu.Lock()
	defer _metricsMu.Unlock()

	_metricsOnce.Do(func() {
		_metrics = &Metrics{
			queryDuration: prometheus.NewSummaryVec(
				prometheus.SummaryOpts{
					Name:       "sql_query_duration_milliseconds",
					Help:       "Summary of response time for SQL queries (milliseconds)",
					Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, // nolint:gomnd // it's ok
				},
				[]string{"db_type", "db_addr", "db_name", "is_error", "operation"},
			),
			serializationFailure: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: "sql_serialization_failure_errors_total",
					Help: "SQL transaction serialization failure count",
				},
				[]string{"db_type", "db_addr", "db_name"},
			),
		}

		if perr := prometheus.Register(metrics.queryDuration); perr != nil {
			err = fmt.Errorf("register 'query_duration' metric: %w", perr)

			return
		}

		if perr := prometheus.Register(metrics.serializationFailure); perr != nil {
			err = fmt.Errorf("register 'serialization_failure' metric: %w", perr)

			return
		}
	})

	return _metrics, nil
}

func (m *Metrics) SerializationFailureInc(dbType, dbAddr, dbName string) {
	m.serializationFailure.With(prometheus.Labels{
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
