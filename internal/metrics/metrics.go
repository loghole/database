package metrics

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

//nolint:gochecknoglobals // singleton object.
var (
	_metrics     *Metrics
	_metricsMu   sync.Mutex
	_metricsOnce sync.Once
)

type Metrics struct {
	queryDuration        *prometheus.SummaryVec
	serializationFailure *prometheus.CounterVec
}

func NewMetrics() (*Metrics, error) {
	_metricsMu.Lock()
	defer _metricsMu.Unlock()

	var err error

	_metricsOnce.Do(func() {
		_metrics = &Metrics{
			queryDuration:        queryDurationSummaryVec(),
			serializationFailure: serializationFailureCounterVec(),
		}

		if perr := prometheus.Register(_metrics.queryDuration); perr != nil {
			err = fmt.Errorf("register 'query_duration' metric: %w", perr)

			return
		}

		if perr := prometheus.Register(_metrics.serializationFailure); perr != nil {
			err = fmt.Errorf("register 'serialization_failure' metric: %w", perr)

			return
		}
	})

	return _metrics, err
}

func (m *Metrics) SerializationFailureInc(dbType, dbAddr, dbName string) {
	m.serializationFailure.With(prometheus.Labels{
		"db_type": dbType,
		"db_addr": dbAddr,
		"db_name": dbName,
	}).Inc()
}

func (m *Metrics) QueryDurationObserve(
	dbType,
	dbAddr,
	dbName,
	operation,
	table string,
	isError bool,
	since time.Duration,
) {
	m.queryDuration.With(prometheus.Labels{
		"db_type":   dbType,
		"db_addr":   dbAddr,
		"db_name":   dbName,
		"is_error":  strconv.FormatBool(isError),
		"operation": operation,
		"table":     table,
	}).Observe(float64(since) / float64(time.Millisecond))
}

//nolint:promlinter // skip milliseconds.
func queryDurationSummaryVec() *prometheus.SummaryVec {
	return prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "sql_query_duration_milliseconds",
			Help:       "Summary of response time for SQL queries (milliseconds)",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:gomnd // it's ok
		},
		[]string{"db_type", "db_addr", "db_name", "is_error", "operation", "table"},
	)
}

func serializationFailureCounterVec() *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sql_serialization_failure_errors_total",
			Help: "SQL transaction serialization failure count",
		},
		[]string{"db_type", "db_addr", "db_name"},
	)
}
