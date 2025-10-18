package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
)

var (
    batches = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "cs_batches_total",
            Help: "Total number of ClickHouse insert batches",
        },
        []string{"pipelineId"},
    )
    rows = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "cs_rows_total",
            Help: "Total number of rows inserted into ClickHouse",
        },
        []string{"pipelineId"},
    )
    insertLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "cs_insert_latency_seconds",
            Help:    "ClickHouse insert latency in seconds",
            Buckets: prometheus.DefBuckets,
        },
        []string{"pipelineId"},
    )
    errors = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "cs_errors_total",
            Help: "Total number of errors by kind",
        },
        []string{"pipelineId", "kind"},
    )
    rebalances = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "cs_rebalances_total",
            Help: "Total number of consumer partition assignment events (proxy for rebalances)",
        },
        []string{"pipelineId"},
    )
    activeAssignments = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "cs_active_assignments",
            Help: "Current number of active assignments (replica slots) per pipeline",
        },
        []string{"pipelineId"},
    )
)

func init() {
    prometheus.MustRegister(batches, rows, insertLatency, errors, rebalances, activeAssignments)
}

func IncBatch(pipelineID string) {
    batches.WithLabelValues(pipelineID).Inc()
}

func AddRows(pipelineID string, n int) {
    if n > 0 {
        rows.WithLabelValues(pipelineID).Add(float64(n))
    }
}

func ObserveInsertLatency(pipelineID string, seconds float64) {
    insertLatency.WithLabelValues(pipelineID).Observe(seconds)
}

func IncError(pipelineID, kind string) {
    errors.WithLabelValues(pipelineID, kind).Inc()
}

func IncRebalance(pipelineID string) {
    rebalances.WithLabelValues(pipelineID).Inc()
}

func SetActiveAssignments(pipelineID string, n int) {
    activeAssignments.WithLabelValues(pipelineID).Set(float64(n))
}
