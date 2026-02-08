package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	MessagesConsumed  *prometheus.CounterVec
	MessagesProcessed *prometheus.CounterVec
	ProcessingLatency prometheus.Histogram

	ValidationResults *prometheus.CounterVec

	ClickHouseInserts *prometheus.CounterVec
	ClickHouseLatency prometheus.Histogram
	DLQInserts        prometheus.Counter

	BufferSize prometheus.Gauge
	FlushCount prometheus.Counter

	ConsumerLag      prometheus.Gauge
	LastCommitOffset *prometheus.GaugeVec

	KafkaConnected      prometheus.Gauge
	ClickHouseConnected prometheus.Gauge
}

func NewMetrics() *Metrics {
	return &Metrics{
		MessagesConsumed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_messages_consumed_total",
				Help: "Total number of messages consumed from Kafka",
			},
			[]string{"partition"},
		),

		MessagesProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_messages_processed_total",
				Help: "Total number of messages processed",
			},
			[]string{"status"},
		),

		ProcessingLatency: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "consumer_processing_latency_seconds",
				Help:    "Latency of message processing",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
			},
		),

		ValidationResults: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_validation_results_total",
				Help: "Total number of validation results",
			},
			[]string{"result"},
		),

		ClickHouseInserts: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_clickhouse_inserts_total",
				Help: "Total number of ClickHouse insert operations",
			},
			[]string{"status", "table"},
		),

		ClickHouseLatency: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "consumer_clickhouse_latency_seconds",
				Help:    "Latency of ClickHouse insert operations",
				Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
			},
		),

		DLQInserts: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "consumer_dlq_inserts_total",
				Help: "Total number of messages inserted into DLQ",
			},
		),

		BufferSize: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "consumer_buffer_size",
				Help: "Current size of message buffer",
			},
		),

		FlushCount: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "consumer_flush_total",
				Help: "Total number of buffer flushes",
			},
		),

		ConsumerLag: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "consumer_lag",
				Help: "Current consumer lag (messages behind)",
			},
		),

		LastCommitOffset: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "consumer_last_commit_offset",
				Help: "Last committed offset per partition",
			},
			[]string{"partition"},
		),

		KafkaConnected: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "consumer_kafka_connected",
				Help: "Whether consumer is connected to Kafka (1=connected, 0=disconnected)",
			},
		),

		ClickHouseConnected: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "consumer_clickhouse_connected",
				Help: "Whether consumer is connected to ClickHouse (1=connected, 0=disconnected)",
			},
		),
	}
}

func (m *Metrics) RecordMessageConsumed(partition string) {
	m.MessagesConsumed.WithLabelValues(partition).Inc()
}

func (m *Metrics) RecordMessageProcessed(status string) {
	m.MessagesProcessed.WithLabelValues(status).Inc()
}

func (m *Metrics) RecordProcessingLatency(seconds float64) {
	m.ProcessingLatency.Observe(seconds)
}

func (m *Metrics) RecordValidation(result string) {
	m.ValidationResults.WithLabelValues(result).Inc()
}

func (m *Metrics) RecordClickHouseInsert(status, table string) {
	m.ClickHouseInserts.WithLabelValues(status, table).Inc()
}

func (m *Metrics) RecordClickHouseLatency(seconds float64) {
	m.ClickHouseLatency.Observe(seconds)
}

func (m *Metrics) RecordDLQInsert() {
	m.DLQInserts.Inc()
}

func (m *Metrics) SetBufferSize(size float64) {
	m.BufferSize.Set(size)
}

func (m *Metrics) RecordFlush() {
	m.FlushCount.Inc()
}

func (m *Metrics) SetConsumerLag(lag float64) {
	m.ConsumerLag.Set(lag)
}

func (m *Metrics) SetLastCommitOffset(partition string, offset float64) {
	m.LastCommitOffset.WithLabelValues(partition).Set(offset)
}

func (m *Metrics) SetKafkaConnected(connected bool) {
	if connected {
		m.KafkaConnected.Set(1)
	} else {
		m.KafkaConnected.Set(0)
	}
}

func (m *Metrics) SetClickHouseConnected(connected bool) {
	if connected {
		m.ClickHouseConnected.Set(1)
	} else {
		m.ClickHouseConnected.Set(0)
	}
}
