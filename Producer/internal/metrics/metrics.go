package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	EventsGenerated *prometheus.CounterVec
	EventsType      *prometheus.CounterVec

	MessagesSent *prometheus.CounterVec
	SendLatency  prometheus.Histogram
	SendErrors   *prometheus.CounterVec
	Retries      prometheus.Counter

	BatchSize    prometheus.Histogram
	BatchFlushes prometheus.Counter

	KafkaConnected prometheus.Gauge

	CurrentMode *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	return &Metrics{
		EventsGenerated: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "producer_events_generated_total",
				Help: "Total number of events generated",
			},
			[]string{"mode"},
		),

		EventsType: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "producer_events_by_type_total",
				Help: "Total number of events by type",
			},
			[]string{"type"},
		),

		MessagesSent: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "producer_messages_sent_total",
				Help: "Total number of messages sent to Kafka",
			},
			[]string{"status", "strategy"},
		),

		SendLatency: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "producer_send_latency_seconds",
				Help:    "Latency of sending messages to Kafka",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
			},
		),

		SendErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "producer_send_errors_total",
				Help: "Total number of send errors",
			},
			[]string{"error_type"},
		),

		Retries: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "producer_retries_total",
				Help: "Total number of retry attempts",
			},
		),

		BatchSize: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "producer_batch_size",
				Help:    "Size of batches sent to Kafka",
				Buckets: prometheus.LinearBuckets(10, 10, 20),
			},
		),

		BatchFlushes: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "producer_batch_flushes_total",
				Help: "Total number of batch flushes",
			},
		),

		KafkaConnected: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "producer_kafka_connected",
				Help: "Whether producer is connected to Kafka (1=connected, 0=disconnected)",
			},
		),

		CurrentMode: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "producer_current_mode",
				Help: "Current generation mode (1 if active, 0 otherwise)",
			},
			[]string{"mode"},
		),
	}
}

func (m *Metrics) RecordEventGenerated(mode, eventType string) {
	m.EventsGenerated.WithLabelValues(mode).Inc()
	m.EventsType.WithLabelValues(eventType).Inc()
}

func (m *Metrics) RecordMessageSent(status, strategy string) {
	m.MessagesSent.WithLabelValues(status, strategy).Inc()
}

func (m *Metrics) RecordSendLatency(seconds float64) {
	m.SendLatency.Observe(seconds)
}

func (m *Metrics) RecordSendError(errorType string) {
	m.SendErrors.WithLabelValues(errorType).Inc()
}

func (m *Metrics) RecordRetry() {
	m.Retries.Inc()
}

func (m *Metrics) RecordBatchFlush(size int) {
	m.BatchSize.Observe(float64(size))
	m.BatchFlushes.Inc()
}

func (m *Metrics) SetKafkaConnected(connected bool) {
	if connected {
		m.KafkaConnected.Set(1)
	} else {
		m.KafkaConnected.Set(0)
	}
}

func (m *Metrics) SetMode(mode string) {

	for _, modeStr := range []string{"regular", "peak", "night"} {
		m.CurrentMode.WithLabelValues(modeStr).Set(0)
	}

	m.CurrentMode.WithLabelValues(mode).Set(1)
}
