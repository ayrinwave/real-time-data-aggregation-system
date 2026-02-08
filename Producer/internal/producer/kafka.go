package producer

import (
	"fmt"
	"log"
	"sync"
	"time"

	"real-time_data_aggregation_system/Producer/internal/config"
	"real-time_data_aggregation_system/Producer/internal/generator"

	"github.com/IBM/sarama"
)

type SendStrategy int

const (
	SyncSend SendStrategy = iota
	AsyncSend
	BatchSend
)

type KafkaProducer struct {
	cfg           *config.KafkaConfig
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer

	batchBuffer   []*sarama.ProducerMessage
	batchMu       sync.Mutex
	lastBatchSend time.Time

	partitioner PartitionStrategy

	stats   ProducerStats
	statsMu sync.RWMutex
}

type ProducerStats struct {
	SentTotal       int64
	SentSuccess     int64
	SentFailed      int64
	RetriesTotal    int64
	LastSendLatency time.Duration
}

func NewKafkaProducer(cfg *config.KafkaConfig) (*KafkaProducer, error) {
	saramaConfig := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Kafka version: %w", err)
	}
	saramaConfig.Version = version

	saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(cfg.RequiredAcks)
	saramaConfig.Producer.Retry.Max = cfg.RetryMax
	saramaConfig.Producer.Retry.Backoff = cfg.RetryBackoff
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.MaxMessageBytes = cfg.MaxMessageBytes

	switch cfg.Compression {
	case "snappy":
		saramaConfig.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		saramaConfig.Producer.Compression = sarama.CompressionLZ4
	case "gzip":
		saramaConfig.Producer.Compression = sarama.CompressionGZIP
	default:
		saramaConfig.Producer.Compression = sarama.CompressionNone
	}

	syncProducer, err := sarama.NewSyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer: %w", err)
	}

	asyncProducer, err := sarama.NewAsyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		syncProducer.Close()
		return nil, fmt.Errorf("failed to create async producer: %w", err)
	}

	kp := &KafkaProducer{
		cfg:           cfg,
		syncProducer:  syncProducer,
		asyncProducer: asyncProducer,
		batchBuffer:   make([]*sarama.ProducerMessage, 0, 1000),
		lastBatchSend: time.Now(),
		partitioner:   PartitionByKey,
	}

	go kp.handleAsyncResults()

	return kp, nil
}

func (kp *KafkaProducer) Close() error {
	var err error

	if len(kp.batchBuffer) > 0 {
		kp.flushBatch()
	}

	if kp.syncProducer != nil {
		if e := kp.syncProducer.Close(); e != nil {
			err = e
		}
	}

	if kp.asyncProducer != nil {
		if e := kp.asyncProducer.Close(); e != nil {
			err = e
		}
	}

	return err
}

func (kp *KafkaProducer) SendSync(event *generator.PageViewEvent) error {
	start := time.Now()
	defer func() {
		kp.updateLatency(time.Since(start))
	}()

	msg, err := kp.prepareMessage(event)
	if err != nil {
		kp.incrementStat("failed")
		return fmt.Errorf("failed to prepare message: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= kp.cfg.RetryMax; attempt++ {
		if attempt > 0 {

			backoff := kp.cfg.RetryBackoff * time.Duration(1<<uint(attempt-1))
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			time.Sleep(backoff)
			kp.incrementStat("retries")
			log.Printf("Retry attempt %d/%d after %v", attempt, kp.cfg.RetryMax, backoff)
		}

		partition, offset, err := kp.syncProducer.SendMessage(msg)
		if err == nil {
			kp.incrementStat("success")
			log.Printf("Message sent successfully: partition=%d, offset=%d", partition, offset)
			return nil
		}

		lastErr = err
		log.Printf("Send failed (attempt %d): %v", attempt+1, err)
	}

	kp.incrementStat("failed")
	return fmt.Errorf("failed after %d retries: %w", kp.cfg.RetryMax, lastErr)
}

func (kp *KafkaProducer) SendAsync(event *generator.PageViewEvent) error {
	msg, err := kp.prepareMessage(event)
	if err != nil {
		kp.incrementStat("failed")
		return fmt.Errorf("failed to prepare message: %w", err)
	}

	select {
	case kp.asyncProducer.Input() <- msg:
		kp.incrementStat("sent")
		return nil
	default:
		kp.incrementStat("failed")
		return fmt.Errorf("async producer input channel full")
	}
}

func (kp *KafkaProducer) SendBatch(event *generator.PageViewEvent, batchSize int, batchTimeout time.Duration) error {
	msg, err := kp.prepareMessage(event)
	if err != nil {
		return fmt.Errorf("failed to prepare message: %w", err)
	}

	kp.batchMu.Lock()
	defer kp.batchMu.Unlock()

	kp.batchBuffer = append(kp.batchBuffer, msg)

	shouldFlush := len(kp.batchBuffer) >= batchSize ||
		time.Since(kp.lastBatchSend) >= batchTimeout

	if shouldFlush {
		return kp.flushBatch()
	}

	return nil
}

func (kp *KafkaProducer) flushBatch() error {
	if len(kp.batchBuffer) == 0 {
		return nil
	}

	start := time.Now()
	log.Printf("Flushing batch of %d messages", len(kp.batchBuffer))

	for _, msg := range kp.batchBuffer {
		_, _, err := kp.syncProducer.SendMessage(msg)
		if err != nil {
			kp.incrementStat("failed")
			log.Printf("Batch send error: %v", err)
		} else {
			kp.incrementStat("success")
		}
	}

	kp.batchBuffer = kp.batchBuffer[:0]
	kp.lastBatchSend = time.Now()

	kp.updateLatency(time.Since(start))
	log.Printf("Batch flush completed in %v", time.Since(start))

	return nil
}

func (kp *KafkaProducer) FlushPendingBatch() error {
	kp.batchMu.Lock()
	defer kp.batchMu.Unlock()
	return kp.flushBatch()
}

func (kp *KafkaProducer) SetPartitionStrategy(strategy PartitionStrategy) {
	kp.partitioner = strategy
	log.Printf("Partition strategy changed to: %v", strategy)
}

func (kp *KafkaProducer) prepareMessage(event *generator.PageViewEvent) (*sarama.ProducerMessage, error) {

	value, err := event.ToJSON()
	if err != nil {

		if err.Error() == "invalid" {
			value = event.ToInvalidJSON()
		} else {
			return nil, fmt.Errorf("failed to serialize event: %w", err)
		}
	}

	var key sarama.Encoder
	switch kp.partitioner {
	case PartitionByKey:
		key = sarama.StringEncoder(event.GetMessageKey())
	case PartitionRoundRobin:
		key = nil
	case PartitionRandom:
		key = sarama.StringEncoder(fmt.Sprintf("random_%d", time.Now().UnixNano()))
	}

	msg := &sarama.ProducerMessage{
		Topic: kp.cfg.Topic,
		Key:   key,
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("source"),
				Value: []byte("event-generator"),
			},
			{
				Key:   []byte("version"),
				Value: []byte("1.0"),
			},
		},
		Timestamp: event.Timestamp,
	}

	return msg, nil
}

func (kp *KafkaProducer) handleAsyncResults() {
	for {
		select {
		case success := <-kp.asyncProducer.Successes():
			kp.incrementStat("success")
			if success != nil {
				log.Printf("Async message sent: partition=%d, offset=%d",
					success.Partition, success.Offset)
			}

		case err := <-kp.asyncProducer.Errors():
			kp.incrementStat("failed")
			if err != nil {
				log.Printf("Async send error: %v", err.Err)

			}
		}
	}
}

func (kp *KafkaProducer) GetStats() ProducerStats {
	kp.statsMu.RLock()
	defer kp.statsMu.RUnlock()
	return kp.stats
}

func (kp *KafkaProducer) incrementStat(stat string) {
	kp.statsMu.Lock()
	defer kp.statsMu.Unlock()

	switch stat {
	case "sent":
		kp.stats.SentTotal++
	case "success":
		kp.stats.SentSuccess++
	case "failed":
		kp.stats.SentFailed++
	case "retries":
		kp.stats.RetriesTotal++
	}
}

func (kp *KafkaProducer) updateLatency(latency time.Duration) {
	kp.statsMu.Lock()
	defer kp.statsMu.Unlock()
	kp.stats.LastSendLatency = latency
}
