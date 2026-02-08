package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"real-time_data_aggregation_system/Consumer/internal/clickhouse"
	"real-time_data_aggregation_system/Consumer/internal/config"
	"real-time_data_aggregation_system/Consumer/internal/validator"

	"github.com/IBM/sarama"
)

type PageViewEvent = validator.PageViewEvent
type ErrorRecord = validator.ErrorRecord

type Processor struct {
	cfg       *config.ConsumerConfig
	validator *validator.Validator
	writer    *clickhouse.Writer

	validEvents  []*PageViewEvent
	validOffsets []clickhouse.KafkaOffset
	errorRecords []*ErrorRecord
	bufferMu     sync.Mutex

	lastFlush time.Time

	session   sarama.ConsumerGroupSession
	sessionMu sync.RWMutex

	stats   ProcessorStats
	statsMu sync.RWMutex
}

type ProcessorStats struct {
	MessagesProcessed int64
	ValidMessages     int64
	InvalidMessages   int64
	FlushCount        int64
	LastFlushTime     time.Time
}

func NewProcessor(cfg *config.ConsumerConfig, validator *validator.Validator, writer *clickhouse.Writer) *Processor {
	return &Processor{
		cfg:          cfg,
		validator:    validator,
		writer:       writer,
		validEvents:  make([]*PageViewEvent, 0, cfg.BatchSize),
		validOffsets: make([]clickhouse.KafkaOffset, 0, cfg.BatchSize),
		errorRecords: make([]*ErrorRecord, 0, 100),
		lastFlush:    time.Now(),
	}
}

func (p *Processor) SetSession(session sarama.ConsumerGroupSession) {
	p.sessionMu.Lock()
	defer p.sessionMu.Unlock()
	p.session = session
}

func (p *Processor) ProcessMessage(ctx context.Context, msg *Message) error {
	p.incrementStat("processed")

	result := p.validator.ValidateJSON(msg.Value)

	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	if result.Valid {

		p.validEvents = append(p.validEvents, result.Event)
		p.validOffsets = append(p.validOffsets, clickhouse.KafkaOffset{
			Partition: msg.Partition,
			Offset:    msg.Offset,
		})
		p.incrementStat("valid")
	} else {

		errorRecord := validator.CreateErrorRecord(msg.Value, result.Errors, msg.Offset, msg.Partition)
		p.errorRecords = append(p.errorRecords, errorRecord)
		p.incrementStat("invalid")

		log.Printf("Invalid message (partition=%d, offset=%d): %s",
			msg.Partition, msg.Offset, errorRecord.ErrorReason)
	}

	shouldFlush := p.shouldFlush()

	if shouldFlush {
		return p.flush(ctx)
	}

	return nil
}

func (p *Processor) shouldFlush() bool {
	switch p.cfg.Mode {
	case "batch":

		return len(p.validEvents) >= p.cfg.BatchSize

	case "time":

		return time.Since(p.lastFlush) >= p.cfg.BatchTimeout

	case "hybrid":

		return len(p.validEvents) >= p.cfg.BatchSize ||
			time.Since(p.lastFlush) >= p.cfg.BatchTimeout

	default:
		return len(p.validEvents) >= p.cfg.BatchSize
	}
}

func (p *Processor) flush(ctx context.Context) error {
	if len(p.validEvents) == 0 && len(p.errorRecords) == 0 {
		return nil
	}

	start := time.Now()
	log.Printf("Flushing: %d valid events, %d errors", len(p.validEvents), len(p.errorRecords))

	validEvents := make([]*PageViewEvent, len(p.validEvents))
	validOffsets := make([]clickhouse.KafkaOffset, len(p.validOffsets))
	errorRecords := make([]*ErrorRecord, len(p.errorRecords))

	copy(validEvents, p.validEvents)
	copy(validOffsets, p.validOffsets)
	copy(errorRecords, p.errorRecords)

	p.validEvents = p.validEvents[:0]
	p.validOffsets = p.validOffsets[:0]
	p.errorRecords = p.errorRecords[:0]
	p.lastFlush = time.Now()

	if len(validEvents) > 0 {
		err := p.insertWithRetry(ctx, func() error {
			return p.writer.InsertRawEvents(ctx, validEvents, validOffsets)
		})

		if err != nil {
			log.Printf("Failed to insert events after retries: %v", err)

			p.validEvents = append(p.validEvents, validEvents...)
			p.validOffsets = append(p.validOffsets, validOffsets...)
			return err
		}
	}

	if len(errorRecords) > 0 {
		err := p.insertWithRetry(ctx, func() error {
			return p.writer.InsertErrors(ctx, errorRecords)
		})

		if err != nil {
			log.Printf("Failed to insert errors to DLQ: %v", err)

			p.errorRecords = append(p.errorRecords, errorRecords...)
			return err
		}
	}

	p.commitOffsets(validOffsets)

	p.statsMu.Lock()
	p.stats.FlushCount++
	p.stats.LastFlushTime = time.Now()
	p.statsMu.Unlock()

	log.Printf("Flush completed in %v", time.Since(start))
	return nil
}

func (p *Processor) insertWithRetry(ctx context.Context, insertFunc func() error) error {
	var lastErr error

	for attempt := 0; attempt <= p.cfg.RetryMax; attempt++ {
		if attempt > 0 {

			backoff := p.cfg.RetryBackoff * time.Duration(1<<uint(attempt-1))
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}

			log.Printf("Retry attempt %d/%d after %v", attempt, p.cfg.RetryMax, backoff)
			time.Sleep(backoff)
		}

		err := insertFunc()
		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("Insert failed (attempt %d): %v", attempt+1, err)
	}

	return fmt.Errorf("insert failed after %d retries: %w", p.cfg.RetryMax, lastErr)
}

func (p *Processor) commitOffsets(offsets []clickhouse.KafkaOffset) {
	p.sessionMu.RLock()
	session := p.session
	p.sessionMu.RUnlock()

	if session == nil {
		log.Printf("No session available for offset commit")
		return
	}

	partitionOffsets := make(map[int32]int64)
	for _, offset := range offsets {
		if existing, ok := partitionOffsets[offset.Partition]; !ok || offset.Offset > existing {
			partitionOffsets[offset.Partition] = offset.Offset
		}
	}

	claims := session.Claims()
	var topic string
	for t := range claims {
		topic = t
		break
	}

	if topic == "" {
		log.Printf("No topic found in session claims")
		return
	}

	for partition, offset := range partitionOffsets {
		session.MarkOffset(topic, partition, offset+1, "")
	}

	session.Commit()

	log.Printf("Committed offsets for %d partitions", len(partitionOffsets))
}

func (p *Processor) Flush(ctx context.Context) error {
	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	return p.flush(ctx)
}

func (p *Processor) GetStats() ProcessorStats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.stats
}

func (p *Processor) incrementStat(stat string) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()

	switch stat {
	case "processed":
		p.stats.MessagesProcessed++
	case "valid":
		p.stats.ValidMessages++
	case "invalid":
		p.stats.InvalidMessages++
	}
}

func (p *Processor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():

			p.bufferMu.Lock()
			p.flush(context.Background())
			p.bufferMu.Unlock()
			return

		case <-ticker.C:
			p.bufferMu.Lock()
			if p.shouldFlush() {
				p.flush(ctx)
			}
			p.bufferMu.Unlock()
		}
	}
}
