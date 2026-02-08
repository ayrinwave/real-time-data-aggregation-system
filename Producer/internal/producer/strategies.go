package producer

import (
	"context"
	"log"
	"math/rand"
	"time"

	"real-time_data_aggregation_system/Producer/internal/generator"
)

type PartitionStrategy int

const (
	PartitionByKey PartitionStrategy = iota
	PartitionRoundRobin
	PartitionRandom
)

type StrategyRouter struct {
	producer     *KafkaProducer
	batchSize    int
	batchTimeout time.Duration
}

func NewStrategyRouter(producer *KafkaProducer, batchSize int, batchTimeout time.Duration) *StrategyRouter {
	return &StrategyRouter{
		producer:     producer,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
	}
}

func (sr *StrategyRouter) RouteEvent(event *generator.PageViewEvent) error {

	strategy := sr.selectSendStrategy()
	partition := sr.selectPartitionStrategy()

	sr.producer.SetPartitionStrategy(partition)

	switch strategy {
	case SyncSend:
		return sr.producer.SendSync(event)

	case AsyncSend:
		return sr.producer.SendAsync(event)

	case BatchSend:
		return sr.producer.SendBatch(event, sr.batchSize, sr.batchTimeout)

	default:
		return sr.producer.SendSync(event)
	}
}

func (sr *StrategyRouter) selectSendStrategy() SendStrategy {
	roll := rand.Float64()

	if roll < 0.30 {
		return SyncSend
	} else if roll < 0.70 {
		return AsyncSend
	}
	return BatchSend
}

func (sr *StrategyRouter) selectPartitionStrategy() PartitionStrategy {
	roll := rand.Float64()

	if roll < 0.70 {
		return PartitionByKey
	} else if roll < 0.90 {
		return PartitionRoundRobin
	}
	return PartitionRandom
}

func (sr *StrategyRouter) ProcessEvents(ctx context.Context, eventChan <-chan *generator.PageViewEvent) {

	flushTicker := time.NewTicker(sr.batchTimeout)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping event processing")

			if err := sr.producer.FlushPendingBatch(); err != nil {
				log.Printf("Error flushing final batch: %v", err)
			}
			return

		case event, ok := <-eventChan:
			if !ok {
				log.Println("Event channel closed")
				return
			}

			if err := sr.RouteEvent(event); err != nil {
				log.Printf("Error routing event: %v", err)
			}

		case <-flushTicker.C:

			if err := sr.producer.FlushPendingBatch(); err != nil {
				log.Printf("Error flushing batch: %v", err)
			}
		}
	}
}

type SendStrategyStats struct {
	SyncCount  int64
	AsyncCount int64
	BatchCount int64
}

type PartitionStrategyStats struct {
	ByKeyCount      int64
	RoundRobinCount int64
	RandomCount     int64
}
