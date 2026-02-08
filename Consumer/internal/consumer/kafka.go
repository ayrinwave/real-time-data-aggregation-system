package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"real-time_data_aggregation_system/Consumer/internal/config"

	"github.com/IBM/sarama"
)

type KafkaConsumer struct {
	cfg           *config.KafkaConfig
	consumerGroup sarama.ConsumerGroup

	messageChan chan *Message

	stats   ConsumerStats
	statsMu sync.RWMutex
}

type Message struct {
	Value     []byte
	Offset    int64
	Partition int32
	Timestamp time.Time
	Topic     string
}

type ConsumerStats struct {
	MessagesConsumed int64
	LastConsumeTime  time.Time
	CurrentLag       int64
	Errors           int64
}

func NewKafkaConsumer(cfg *config.KafkaConfig) (*KafkaConsumer, error) {
	saramaConfig := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Kafka version: %w", err)
	}
	saramaConfig.Version = version

	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	if cfg.AutoOffsetReset == "latest" {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	saramaConfig.Consumer.Offsets.AutoCommit.Enable = cfg.EnableAutoCommit

	saramaConfig.Consumer.Group.Session.Timeout = cfg.SessionTimeout
	saramaConfig.Consumer.Group.Heartbeat.Interval = cfg.HeartbeatInterval

	saramaConfig.Consumer.Fetch.Min = int32(cfg.FetchMinBytes)
	saramaConfig.Consumer.Fetch.Max = int32(cfg.FetchMaxBytes)
	saramaConfig.Consumer.Fetch.Default = int32(cfg.FetchMaxBytes)
	saramaConfig.Consumer.MaxProcessingTime = cfg.MaxProcessingTime

	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}

	consumerGroup, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	return &KafkaConsumer{
		cfg:           cfg,
		consumerGroup: consumerGroup,
		messageChan:   make(chan *Message, 10000),
	}, nil
}

func (kc *KafkaConsumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka consumer, group: %s, topic: %s", kc.cfg.GroupID, kc.cfg.Topic)

	handler := &consumerGroupHandler{
		messageChan: kc.messageChan,
		consumer:    kc,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Consumer context cancelled, stopping...")
				return
			default:

				if err := kc.consumerGroup.Consume(ctx, []string{kc.cfg.Topic}, handler); err != nil {
					log.Printf("Error from consumer: %v", err)
					kc.incrementStat("errors")
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()

	go func() {
		for err := range kc.consumerGroup.Errors() {
			log.Printf("Consumer error: %v", err)
			kc.incrementStat("errors")
		}
	}()

	return nil
}

func (kc *KafkaConsumer) MessageChannel() <-chan *Message {
	return kc.messageChan
}

func (kc *KafkaConsumer) Close() error {
	close(kc.messageChan)
	return kc.consumerGroup.Close()
}

func (kc *KafkaConsumer) GetStats() ConsumerStats {
	kc.statsMu.RLock()
	defer kc.statsMu.RUnlock()
	return kc.stats
}

func (kc *KafkaConsumer) incrementStat(stat string) {
	kc.statsMu.Lock()
	defer kc.statsMu.Unlock()

	switch stat {
	case "consumed":
		kc.stats.MessagesConsumed++
		kc.stats.LastConsumeTime = time.Now()
	case "errors":
		kc.stats.Errors++
	}
}

type consumerGroupHandler struct {
	messageChan chan<- *Message
	consumer    *KafkaConsumer

	session   sarama.ConsumerGroupSession
	sessionMu sync.RWMutex
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.sessionMu.Lock()
	h.session = session
	h.sessionMu.Unlock()

	log.Printf("Consumer group session started, generation: %d", session.GenerationID())
	return nil
}

func (h *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("Consumer group session ended")
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("Started consuming partition %d from offset %d", claim.Partition(), claim.InitialOffset())

	for {
		select {
		case <-session.Context().Done():
			return nil

		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			message := &Message{
				Value:     msg.Value,
				Offset:    msg.Offset,
				Partition: msg.Partition,
				Timestamp: msg.Timestamp,
				Topic:     msg.Topic,
			}

			select {
			case h.messageChan <- message:
				h.consumer.incrementStat("consumed")
			default:
				log.Printf("Message channel full, dropping message (partition=%d, offset=%d)",
					msg.Partition, msg.Offset)
			}

		}
	}
}

func (h *consumerGroupHandler) GetSession() sarama.ConsumerGroupSession {
	h.sessionMu.RLock()
	defer h.sessionMu.RUnlock()
	return h.session
}

type OffsetManager struct {
	session sarama.ConsumerGroupSession
	mu      sync.Mutex
}

func NewOffsetManager(session sarama.ConsumerGroupSession) *OffsetManager {
	return &OffsetManager{
		session: session,
	}
}

func (om *OffsetManager) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.session != nil {
		om.session.MarkOffset(topic, partition, offset+1, metadata)
	}
}

func (om *OffsetManager) Commit() {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.session != nil {
		om.session.Commit()
	}
}
