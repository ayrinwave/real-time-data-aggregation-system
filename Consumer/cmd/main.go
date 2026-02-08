package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"real-time_data_aggregation_system/Consumer/internal/api"
	"real-time_data_aggregation_system/Consumer/internal/clickhouse"
	"real-time_data_aggregation_system/Consumer/internal/config"
	"real-time_data_aggregation_system/Consumer/internal/consumer"
	"real-time_data_aggregation_system/Consumer/internal/metrics"
	"real-time_data_aggregation_system/Consumer/internal/validator"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
	initSchema = flag.Bool("init-schema", false, "Initialize ClickHouse schema and exit")
	dropSchema = flag.Bool("drop-schema", false, "Drop ClickHouse schema and exit")
	version    = "1.0.0"
)

func main() {
	flag.Parse()

	log.Printf("Starting Event Consumer v%s", version)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Printf("Failed to load config, using defaults: %v", err)
		cfg = config.DefaultConfig()
	}

	log.Printf("Configuration loaded successfully")
	log.Printf("Kafka brokers: %v", cfg.Kafka.Brokers)
	log.Printf("Kafka topic: %s", cfg.Kafka.Topic)
	log.Printf("Consumer group: %s", cfg.Kafka.GroupID)
	log.Printf("ClickHouse: %v", cfg.ClickHouse.Addresses)
	log.Printf("Processing mode: %s", cfg.Consumer.Mode)
	log.Printf("Batch size: %d", cfg.Consumer.BatchSize)
	log.Printf("Batch timeout: %v", cfg.Consumer.BatchTimeout)
	log.Printf("Delivery guarantee: %s", cfg.Consumer.DeliveryGuarantee)

	m := metrics.NewMetrics()
	log.Printf("Metrics initialized")

	chClient, err := clickhouse.NewClient(&cfg.ClickHouse)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer chClient.Close()

	m.SetClickHouseConnected(true)
	log.Printf("Connected to ClickHouse")

	ctx := context.Background()

	if *dropSchema {
		log.Println("Dropping schema...")
		if err := chClient.DropSchema(ctx); err != nil {
			log.Fatalf("Failed to drop schema: %v", err)
		}
		log.Println("Schema dropped successfully")
		return
	}

	if *initSchema {
		log.Println("Initializing schema...")
		if err := chClient.InitSchema(ctx); err != nil {
			log.Fatalf("Failed to initialize schema: %v", err)
		}
		log.Println("Schema initialized successfully")
		return
	}

	if err := chClient.VerifySchema(ctx); err != nil {
		log.Printf("Schema verification failed: %v", err)
		log.Println("Initializing schema...")
		if err := chClient.InitSchema(ctx); err != nil {
			log.Fatalf("Failed to initialize schema: %v", err)
		}
	}

	valid := validator.NewValidator()
	log.Printf("Validator created")

	writer := clickhouse.NewWriter(chClient)
	log.Printf("ClickHouse writer created")

	kafkaConsumer, err := consumer.NewKafkaConsumer(&cfg.Kafka)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer kafkaConsumer.Close()

	m.SetKafkaConnected(true)
	log.Printf("Kafka consumer created")

	proc := consumer.NewProcessor(&cfg.Consumer, valid, writer)
	log.Printf("Message processor created")

	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := kafkaConsumer.Start(appCtx); err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}
	log.Printf("Kafka consumer started")

	go proc.Start(appCtx)
	log.Printf("Processor started")

	go processMessages(appCtx, kafkaConsumer, proc, m)
	log.Printf("Message processing started")

	go logStatistics(appCtx, kafkaConsumer, proc, valid, writer)

	httpHandler := api.NewHandler(kafkaConsumer, proc, valid, writer, chClient)
	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: mux,
	}

	go func() {
		log.Printf("HTTP server listening on :%d", cfg.Server.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	log.Printf("Consumer is running. Press Ctrl+C to stop.")
	log.Printf("Available endpoints:")
	log.Printf("  - GET  /status                    - Get current status")
	log.Printf("  - GET  /health                    - Health check")
	log.Printf("  - POST /flush                     - Manual flush")
	log.Printf("  - GET  /query/aggregates?hours=1  - Query aggregates")
	log.Printf("  - GET  /query/errors?limit=10     - Query DLQ errors")
	log.Printf("  - GET  /metrics                   - Prometheus metrics")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("\nShutdown signal received, gracefully shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	shutdownCancel()

	log.Println("Flushing remaining messages...")
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)

	if err := proc.Flush(flushCtx); err != nil {
		log.Printf("Error during final flush: %v", err)
	}
	flushCancel()

	log.Println("Shutdown complete")
}

func processMessages(
	ctx context.Context,
	kafkaConsumer *consumer.KafkaConsumer,
	proc *consumer.Processor,
	m *metrics.Metrics,
) {
	messageChan := kafkaConsumer.MessageChannel()

	for {
		select {
		case <-ctx.Done():
			return

		case msg, ok := <-messageChan:
			if !ok {
				return
			}

			start := time.Now()

			if err := proc.ProcessMessage(ctx, msg); err != nil {
				log.Printf("Error processing message: %v", err)
			}

			m.RecordProcessingLatency(time.Since(start).Seconds())
		}
	}
}

func logStatistics(
	ctx context.Context,
	kafkaConsumer *consumer.KafkaConsumer,
	proc *consumer.Processor,
	valid *validator.Validator,
	writer *clickhouse.Writer,
) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			consumerStats := kafkaConsumer.GetStats()
			processorStats := proc.GetStats()
			validatorStats := valid.GetStats()
			writerStats := writer.GetStats()

			log.Printf("=== Statistics ===")
			log.Printf("Consumer - Consumed: %d | Lag: %d | Errors: %d",
				consumerStats.MessagesConsumed,
				consumerStats.CurrentLag,
				consumerStats.Errors,
			)
			log.Printf("Processor - Processed: %d | Valid: %d | Invalid: %d | Flushes: %d",
				processorStats.MessagesProcessed,
				processorStats.ValidMessages,
				processorStats.InvalidMessages,
				processorStats.FlushCount,
			)
			log.Printf("Validator - Total: %d | Valid: %d | Invalid: %d",
				validatorStats["total"],
				validatorStats["valid"],
				validatorStats["invalid"],
			)
			log.Printf("Writer - Raw: %d | Errors (DLQ): %d | Failed: %d",
				writerStats.RawInserted,
				writerStats.ErrorsInserted,
				writerStats.InsertsFailed,
			)
			log.Printf("==================")
		}
	}
}
