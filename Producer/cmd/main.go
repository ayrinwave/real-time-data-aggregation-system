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

	"real-time_data_aggregation_system/Producer/internal/api"
	"real-time_data_aggregation_system/Producer/internal/config"
	"real-time_data_aggregation_system/Producer/internal/generator"
	"real-time_data_aggregation_system/Producer/internal/metrics"
	"real-time_data_aggregation_system/Producer/internal/producer"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
	version    = "1.0.0"
)

func main() {
	flag.Parse()

	log.Printf("Starting Event Producer v%s", version)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Printf("Failed to load config, using defaults: %v", err)
		cfg = config.DefaultConfig()
	}

	log.Printf("Configuration loaded successfully")
	log.Printf("Kafka brokers: %v", cfg.Kafka.Brokers)
	log.Printf("Topic: %s", cfg.Kafka.Topic)
	log.Printf("Default mode: %s", cfg.Producer.DefaultMode)

	m := metrics.NewMetrics()
	m.SetMode(cfg.Producer.DefaultMode)
	log.Printf("Metrics initialized")

	kafkaProducer, err := producer.NewKafkaProducer(&cfg.Kafka)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	m.SetKafkaConnected(true)
	log.Printf("Kafka producer created successfully")

	gen := generator.NewGenerator(&cfg.Producer)
	log.Printf("Event generator created")

	router := producer.NewStrategyRouter(
		kafkaProducer,
		cfg.Producer.BatchSize,
		cfg.Producer.BatchTimeout,
	)
	log.Printf("Strategy router created")

	httpHandler := api.NewHandler(gen, kafkaProducer)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := gen.Start(ctx); err != nil {
		log.Fatalf("Failed to start generator: %v", err)
	}
	log.Printf("Event generator started")

	go gen.AutoSwitchMode(ctx)
	log.Printf("Auto mode switching enabled")

	go router.ProcessEvents(ctx, gen.EventChannel())
	log.Printf("Event processing started")

	go logStatistics(ctx, gen, kafkaProducer, m)

	log.Printf("Producer is running. Press Ctrl+C to stop.")
	log.Printf("Available endpoints:")
	log.Printf("  - POST /config/speed?eps=<number>  - Change events per second")
	log.Printf("  - POST /config/mode?mode=<mode>    - Change mode (regular/peak/night)")
	log.Printf("  - POST /generate/burst?count=<n>   - Generate burst of N events")
	log.Printf("  - GET  /status                     - Get current status")
	log.Printf("  - GET  /health                     - Health check")
	log.Printf("  - GET  /metrics                    - Prometheus metrics")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("\nShutdown signal received, gracefully shutting down...")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	if err := kafkaProducer.FlushPendingBatch(); err != nil {
		log.Printf("Error flushing final batch: %v", err)
	}

	if err := kafkaProducer.Close(); err != nil {
		log.Printf("Error closing Kafka producer: %v", err)
	}

	log.Println("Shutdown complete")
}

func logStatistics(ctx context.Context, gen *generator.Generator, prod *producer.KafkaProducer, m *metrics.Metrics) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			genStats := gen.GetStats()
			prodStats := prod.GetStats()

			log.Printf("=== Statistics ===")
			log.Printf("Mode: %s | EPS: %d", gen.GetMode(), gen.GetEventsPerSecond())
			log.Printf("Generated - Total: %d | Normal: %d | Bounce: %d | Error: %d | Duplicate: %d",
				genStats.TotalGenerated,
				genStats.NormalEvents,
				genStats.BounceEvents,
				genStats.ErrorEvents,
				genStats.DuplicateEvents,
			)
			log.Printf("Producer - Sent: %d | Success: %d | Failed: %d | Retries: %d",
				prodStats.SentTotal,
				prodStats.SentSuccess,
				prodStats.SentFailed,
				prodStats.RetriesTotal,
			)
			log.Printf("Last Send Latency: %v", prodStats.LastSendLatency)
			log.Printf("==================")

			m.SetMode(string(gen.GetMode()))

			m.RecordEventGenerated(string(gen.GetMode()), "normal")
			m.RecordEventGenerated(string(gen.GetMode()), "bounce")
			m.RecordEventGenerated(string(gen.GetMode()), "error")
		}
	}
}
