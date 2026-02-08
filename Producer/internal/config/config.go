package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete application configuration
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Producer ProducerConfig `yaml:"producer"`
}

// ServerConfig holds HTTP server settings
type ServerConfig struct {
	Port int `yaml:"port"`
}

// KafkaConfig holds Kafka connection settings
type KafkaConfig struct {
	Brokers         []string      `yaml:"brokers"`
	Topic           string        `yaml:"topic"`
	Version         string        `yaml:"version"`       // e.g., "2.8.0"
	Compression     string        `yaml:"compression"`   // snappy, lz4, gzip, none
	RequiredAcks    int16         `yaml:"required_acks"` // -1, 0, 1
	MaxMessageBytes int           `yaml:"max_message_bytes"`
	RetryMax        int           `yaml:"retry_max"`
	RetryBackoff    time.Duration `yaml:"retry_backoff"`
}

// ProducerConfig holds event generation settings
type ProducerConfig struct {
	DefaultMode     string        `yaml:"default_mode"` // regular, peak, night
	EventsPerSecond int           `yaml:"events_per_second"`
	BatchSize       int           `yaml:"batch_size"`
	BatchTimeout    time.Duration `yaml:"batch_timeout"`
	ErrorRate       float64       `yaml:"error_rate"`     // 0.05 = 5%
	DuplicateRate   float64       `yaml:"duplicate_rate"` // 0.01 = 1%
	BounceRate      float64       `yaml:"bounce_rate"`    // 0.10 = 10%

	// Mode-specific settings
	RegularEventsPerSec int           `yaml:"regular_events_per_sec"` // 1-10
	PeakBurstSize       int           `yaml:"peak_burst_size"`        // 100-1000
	PeakBurstInterval   time.Duration `yaml:"peak_burst_interval"`    // every 5 min
	NightEventsInterval time.Duration `yaml:"night_events_interval"`  // 1 per 10 sec

	// Data generation
	PageIDPool int      `yaml:"page_id_pool"` // number of unique pages
	UserIDPool int      `yaml:"user_id_pool"` // number of active users
	Regions    []string `yaml:"regions"`
	UserAgents []string `yaml:"user_agents"`
}

// Load reads configuration from YAML file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if cfg.Kafka.Version == "" {
		cfg.Kafka.Version = "2.8.0"
	}
	if cfg.Kafka.Compression == "" {
		cfg.Kafka.Compression = "snappy"
	}
	if cfg.Kafka.RequiredAcks == 0 {
		cfg.Kafka.RequiredAcks = -1 // WaitForAll
	}
	if cfg.Kafka.MaxMessageBytes == 0 {
		cfg.Kafka.MaxMessageBytes = 1000000
	}
	if cfg.Producer.BatchSize == 0 {
		cfg.Producer.BatchSize = 100
	}
	if cfg.Producer.BatchTimeout == 0 {
		cfg.Producer.BatchTimeout = 5 * time.Second
	}

	return &cfg, nil
}

// DefaultConfig returns configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port: 8080,
		},
		Kafka: KafkaConfig{
			Brokers:         []string{"localhost:9092"},
			Topic:           "page-views",
			Version:         "2.8.0",
			Compression:     "snappy",
			RequiredAcks:    -1,
			MaxMessageBytes: 1000000,
			RetryMax:        5,
			RetryBackoff:    100 * time.Millisecond,
		},
		Producer: ProducerConfig{
			DefaultMode:         "regular",
			EventsPerSecond:     5,
			BatchSize:           100,
			BatchTimeout:        5 * time.Second,
			ErrorRate:           0.05,
			DuplicateRate:       0.01,
			BounceRate:          0.10,
			RegularEventsPerSec: 5,
			PeakBurstSize:       500,
			PeakBurstInterval:   5 * time.Minute,
			NightEventsInterval: 10 * time.Second,
			PageIDPool:          500,
			UserIDPool:          10000,
			Regions: []string{
				"EU", "US", "ASIA", "SA", "AF", "AU",
			},
			UserAgents: []string{
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
				"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
				"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)",
				"Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X)",
			},
		},
	}
}
