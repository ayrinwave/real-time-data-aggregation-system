package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Consumer   ConsumerConfig   `yaml:"consumer"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	GroupID string   `yaml:"group_id"`
	Version string   `yaml:"version"`

	AutoOffsetReset   string        `yaml:"auto_offset_reset"`
	EnableAutoCommit  bool          `yaml:"enable_auto_commit"`
	SessionTimeout    time.Duration `yaml:"session_timeout"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`

	FetchMinBytes  int           `yaml:"fetch_min_bytes"`
	FetchMaxBytes  int           `yaml:"fetch_max_bytes"`
	FetchMaxWaitMs time.Duration `yaml:"fetch_max_wait_ms"`

	MaxProcessingTime time.Duration `yaml:"max_processing_time"`
}

type ClickHouseConfig struct {
	Addresses []string `yaml:"addresses"`
	Database  string   `yaml:"database"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`

	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`

	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`

	Compression string `yaml:"compression"`
}

type ConsumerConfig struct {
	Mode string `yaml:"mode"`

	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`

	RetryMax     int           `yaml:"retry_max"`
	RetryBackoff time.Duration `yaml:"retry_backoff"`

	BufferSize int `yaml:"buffer_size"`

	Workers int `yaml:"workers"`

	DeliveryGuarantee string `yaml:"delivery_guarantee"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	setDefaults(&cfg)

	return &cfg, nil
}

func setDefaults(cfg *Config) {

	if cfg.Kafka.Version == "" {
		cfg.Kafka.Version = "2.8.0"
	}
	if cfg.Kafka.GroupID == "" {
		cfg.Kafka.GroupID = "page-views-consumer"
	}
	if cfg.Kafka.AutoOffsetReset == "" {
		cfg.Kafka.AutoOffsetReset = "earliest"
	}
	if cfg.Kafka.SessionTimeout == 0 {
		cfg.Kafka.SessionTimeout = 10 * time.Second
	}
	if cfg.Kafka.HeartbeatInterval == 0 {
		cfg.Kafka.HeartbeatInterval = 3 * time.Second
	}
	if cfg.Kafka.FetchMinBytes == 0 {
		cfg.Kafka.FetchMinBytes = 1
	}
	if cfg.Kafka.FetchMaxBytes == 0 {
		cfg.Kafka.FetchMaxBytes = 1024 * 1024
	}
	if cfg.Kafka.FetchMaxWaitMs == 0 {
		cfg.Kafka.FetchMaxWaitMs = 500 * time.Millisecond
	}
	if cfg.Kafka.MaxProcessingTime == 0 {
		cfg.Kafka.MaxProcessingTime = 30 * time.Second
	}

	if cfg.ClickHouse.Database == "" {
		cfg.ClickHouse.Database = "default"
	}
	if cfg.ClickHouse.MaxOpenConns == 0 {
		cfg.ClickHouse.MaxOpenConns = 50
	}
	if cfg.ClickHouse.MaxIdleConns == 0 {
		cfg.ClickHouse.MaxIdleConns = 10
	}
	if cfg.ClickHouse.ConnMaxLifetime == 0 {
		cfg.ClickHouse.ConnMaxLifetime = 1 * time.Hour
	}
	if cfg.ClickHouse.DialTimeout == 0 {
		cfg.ClickHouse.DialTimeout = 10 * time.Second
	}
	if cfg.ClickHouse.ReadTimeout == 0 {
		cfg.ClickHouse.ReadTimeout = 30 * time.Second
	}
	if cfg.ClickHouse.WriteTimeout == 0 {
		cfg.ClickHouse.WriteTimeout = 30 * time.Second
	}
	if cfg.ClickHouse.Compression == "" {
		cfg.ClickHouse.Compression = "lz4"
	}

	if cfg.Consumer.Mode == "" {
		cfg.Consumer.Mode = "hybrid"
	}
	if cfg.Consumer.BatchSize == 0 {
		cfg.Consumer.BatchSize = 1000
	}
	if cfg.Consumer.BatchTimeout == 0 {
		cfg.Consumer.BatchTimeout = 10 * time.Second
	}
	if cfg.Consumer.RetryMax == 0 {
		cfg.Consumer.RetryMax = 3
	}
	if cfg.Consumer.RetryBackoff == 0 {
		cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	}
	if cfg.Consumer.BufferSize == 0 {
		cfg.Consumer.BufferSize = 10000
	}
	if cfg.Consumer.Workers == 0 {
		cfg.Consumer.Workers = 4
	}
	if cfg.Consumer.DeliveryGuarantee == "" {
		cfg.Consumer.DeliveryGuarantee = "at-least-once"
	}

	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8081
	}
}

func DefaultConfig() *Config {
	cfg := &Config{
		Server: ServerConfig{
			Port: 8081,
		},
		Kafka: KafkaConfig{
			Brokers:           []string{"localhost:9092"},
			Topic:             "page-views",
			GroupID:           "page-views-consumer",
			Version:           "2.8.0",
			AutoOffsetReset:   "earliest",
			EnableAutoCommit:  false,
			SessionTimeout:    10 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			FetchMinBytes:     1,
			FetchMaxBytes:     1024 * 1024,
			FetchMaxWaitMs:    500 * time.Millisecond,
			MaxProcessingTime: 30 * time.Second,
		},
		ClickHouse: ClickHouseConfig{
			Addresses:       []string{"localhost:9000"},
			Database:        "default",
			Username:        "default",
			Password:        "",
			MaxOpenConns:    50,
			MaxIdleConns:    10,
			ConnMaxLifetime: 1 * time.Hour,
			DialTimeout:     10 * time.Second,
			ReadTimeout:     30 * time.Second,
			WriteTimeout:    30 * time.Second,
			Compression:     "lz4",
		},
		Consumer: ConsumerConfig{
			Mode:              "hybrid",
			BatchSize:         1000,
			BatchTimeout:      10 * time.Second,
			RetryMax:          3,
			RetryBackoff:      100 * time.Millisecond,
			BufferSize:        10000,
			Workers:           4,
			DeliveryGuarantee: "at-least-once",
		},
	}

	return cfg
}
