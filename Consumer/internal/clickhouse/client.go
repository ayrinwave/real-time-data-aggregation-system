package clickhouse

import (
	"context"
	"fmt"
	"log"
	"time"

	"real-time_data_aggregation_system/Consumer/internal/config"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Client struct {
	conn   driver.Conn
	config *config.ClickHouseConfig
}

func NewClient(cfg *config.ClickHouseConfig) (*Client, error) {

	var compression clickhouse.CompressionMethod
	switch cfg.Compression {
	case "lz4":
		compression = clickhouse.CompressionLZ4
	case "zstd":
		compression = clickhouse.CompressionZSTD
	default:
		compression = clickhouse.CompressionNone
	}

	opts := &clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Protocol: clickhouse.Native,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:      cfg.DialTimeout,
		MaxOpenConns:     cfg.MaxOpenConns,
		MaxIdleConns:     cfg.MaxIdleConns,
		ConnMaxLifetime:  cfg.ConnMaxLifetime,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		Compression: &clickhouse.Compression{
			Method: compression,
		},
	}

	log.Printf("DEBUG: Connecting as user='%s' with password length=%d", opts.Auth.Username, len(opts.Auth.Password))
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	log.Printf("Connected to ClickHouse: %v", cfg.Addresses)

	return &Client{
		conn:   conn,
		config: cfg,
	}, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

func (c *Client) QueryRow(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return c.conn.QueryRow(ctx, query, args...).Scan(dest)
}

func (c *Client) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return c.conn.PrepareBatch(ctx, query)
}

func (c *Client) GetConn() driver.Conn {
	return c.conn
}

type Stats struct {
	InsertsTotal   int64
	InsertsFailed  int64
	RowsInserted   int64
	LastInsertTime time.Time
	LastError      error
}
