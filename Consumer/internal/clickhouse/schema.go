package clickhouse

import (
	"context"
	"fmt"
	"log"
)

func (c *Client) InitSchema(ctx context.Context) error {
	log.Println("Initializing ClickHouse schema...")

	tables := []struct {
		name  string
		query string
	}{
		{
			name:  "page_views_raw",
			query: createPageViewsRawTable,
		},
		{
			name:  "page_views_agg_minute",
			query: createPageViewsAggMinuteTable,
		},
		{
			name:  "page_views_agg_hour",
			query: createPageViewsAggHourTable,
		},
		{
			name:  "processing_errors",
			query: createProcessingErrorsTable,
		},
	}

	for _, table := range tables {
		log.Printf("Creating table: %s", table.name)
		if err := c.Exec(ctx, table.query); err != nil {
			return fmt.Errorf("failed to create table %s: %w", table.name, err)
		}
	}

	views := []struct {
		name  string
		query string
	}{
		{
			name:  "page_views_raw_to_minute",
			query: createRawToMinuteMV,
		},
		{
			name:  "page_views_minute_to_hour",
			query: createMinuteToHourMV,
		},
	}

	for _, view := range views {
		log.Printf("Creating materialized view: %s", view.name)
		if err := c.Exec(ctx, view.query); err != nil {

			log.Printf("Warning: failed to create MV %s (might already exist): %v", view.name, err)
		}
	}

	log.Println("Schema initialization complete")
	return nil
}

const createPageViewsRawTable = `
CREATE TABLE IF NOT EXISTS page_views_raw
(
    event_date    Date DEFAULT today(),
    event_time    DateTime64(3, 'UTC'),
    page_id       String,
    user_id       String,
    duration_ms   UInt32,
    user_agent    String,
    ip_address    IPv6,
    region        LowCardinality(String),
    is_bounce     UInt8,
    kafka_offset  Int64,
    kafka_partition Int32,
    processed_time DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, page_id, user_id)
TTL event_date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192
`

const createPageViewsAggMinuteTable = `
CREATE TABLE IF NOT EXISTS page_views_agg_minute
(
    window_start  DateTime,
    page_id       String,
    view_count    AggregateFunction(sum, UInt64),
    total_duration AggregateFunction(sum, UInt64),
    unique_users  AggregateFunction(uniq, String),
    bounce_count  AggregateFunction(sum, UInt8)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, page_id)
TTL window_start + INTERVAL 7 DAY
`

const createPageViewsAggHourTable = `
CREATE TABLE IF NOT EXISTS page_views_agg_hour
(
    window_start  DateTime,
    page_id       String,
    view_count    UInt64,
    avg_duration  Float32,
    unique_users  UInt64,
    bounce_rate   Float32
) ENGINE = SummingMergeTree()
ORDER BY (window_start, page_id)
`

const createProcessingErrorsTable = `
CREATE TABLE IF NOT EXISTS processing_errors
(
    error_time    DateTime,
    raw_message   String,
    error_reason  String,
    kafka_offset  Int64,
    kafka_partition Int32
) ENGINE = MergeTree()
ORDER BY (error_time)
`

const createRawToMinuteMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_raw_to_minute
TO page_views_agg_minute
AS
SELECT
    toStartOfMinute(event_time) AS window_start,
    page_id,
    sumState(toUInt64(1)) as view_count,
    sumState(toUInt64(duration_ms)) as total_duration,
    uniqState(user_id) as unique_users,
    sumState(is_bounce) as bounce_count
FROM page_views_raw
GROUP BY window_start, page_id
`

const createMinuteToHourMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_minute_to_hour
TO page_views_agg_hour
AS
SELECT
    toStartOfHour(window_start) AS window_start,
    page_id,
    sumMerge(view_count) as view_count,
    sumMerge(total_duration) / sumMerge(view_count) as avg_duration,
    uniqMerge(unique_users) as unique_users,
    (sumMerge(bounce_count) * 100.0) / sumMerge(view_count) as bounce_rate
FROM page_views_agg_minute
GROUP BY window_start, page_id
`

func (c *Client) DropSchema(ctx context.Context) error {
	log.Println("Dropping schema...")

	drops := []string{
		"DROP VIEW IF EXISTS page_views_raw_to_minute",
		"DROP VIEW IF EXISTS page_views_minute_to_hour",
		"DROP TABLE IF EXISTS page_views_raw",
		"DROP TABLE IF EXISTS page_views_agg_minute",
		"DROP TABLE IF EXISTS page_views_agg_hour",
		"DROP TABLE IF EXISTS processing_errors",
	}

	for _, query := range drops {
		if err := c.Exec(ctx, query); err != nil {
			log.Printf("Warning: %v", err)
		}
	}

	log.Println("Schema dropped")
	return nil
}

func (c *Client) VerifySchema(ctx context.Context) error {
	tables := []string{
		"page_views_raw",
		"page_views_agg_minute",
		"page_views_agg_hour",
		"processing_errors",
	}

	for _, table := range tables {
		var count uint64
		query := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s'",
			c.config.Database, table)

		if err := c.QueryRow(ctx, &count, query); err != nil {
			return fmt.Errorf("failed to check table %s: %w", table, err)
		}

		if count == 0 {
			return fmt.Errorf("table %s does not exist", table)
		}
	}

	log.Println("Schema verification successful")
	return nil
}
