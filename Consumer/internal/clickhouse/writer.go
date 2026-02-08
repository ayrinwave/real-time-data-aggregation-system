package clickhouse

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"real-time_data_aggregation_system/Consumer/internal/validator"
)

type Writer struct {
	client *Client

	stats   WriterStats
	statsMu sync.RWMutex
}

type WriterStats struct {
	RawInserted    int64
	ErrorsInserted int64
	InsertsFailed  int64
	LastInsertTime time.Time
}

func NewWriter(client *Client) *Writer {
	return &Writer{
		client: client,
	}
}

func (w *Writer) InsertRawEvents(ctx context.Context, events []*validator.PageViewEvent, offsets []KafkaOffset) error {
	if len(events) == 0 {
		return nil
	}

	start := time.Now()

	batch, err := w.client.PrepareBatch(ctx, `
		INSERT INTO page_views_raw (
			event_date,
			event_time,
			page_id,
			user_id,
			duration_ms,
			user_agent,
			ip_address,
			region,
			is_bounce,
			kafka_offset,
			kafka_partition,
			processed_time
		)
	`)
	if err != nil {
		w.incrementStat("failed")
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for i, event := range events {

		ipAddr := net.ParseIP(event.IPAddress)
		if ipAddr == nil {
			ipAddr = net.ParseIP("0.0.0.0")
		}

		var isBounce uint8
		if event.IsBounce {
			isBounce = 1
		}

		err := batch.Append(
			event.Timestamp.Truncate(24*time.Hour),
			event.Timestamp,
			event.PageID,
			event.UserID,
			uint32(event.ViewDuration),
			event.UserAgent,
			ipAddr,
			event.Region,
			isBounce,
			offsets[i].Offset,
			offsets[i].Partition,
			time.Now(),
		)
		if err != nil {
			log.Printf("Failed to append event to batch: %v", err)
			continue
		}
	}

	if err := batch.Send(); err != nil {
		w.incrementStat("failed")
		return fmt.Errorf("failed to send batch: %w", err)
	}

	w.statsMu.Lock()
	w.stats.RawInserted += int64(len(events))
	w.stats.LastInsertTime = time.Now()
	w.statsMu.Unlock()

	log.Printf("Inserted %d events into page_views_raw in %v", len(events), time.Since(start))
	return nil
}

func (w *Writer) InsertErrors(ctx context.Context, errors []*validator.ErrorRecord) error {
	if len(errors) == 0 {
		return nil
	}

	start := time.Now()

	batch, err := w.client.PrepareBatch(ctx, `
		INSERT INTO processing_errors (
			error_time,
			raw_message,
			error_reason,
			kafka_offset,
			kafka_partition
		)
	`)
	if err != nil {
		w.incrementStat("failed")
		return fmt.Errorf("failed to prepare error batch: %w", err)
	}

	for _, errRecord := range errors {
		err := batch.Append(
			errRecord.ErrorTime,
			errRecord.RawMessage,
			errRecord.ErrorReason,
			errRecord.KafkaOffset,
			errRecord.KafkaPartition,
		)
		if err != nil {
			log.Printf("Failed to append error to batch: %v", err)
			continue
		}
	}

	if err := batch.Send(); err != nil {
		w.incrementStat("failed")
		return fmt.Errorf("failed to send error batch: %w", err)
	}

	w.statsMu.Lock()
	w.stats.ErrorsInserted += int64(len(errors))
	w.stats.LastInsertTime = time.Now()
	w.statsMu.Unlock()

	log.Printf("Inserted %d errors into DLQ in %v", len(errors), time.Since(start))
	return nil
}

func (w *Writer) GetStats() WriterStats {
	w.statsMu.RLock()
	defer w.statsMu.RUnlock()
	return w.stats
}

func (w *Writer) incrementStat(stat string) {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()

	switch stat {
	case "failed":
		w.stats.InsertsFailed++
	}
}

type KafkaOffset struct {
	Partition int32
	Offset    int64
}

func (w *Writer) QueryAggregates(ctx context.Context, hours int) ([]MinuteAggregate, error) {
	query := `
		SELECT 
			window_start,
			page_id,
			sumMerge(view_count) as view_count,
			sumMerge(total_duration) as total_duration,
			uniqMerge(unique_users) as unique_users,
			sumMerge(bounce_count) as bounce_count
		FROM page_views_agg_minute
		WHERE window_start >= now() - INTERVAL ? HOUR
		GROUP BY window_start, page_id
		ORDER BY window_start DESC
		LIMIT 100
	`

	rows, err := w.client.Query(ctx, query, hours)
	if err != nil {
		return nil, fmt.Errorf("failed to query aggregates: %w", err)
	}
	defer rows.Close()

	var results []MinuteAggregate
	for rows.Next() {
		var agg MinuteAggregate
		if err := rows.Scan(
			&agg.WindowStart,
			&agg.PageID,
			&agg.ViewCount,
			&agg.TotalDuration,
			&agg.UniqueUsers,
			&agg.BounceCount,
		); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		results = append(results, agg)
	}

	return results, nil
}

type MinuteAggregate struct {
	WindowStart   time.Time
	PageID        string
	ViewCount     uint64
	TotalDuration uint64
	UniqueUsers   uint64
	BounceCount   uint64
}

func (w *Writer) QueryErrors(ctx context.Context, limit int) ([]ErrorInfo, error) {
	query := `
		SELECT 
			error_time,
			raw_message,
			error_reason,
			kafka_offset,
			kafka_partition
		FROM processing_errors
		ORDER BY error_time DESC
		LIMIT ?
	`

	rows, err := w.client.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query errors: %w", err)
	}
	defer rows.Close()

	var results []ErrorInfo
	for rows.Next() {
		var info ErrorInfo
		if err := rows.Scan(
			&info.ErrorTime,
			&info.RawMessage,
			&info.ErrorReason,
			&info.KafkaOffset,
			&info.KafkaPartition,
		); err != nil {
			log.Printf("Failed to scan error row: %v", err)
			continue
		}
		results = append(results, info)
	}

	return results, nil
}

type ErrorInfo struct {
	ErrorTime      time.Time
	RawMessage     string
	ErrorReason    string
	KafkaOffset    int64
	KafkaPartition int32
}
