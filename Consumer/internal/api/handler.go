package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"real-time_data_aggregation_system/Consumer/internal/clickhouse"
	"real-time_data_aggregation_system/Consumer/internal/consumer"
	"real-time_data_aggregation_system/Consumer/internal/validator"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Handler struct {
	kafkaConsumer *consumer.KafkaConsumer
	processor     *consumer.Processor
	validator     *validator.Validator
	writer        *clickhouse.Writer
	chClient      *clickhouse.Client
}

func NewHandler(
	kafkaConsumer *consumer.KafkaConsumer,
	processor *consumer.Processor,
	validator *validator.Validator,
	writer *clickhouse.Writer,
	chClient *clickhouse.Client,
) *Handler {
	return &Handler{
		kafkaConsumer: kafkaConsumer,
		processor:     processor,
		validator:     validator,
		writer:        writer,
		chClient:      chClient,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {

	mux.HandleFunc("/status", h.handleStatus)
	mux.HandleFunc("/health", h.handleHealth)

	mux.HandleFunc("/flush", h.handleFlush)

	mux.HandleFunc("/query/aggregates", h.handleQueryAggregates)
	mux.HandleFunc("/query/errors", h.handleQueryErrors)

	mux.Handle("/metrics", promhttp.Handler())
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	consumerStats := h.kafkaConsumer.GetStats()
	processorStats := h.processor.GetStats()
	validatorStats := h.validator.GetStats()
	writerStats := h.writer.GetStats()

	response := map[string]interface{}{
		"consumer": map[string]interface{}{
			"messages_consumed": consumerStats.MessagesConsumed,
			"last_consume_time": consumerStats.LastConsumeTime.Format(time.RFC3339),
			"current_lag":       consumerStats.CurrentLag,
			"errors":            consumerStats.Errors,
		},
		"processor": map[string]interface{}{
			"messages_processed": processorStats.MessagesProcessed,
			"valid_messages":     processorStats.ValidMessages,
			"invalid_messages":   processorStats.InvalidMessages,
			"flush_count":        processorStats.FlushCount,
			"last_flush_time":    processorStats.LastFlushTime.Format(time.RFC3339),
		},
		"validator": map[string]interface{}{
			"total_validated": validatorStats["total"],
			"valid_count":     validatorStats["valid"],
			"invalid_count":   validatorStats["invalid"],
		},
		"writer": map[string]interface{}{
			"raw_inserted":     writerStats.RawInserted,
			"errors_inserted":  writerStats.ErrorsInserted,
			"inserts_failed":   writerStats.InsertsFailed,
			"last_insert_time": writerStats.LastInsertTime.Format(time.RFC3339),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	healthy := true
	issues := []string{}

	consumerStats := h.kafkaConsumer.GetStats()
	if consumerStats.Errors > 100 {
		healthy = false
		issues = append(issues, "High Kafka error count")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.chClient.Ping(ctx); err != nil {
		healthy = false
		issues = append(issues, fmt.Sprintf("ClickHouse ping failed: %v", err))
	}

	processorStats := h.processor.GetStats()
	if processorStats.MessagesProcessed > 0 {
		invalidRate := float64(processorStats.InvalidMessages) / float64(processorStats.MessagesProcessed)
		if invalidRate > 0.5 {
			healthy = false
			issues = append(issues, fmt.Sprintf("High invalid message rate: %.2f%%", invalidRate*100))
		}
	}

	status := "healthy"
	statusCode := http.StatusOK
	if !healthy {
		status = "unhealthy"
		statusCode = http.StatusServiceUnavailable
	}

	response := map[string]interface{}{
		"status":    status,
		"issues":    issues,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := h.processor.Flush(ctx); err != nil {
		response := map[string]interface{}{
			"status": "error",
			"error":  err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	response := map[string]interface{}{
		"status":  "success",
		"message": "Flush completed",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) handleQueryAggregates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hoursStr := r.URL.Query().Get("hours")
	if hoursStr == "" {
		hoursStr = "1"
	}

	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hours < 1 || hours > 24 {
		http.Error(w, "Invalid hours parameter (must be 1-24)", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	aggregates, err := h.writer.QueryAggregates(ctx, hours)
	if err != nil {
		log.Printf("Failed to query aggregates: %v", err)
		http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"hours":      hours,
		"count":      len(aggregates),
		"aggregates": aggregates,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) handleQueryErrors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limitStr := r.URL.Query().Get("limit")
	if limitStr == "" {
		limitStr = "10"
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 || limit > 1000 {
		http.Error(w, "Invalid limit parameter (must be 1-1000)", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	errors, err := h.writer.QueryErrors(ctx, limit)
	if err != nil {
		log.Printf("Failed to query errors: %v", err)
		http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"limit":  limit,
		"count":  len(errors),
		"errors": errors,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
