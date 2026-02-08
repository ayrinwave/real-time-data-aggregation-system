package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"real-time_data_aggregation_system/Producer/internal/generator"
	"real-time_data_aggregation_system/Producer/internal/producer"
)

type Handler struct {
	generator *generator.Generator
	producer  *producer.KafkaProducer
}

func NewHandler(gen *generator.Generator, prod *producer.KafkaProducer) *Handler {
	return &Handler{
		generator: gen,
		producer:  prod,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {

	mux.HandleFunc("/config/speed", h.handleSetSpeed)
	mux.HandleFunc("/config/mode", h.handleSetMode)

	mux.HandleFunc("/generate/burst", h.handleBurst)

	mux.HandleFunc("/status", h.handleStatus)
	mux.HandleFunc("/health", h.handleHealth)

	mux.Handle("/metrics", promhttp.Handler())
}

func (h *Handler) handleSetSpeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	epsStr := r.URL.Query().Get("eps")
	if epsStr == "" {
		http.Error(w, "Missing 'eps' parameter", http.StatusBadRequest)
		return
	}

	eps, err := strconv.Atoi(epsStr)
	if err != nil {
		http.Error(w, "Invalid 'eps' value", http.StatusBadRequest)
		return
	}

	if eps < 1 || eps > 10000 {
		http.Error(w, "eps must be between 1 and 10000", http.StatusBadRequest)
		return
	}

	h.generator.SetEventsPerSecond(eps)

	response := map[string]interface{}{
		"status":            "success",
		"events_per_second": eps,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Printf("Speed changed to %d events/sec via API", eps)
}

func (h *Handler) handleSetMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	modeStr := r.URL.Query().Get("mode")
	if modeStr == "" {
		http.Error(w, "Missing 'mode' parameter", http.StatusBadRequest)
		return
	}

	mode := generator.GenerationMode(modeStr)

	if mode != generator.ModeRegular && mode != generator.ModePeak && mode != generator.ModeNight {
		http.Error(w, "Invalid mode. Must be: regular, peak, or night", http.StatusBadRequest)
		return
	}

	h.generator.SetMode(mode)

	response := map[string]interface{}{
		"status": "success",
		"mode":   modeStr,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Printf("Mode changed to %s via API", modeStr)
}

func (h *Handler) handleBurst(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	countStr := r.URL.Query().Get("count")
	if countStr == "" {
		http.Error(w, "Missing 'count' parameter", http.StatusBadRequest)
		return
	}

	count, err := strconv.Atoi(countStr)
	if err != nil {
		http.Error(w, "Invalid 'count' value", http.StatusBadRequest)
		return
	}

	if count < 1 || count > 10000 {
		http.Error(w, "count must be between 1 and 10000", http.StatusBadRequest)
		return
	}

	h.generator.GenerateBurst(count)

	response := map[string]interface{}{
		"status":     "success",
		"burst_size": count,
		"message":    fmt.Sprintf("Generating %d events", count),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Printf("Burst of %d events triggered via API", count)
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	genStats := h.generator.GetStats()
	prodStats := h.producer.GetStats()

	response := map[string]interface{}{
		"generator": map[string]interface{}{
			"mode":              h.generator.GetMode(),
			"events_per_second": h.generator.GetEventsPerSecond(),
			"total_generated":   genStats.TotalGenerated,
			"normal_events":     genStats.NormalEvents,
			"bounce_events":     genStats.BounceEvents,
			"error_events":      genStats.ErrorEvents,
			"duplicate_events":  genStats.DuplicateEvents,
		},
		"producer": map[string]interface{}{
			"sent_total":        prodStats.SentTotal,
			"sent_success":      prodStats.SentSuccess,
			"sent_failed":       prodStats.SentFailed,
			"retries_total":     prodStats.RetriesTotal,
			"last_send_latency": prodStats.LastSendLatency.String(),
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

	prodStats := h.producer.GetStats()

	healthy := true
	issues := []string{}

	if prodStats.SentTotal > 100 {
		successRate := float64(prodStats.SentSuccess) / float64(prodStats.SentTotal)
		if successRate < 0.95 {
			healthy = false
			issues = append(issues, fmt.Sprintf("Low success rate: %.2f%%", successRate*100))
		}
	}

	status := "healthy"
	if !healthy {
		status = "unhealthy"
	}

	response := map[string]interface{}{
		"status": status,
		"issues": issues,
	}

	if !healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
