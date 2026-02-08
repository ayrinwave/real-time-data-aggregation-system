package validator

import (
	"encoding/json"
	"fmt"
	"time"
)

type PageViewEvent struct {
	PageID       string    `json:"page_id"`
	UserID       string    `json:"user_id"`
	ViewDuration int       `json:"view_duration_ms"`
	Timestamp    time.Time `json:"timestamp"`
	UserAgent    string    `json:"user_agent,omitempty"`
	IPAddress    string    `json:"ip_address,omitempty"`
	Region       string    `json:"region,omitempty"`
	IsBounce     bool      `json:"is_bounce"`
}

type ValidationError struct {
	Field   string
	Message string
	Value   interface{}
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error: field=%s, message=%s, value=%v", e.Field, e.Message, e.Value)
}

type ValidationResult struct {
	Valid  bool
	Errors []ValidationError
	Event  *PageViewEvent
}

type Validator struct {
	totalValidated int64
	validCount     int64
	invalidCount   int64
}

func NewValidator() *Validator {
	return &Validator{}
}

func (v *Validator) ValidateJSON(data []byte) *ValidationResult {
	v.totalValidated++

	result := &ValidationResult{
		Valid:  true,
		Errors: []ValidationError{},
	}

	var event PageViewEvent
	if err := json.Unmarshal(data, &event); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "json",
			Message: "invalid JSON format",
			Value:   string(data),
		})
		v.invalidCount++
		return result
	}

	result.Event = &event

	v.validateFields(&event, result)

	if !result.Valid {
		v.invalidCount++
	} else {
		v.validCount++
	}

	return result
}

func (v *Validator) validateFields(event *PageViewEvent, result *ValidationResult) {

	if event.PageID == "" {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "page_id",
			Message: "page_id cannot be empty",
			Value:   event.PageID,
		})
	}

	if event.UserID == "" {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "user_id",
			Message: "user_id cannot be empty",
			Value:   event.UserID,
		})
	}

	if event.ViewDuration < 0 {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "view_duration_ms",
			Message: "view_duration_ms cannot be negative",
			Value:   event.ViewDuration,
		})
	}

	if event.ViewDuration > 24*60*60*1000 {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "view_duration_ms",
			Message: "view_duration_ms exceeds maximum (24 hours)",
			Value:   event.ViewDuration,
		})
	}

	if event.Timestamp.IsZero() {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "timestamp",
			Message: "timestamp cannot be zero",
			Value:   event.Timestamp,
		})
	}

	if event.Timestamp.After(time.Now().Add(1 * time.Hour)) {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "timestamp",
			Message: "timestamp is too far in the future",
			Value:   event.Timestamp,
		})
	}

	if event.Timestamp.Before(time.Now().Add(-30 * 24 * time.Hour)) {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:   "timestamp",
			Message: "timestamp is too old (more than 30 days)",
			Value:   event.Timestamp,
		})
	}
}

func (v *Validator) GetStats() map[string]int64 {
	return map[string]int64{
		"total":   v.totalValidated,
		"valid":   v.validCount,
		"invalid": v.invalidCount,
	}
}

type ErrorRecord struct {
	ErrorTime      time.Time
	RawMessage     string
	ErrorReason    string
	KafkaOffset    int64
	KafkaPartition int32
}

func CreateErrorRecord(rawMessage []byte, errors []ValidationError, offset int64, partition int32) *ErrorRecord {

	errorReason := ""
	for i, err := range errors {
		if i > 0 {
			errorReason += "; "
		}
		errorReason += err.Error()
	}

	return &ErrorRecord{
		ErrorTime:      time.Now(),
		RawMessage:     string(rawMessage),
		ErrorReason:    errorReason,
		KafkaOffset:    offset,
		KafkaPartition: partition,
	}
}
