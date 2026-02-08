package generator

import (
	"encoding/json"
	"fmt"
	"math/rand"
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

type EventType int

const (
	NormalEvent EventType = iota
	BounceEvent
	ErrorEventEmptyPageID
	ErrorEventNegativeDuration
	ErrorEventInvalidJSON
)

func GenerateEvent(pageIDPool, userIDPool int, regions, userAgents []string, eventType EventType) *PageViewEvent {
	event := &PageViewEvent{
		Timestamp: time.Now().UTC(),
	}

	switch eventType {
	case NormalEvent:
		event.PageID = fmt.Sprintf("page_%d", rand.Intn(pageIDPool))
		event.UserID = fmt.Sprintf("user_%d", rand.Intn(userIDPool))
		event.ViewDuration = 10000 + rand.Intn(590000)
		event.IsBounce = false

	case BounceEvent:
		event.PageID = fmt.Sprintf("page_%d", rand.Intn(pageIDPool))
		event.UserID = fmt.Sprintf("user_%d", rand.Intn(userIDPool))
		event.ViewDuration = 500 + rand.Intn(4500)
		event.IsBounce = true

	case ErrorEventEmptyPageID:
		event.PageID = ""
		event.UserID = fmt.Sprintf("user_%d", rand.Intn(userIDPool))
		event.ViewDuration = 10000 + rand.Intn(590000)
		event.IsBounce = false

	case ErrorEventNegativeDuration:
		event.PageID = fmt.Sprintf("page_%d", rand.Intn(pageIDPool))
		event.UserID = fmt.Sprintf("user_%d", rand.Intn(userIDPool))
		event.ViewDuration = -1 * (1000 + rand.Intn(10000))
		event.IsBounce = false
	}

	if eventType != ErrorEventInvalidJSON {
		if len(userAgents) > 0 {
			event.UserAgent = userAgents[rand.Intn(len(userAgents))]
		}
		event.IPAddress = generateRandomIP()
		if len(regions) > 0 {
			event.Region = regions[rand.Intn(len(regions))]
		}
	}

	return event
}

func (e *PageViewEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

func (e *PageViewEvent) ToInvalidJSON() []byte {

	return []byte(fmt.Sprintf(`{"page_id":"%s","user_id":"%s","view_duration_ms":%d,"timestamp":INVALID`,
		e.PageID, e.UserID, e.ViewDuration))
}

func (e *PageViewEvent) GetMessageKey() string {
	return e.PageID
}

func generateRandomIP() string {

	if rand.Float64() < 0.8 {
		return fmt.Sprintf("%d.%d.%d.%d",
			rand.Intn(256),
			rand.Intn(256),
			rand.Intn(256),
			rand.Intn(256),
		)
	}

	parts := make([]string, 8)
	for i := 0; i < 8; i++ {
		parts[i] = fmt.Sprintf("%04x", rand.Intn(65536))
	}
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%s",
		parts[0], parts[1], parts[2], parts[3],
		parts[4], parts[5], parts[6], parts[7])
}

func DetermineEventType(errorRate, bounceRate float64) EventType {
	roll := rand.Float64()

	if roll < errorRate {

		errorRoll := rand.Float64()
		if errorRoll < 0.33 {
			return ErrorEventEmptyPageID
		} else if errorRoll < 0.66 {
			return ErrorEventNegativeDuration
		}
		return ErrorEventInvalidJSON
	}

	if roll < errorRate+(1-errorRate)*bounceRate {
		return BounceEvent
	}

	return NormalEvent
}
