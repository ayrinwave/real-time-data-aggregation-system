package generator

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"real-time_data_aggregation_system/Producer/internal/config"
)

type GenerationMode string

const (
	ModeRegular GenerationMode = "regular"
	ModePeak    GenerationMode = "peak"
	ModeNight   GenerationMode = "night"
)

type Generator struct {
	cfg            *config.ProducerConfig
	mode           GenerationMode
	modeMu         sync.RWMutex
	eventsPerSec   int
	eventsPerSecMu sync.RWMutex

	eventChan chan *PageViewEvent

	lastEvent   *PageViewEvent
	lastEventMu sync.Mutex

	stats   Stats
	statsMu sync.RWMutex
}

type Stats struct {
	TotalGenerated  int64
	NormalEvents    int64
	BounceEvents    int64
	ErrorEvents     int64
	DuplicateEvents int64
}

func NewGenerator(cfg *config.ProducerConfig) *Generator {
	mode := GenerationMode(cfg.DefaultMode)
	if mode != ModeRegular && mode != ModePeak && mode != ModeNight {
		mode = ModeRegular
	}

	return &Generator{
		cfg:          cfg,
		mode:         mode,
		eventsPerSec: cfg.EventsPerSecond,
		eventChan:    make(chan *PageViewEvent, 1000),
	}
}

func (g *Generator) Start(ctx context.Context) error {
	log.Printf("Starting generator in %s mode", g.mode)

	go g.generateLoop(ctx)

	return nil
}

func (g *Generator) EventChannel() <-chan *PageViewEvent {
	return g.eventChan
}

func (g *Generator) SetMode(mode GenerationMode) {
	g.modeMu.Lock()
	defer g.modeMu.Unlock()

	g.mode = mode
	log.Printf("Generation mode changed to: %s", mode)
}

func (g *Generator) GetMode() GenerationMode {
	g.modeMu.RLock()
	defer g.modeMu.RUnlock()
	return g.mode
}

func (g *Generator) SetEventsPerSecond(eps int) {
	g.eventsPerSecMu.Lock()
	defer g.eventsPerSecMu.Unlock()

	if eps < 1 {
		eps = 1
	}
	if eps > 10000 {
		eps = 10000
	}

	g.eventsPerSec = eps
	log.Printf("Events per second changed to: %d", eps)
}

func (g *Generator) GetEventsPerSecond() int {
	g.eventsPerSecMu.RLock()
	defer g.eventsPerSecMu.RUnlock()
	return g.eventsPerSec
}

func (g *Generator) GenerateBurst(count int) {
	log.Printf("Generating burst of %d events", count)

	go func() {
		for i := 0; i < count; i++ {
			event := g.generateSingleEvent()
			select {
			case g.eventChan <- event:
			default:
				log.Printf("Event channel full, dropping event")
			}
		}
	}()
}

func (g *Generator) GetStats() Stats {
	g.statsMu.RLock()
	defer g.statsMu.RUnlock()
	return g.stats
}

func (g *Generator) generateLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastBurst := time.Now()

	for {
		select {
		case <-ctx.Done():
			close(g.eventChan)
			return

		case <-ticker.C:
			mode := g.GetMode()

			switch mode {
			case ModeRegular:
				g.generateRegularEvents()

			case ModePeak:

				if time.Since(lastBurst) >= g.cfg.PeakBurstInterval {
					g.GenerateBurst(g.cfg.PeakBurstSize)
					lastBurst = time.Now()
				} else {

					g.generateRegularEvents()
				}

			case ModeNight:
				g.generateNightEvents()
			}
		}
	}
}

func (g *Generator) generateRegularEvents() {
	eps := g.GetEventsPerSecond()

	eventsToGenerate := eps / 10

	fraction := float64(eps%10) / 10.0
	if rand.Float64() < fraction {
		eventsToGenerate++
	}

	for i := 0; i < eventsToGenerate; i++ {
		event := g.generateSingleEvent()

		select {
		case g.eventChan <- event:
		default:
			log.Printf("Event channel full, dropping event")
		}
	}
}

func (g *Generator) generateNightEvents() {

	if rand.Float64() < 0.01 {
		event := g.generateSingleEvent()

		select {
		case g.eventChan <- event:
		default:
			log.Printf("Event channel full, dropping event")
		}
	}
}

func (g *Generator) generateSingleEvent() *PageViewEvent {

	isDuplicate := rand.Float64() < g.cfg.DuplicateRate

	if isDuplicate {
		g.lastEventMu.Lock()
		defer g.lastEventMu.Unlock()

		if g.lastEvent != nil {
			g.incrementStat("duplicate")

			duplicate := *g.lastEvent
			return &duplicate
		}
	}

	eventType := DetermineEventType(g.cfg.ErrorRate, g.cfg.BounceRate)

	event := GenerateEvent(
		g.cfg.PageIDPool,
		g.cfg.UserIDPool,
		g.cfg.Regions,
		g.cfg.UserAgents,
		eventType,
	)

	g.lastEventMu.Lock()
	g.lastEvent = event
	g.lastEventMu.Unlock()

	switch eventType {
	case NormalEvent:
		g.incrementStat("normal")
	case BounceEvent:
		g.incrementStat("bounce")
	default:
		g.incrementStat("error")
	}

	g.incrementStat("total")

	return event
}

func (g *Generator) incrementStat(stat string) {
	g.statsMu.Lock()
	defer g.statsMu.Unlock()

	switch stat {
	case "total":
		g.stats.TotalGenerated++
	case "normal":
		g.stats.NormalEvents++
	case "bounce":
		g.stats.BounceEvents++
	case "error":
		g.stats.ErrorEvents++
	case "duplicate":
		g.stats.DuplicateEvents++
	}
}

func IsNightTime() bool {
	hour := time.Now().Hour()
	return hour >= 0 && hour < 6
}

func (g *Generator) AutoSwitchMode(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			currentMode := g.GetMode()

			if currentMode == ModePeak {
				continue
			}

			if IsNightTime() && currentMode != ModeNight {
				g.SetMode(ModeNight)
			} else if !IsNightTime() && currentMode != ModeRegular {
				g.SetMode(ModeRegular)
			}
		}
	}
}
