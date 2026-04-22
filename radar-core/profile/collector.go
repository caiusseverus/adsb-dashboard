package profile

import (
	"sync"
	"time"

	"github.com/caiusseverus/adsb-dashboard/radar-core/export"
)

var knownStages = []string{
	"burst_processing",
	"reference_selection",
	"dominant_check",
	"phase_check",
	"frame_mutation",
	"frame_finalisation",
	"sync_update",
	"snapshot_export",
	"evidence_export",
}

type stat struct {
	count   uint64
	totalMS float64
	maxMS   float64
}

// Collector aggregates wall-clock timings for exported diagnostics.
type Collector struct {
	mu    sync.Mutex
	stats map[string]stat
}

func NewCollector() *Collector {
	stats := make(map[string]stat, len(knownStages))
	for _, name := range knownStages {
		stats[name] = stat{}
	}
	return &Collector{stats: stats}
}

func (c *Collector) Observe(stage string, dur time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.stats[stage]
	ms := float64(dur) / float64(time.Millisecond)
	entry.count++
	entry.totalMS += ms
	if ms > entry.maxMS {
		entry.maxMS = ms
	}
	c.stats[stage] = entry
}

func (c *Collector) Snapshot() map[string]export.StageProfile {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]export.StageProfile, len(c.stats))
	for _, name := range knownStages {
		entry := c.stats[name]
		mean := 0.0
		if entry.count > 0 {
			mean = entry.totalMS / float64(entry.count)
		}
		out[name] = export.StageProfile{
			Count:   entry.count,
			TotalMS: entry.totalMS,
			MaxMS:   entry.maxMS,
			MeanMS:  mean,
		}
	}
	return out
}
