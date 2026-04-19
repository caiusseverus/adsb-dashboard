package iid

import (
	"sync"
	"time"
)

const positionMaxAgeS = 30.0 // stale after 30s — mirrors ADSB_POSITION_MAX_AGE in Python

// AircraftPosition is the most recently received ADS-B fix for one aircraft.
type AircraftPosition struct {
	ICAO  uint32
	Lat   float64
	Lon   float64
	AltFt *int32
	TS    time.Time // wall-clock time of last update
}

// PositionCache is a thread-safe map of ICAO → latest ADS-B fix.
type PositionCache struct {
	mu        sync.RWMutex
	positions map[uint32]*AircraftPosition
}

// NewPositionCache returns an empty PositionCache.
func NewPositionCache() *PositionCache {
	return &PositionCache{positions: make(map[uint32]*AircraftPosition)}
}

// Update stores or replaces the latest position for icao.
func (c *PositionCache) Update(icao uint32, lat, lon float64, altFt *int32, ts float64) {
	wallTS := time.Unix(int64(ts), int64((ts-float64(int64(ts)))*1e9))
	c.mu.Lock()
	c.positions[icao] = &AircraftPosition{
		ICAO:  icao,
		Lat:   lat,
		Lon:   lon,
		AltFt: altFt,
		TS:    wallTS,
	}
	c.mu.Unlock()
}

// Get returns the position for icao if it exists and is not stale, else nil.
func (c *PositionCache) Get(icao uint32) *AircraftPosition {
	c.mu.RLock()
	pos := c.positions[icao]
	c.mu.RUnlock()
	if pos == nil {
		return nil
	}
	if time.Since(pos.TS).Seconds() > positionMaxAgeS {
		return nil
	}
	return pos
}

// Prune removes positions older than positionMaxAgeS. Returns count removed.
func (c *PositionCache) Prune() int {
	cutoff := time.Now().Add(-positionMaxAgeS * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for icao, pos := range c.positions {
		if pos.TS.Before(cutoff) {
			delete(c.positions, icao)
			n++
		}
	}
	return n
}
