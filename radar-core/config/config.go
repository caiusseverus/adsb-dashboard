// Package config holds operational knobs for radar-core.
// Defaults match the Python RadarState constants so behaviour is identical
// before any CONFIG_UPDATE messages arrive.
package config

import (
	"os"
	"strconv"
	"sync/atomic"
)

const (
	DefaultBurstGapUS                = 200_000.0 // 200 ms — replies within this belong to one burst
	DefaultBurstRecordMaxAgeS        = 120.0
	DefaultBurstRecordsMaxPerIID     = 8000
	DefaultCentroidHistoryMaxPerICAO = 120
	DefaultMinBursts                 = 4
	DefaultMinQualifyingICAOs        = 4
	DefaultSocketPath                = "/run/adsb/radar-core.sock"
)

// Config holds live-adjustable operational parameters.
// All float64 fields are read via Load and written via Store on the atomic pointer;
// integer fields use sync/atomic directly. Reads on the hot path do not need a mutex.
type Config struct {
	BurstGapUS                float64
	BurstRecordMaxAgeS        float64
	BurstRecordsMaxPerIID     int
	CentroidHistoryMaxPerICAO int
	MinBursts                 int
	MinQualifyingICAOs        int
	SocketPath                string
	ReceiverLat               float64
	ReceiverLon               float64
	ReceiverLatSet            bool
	ReceiverLonSet            bool
	HasReceiver               bool
}

// global is the live config; accessed via Get/Apply.
var global atomic.Pointer[Config]

func init() {
	global.Store(Defaults())
}

func Defaults() *Config {
	lat, latOK := envFloat("RECEIVER_LAT")
	lon, lonOK := envFloat("RECEIVER_LON")
	return &Config{
		BurstGapUS:                DefaultBurstGapUS,
		BurstRecordMaxAgeS:        DefaultBurstRecordMaxAgeS,
		BurstRecordsMaxPerIID:     DefaultBurstRecordsMaxPerIID,
		CentroidHistoryMaxPerICAO: DefaultCentroidHistoryMaxPerICAO,
		MinBursts:                 DefaultMinBursts,
		MinQualifyingICAOs:        DefaultMinQualifyingICAOs,
		SocketPath:                DefaultSocketPath,
		ReceiverLat:               lat,
		ReceiverLon:               lon,
		ReceiverLatSet:            latOK,
		ReceiverLonSet:            lonOK,
		HasReceiver:               latOK && lonOK,
	}
}

// Get returns the current live config. The returned pointer is immutable.
func Get() *Config { return global.Load() }

// Apply updates a single key. Unknown keys are silently ignored.
func Apply(key string, value interface{}) {
	old := global.Load()
	next := *old // copy
	switch key {
	case "BURST_GAP_US":
		if v, ok := toFloat64(value); ok {
			next.BurstGapUS = v
		}
	case "BURST_RECORD_MAX_AGE_S":
		if v, ok := toFloat64(value); ok {
			next.BurstRecordMaxAgeS = v
		}
	case "BURST_RECORDS_MAX_PER_IID":
		if v, ok := toInt(value); ok {
			next.BurstRecordsMaxPerIID = v
		}
	case "CENTROID_HISTORY_MAX_PER_ICAO":
		if v, ok := toInt(value); ok {
			next.CentroidHistoryMaxPerICAO = v
		}
	case "MIN_BURSTS":
		if v, ok := toInt(value); ok {
			next.MinBursts = v
		}
	case "MIN_QUALIFYING_ICAOS":
		if v, ok := toInt(value); ok {
			next.MinQualifyingICAOs = v
		}
	case "RECEIVER_LAT":
		if v, ok := toFloat64(value); ok {
			next.ReceiverLat = v
			next.ReceiverLatSet = true
			next.HasReceiver = next.ReceiverLatSet && next.ReceiverLonSet
		}
	case "RECEIVER_LON":
		if v, ok := toFloat64(value); ok {
			next.ReceiverLon = v
			next.ReceiverLonSet = true
			next.HasReceiver = next.ReceiverLatSet && next.ReceiverLonSet
		}
	}
	global.Store(&next)
}

func envFloat(key string) (float64, bool) {
	v := os.Getenv(key)
	if v == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func toFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	}
	return 0, false
}

func toInt(v interface{}) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case uint64:
		return int(x), true
	case float64:
		return int(x), true
	}
	return 0, false
}
