package main

import (
	"math"
	"testing"
	"time"

	"github.com/caiusseverus/adsb-dashboard/radar-core/iid"
)

func TestBearingDegCardinalDirections(t *testing.T) {
	originLat := 51.5
	originLon := -0.1

	north := bearingDeg(originLat, originLon, originLat+0.1, originLon)
	east := bearingDeg(originLat, originLon, originLat, originLon+0.1)
	south := bearingDeg(originLat, originLon, originLat-0.1, originLon)
	west := bearingDeg(originLat, originLon, originLat, originLon-0.1)

	if math.Abs(north-0.0) > 3.0 {
		t.Fatalf("north bearing=%.2f, want near 0", north)
	}
	if math.Abs(east-90.0) > 3.0 {
		t.Fatalf("east bearing=%.2f, want near 90", east)
	}
	if math.Abs(south-180.0) > 3.0 {
		t.Fatalf("south bearing=%.2f, want near 180", south)
	}
	if math.Abs(west-270.0) > 3.0 {
		t.Fatalf("west bearing=%.2f, want near 270", west)
	}
}

func TestComputeSyncPopulationDiagnostics_UsesBurstNeighborhoodPositionedDominantCount(t *testing.T) {
	e := newEngine()
	s := iid.NewIIDState(7)
	base := 4.0
	s.SetBasePeriod(base)
	folded := map[uint32]struct{}{
		0xAA: {},
		0xBB: {},
		0xCC: {},
	}
	s.ApplyRotation(&iid.RotationModel{
		DominantPeriodS: &base,
		Family: &iid.ICAOFamily{
			FoldedICAOs:   folded,
			ResidualICAOs: map[uint32]struct{}{0xDD: {}},
		},
	})

	now := float64(time.Now().Unix())
	e.positions.Update(0xAA, 51.5, -0.1, nil, now)
	e.positions.Update(0xBB, 51.6, -0.1, nil, now)
	e.positions.Update(0xCC, 51.7, -0.1, nil, now)

	// Nearby bursts: 3 dominant + positioned aircraft.
	s.AddBurst(0xAA, 100_000_000.0, 3, 3)
	s.AddBurst(0xBB, 100_100_000.0, 3, 3)
	s.AddBurst(0xCC, 100_200_000.0, 3, 3)
	// Residual family burst should be excluded by dominant-family filter.
	s.AddBurst(0xDD, 100_300_000.0, 3, 4)

	diag := e.computeSyncPopulationDiagnostics(s, 0xAA, 100_000_000.0)
	if diag.rawCandidateAircraftCount < 3 {
		t.Fatalf("raw candidate count=%d, want >=3", diag.rawCandidateAircraftCount)
	}
	if diag.positionedAircraftCount != 3 {
		t.Fatalf("positioned count=%d, want 3", diag.positionedAircraftCount)
	}
	if diag.dominantAircraftCount != 3 {
		t.Fatalf("dominant count=%d, want 3", diag.dominantAircraftCount)
	}
	if diag.excludedNotDominant < 1 {
		t.Fatalf("expected excluded_not_dominant >=1, got %d", diag.excludedNotDominant)
	}
}
