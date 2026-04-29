package main

import (
	"math"
	"testing"
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
