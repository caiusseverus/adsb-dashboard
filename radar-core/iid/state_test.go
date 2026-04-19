package iid

import "testing"

func TestRefreshReference_PrefersDominantFamily(t *testing.T) {
	s := NewIIDState(7)
	period := 4.0
	s.PeriodS = &period
	s.LastRotationModel = &RotationModel{
		Family: &ICAOFamily{
			FoldedICAOs:   map[uint32]struct{}{0xAAAAAA: {}},
			ResidualICAOs: map[uint32]struct{}{0xBBBBBB: {}},
		},
	}
	current := uint32(0xBBBBBB)
	s.RefICAO = &current

	records := []BurstRecord{
		{ICAO: 0xAAAAAA, CentroidUS: 0},
		{ICAO: 0xAAAAAA, CentroidUS: 4_000_000},
		{ICAO: 0xAAAAAA, CentroidUS: 8_000_000},
		{ICAO: 0xAAAAAA, CentroidUS: 12_000_000},
		{ICAO: 0xBBBBBB, CentroidUS: 0},
		{ICAO: 0xBBBBBB, CentroidUS: 4_100_000},
		{ICAO: 0xBBBBBB, CentroidUS: 8_200_000},
		{ICAO: 0xBBBBBB, CentroidUS: 12_300_000},
	}
	s.RefreshReference(records, 12_000_000)

	if s.RefICAO == nil {
		t.Fatal("reference ICAO was cleared, expected dominant-family candidate")
	}
	if *s.RefICAO != 0xAAAAAA {
		t.Fatalf("reference ICAO=0x%X, want 0xAAAAAA", *s.RefICAO)
	}
}

func TestRefreshReference_ClearsWhenNoDominantCandidates(t *testing.T) {
	s := NewIIDState(9)
	period := 4.0
	s.PeriodS = &period
	s.LastRotationModel = &RotationModel{
		Family: &ICAOFamily{
			FoldedICAOs:   map[uint32]struct{}{0xCCCCCC: {}}, // not present in records
			ResidualICAOs: map[uint32]struct{}{0xBBBBBB: {}},
		},
	}
	current := uint32(0xBBBBBB)
	s.RefICAO = &current

	records := []BurstRecord{
		{ICAO: 0xBBBBBB, CentroidUS: 0},
		{ICAO: 0xBBBBBB, CentroidUS: 4_000_000},
		{ICAO: 0xBBBBBB, CentroidUS: 8_000_000},
		{ICAO: 0xBBBBBB, CentroidUS: 12_000_000},
	}
	s.RefreshReference(records, 12_000_000)

	if s.RefICAO != nil {
		t.Fatalf("reference ICAO not cleared, got 0x%X", *s.RefICAO)
	}
}
