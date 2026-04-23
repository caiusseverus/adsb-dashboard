package export

// RadarSnapshot is the stable exported top-level object for Python/UI consumers.
type RadarSnapshot struct {
	GeneratedAt       float64                 `codec:"generated_at" json:"generated_at"`
	UptimeS           float64                 `codec:"uptime_s" json:"uptime_s"`
	EventsIn          uint64                  `codec:"events_in" json:"events_in"`
	BurstsFired       uint64                  `codec:"bursts_fired" json:"bursts_fired"`
	FramesEmitted     uint64                  `codec:"frames_emitted" json:"frames_emitted"`
	ActiveIIDs        int                     `codec:"active_iids" json:"active_iids"`
	IIDs              map[string]IIDSnapshot  `codec:"iids" json:"iids"`
	SweepFrames       []SweepFrame            `codec:"sweep_frames" json:"sweep_frames"`
	EvidenceEvents    []EvidenceEvent         `codec:"evidence_events" json:"evidence_events"`
	TrackObservations []TrackObservation      `codec:"track_observations" json:"track_observations"`
	Profile           map[string]StageProfile `codec:"profile" json:"profile"`
}

// IIDSnapshot is the stable compact exported per-IID view.
type IIDSnapshot struct {
	IID                 uint8    `codec:"iid" json:"iid"`
	Status              string   `codec:"status" json:"status"`
	PeriodS             *float64 `codec:"period_s" json:"period_s"`
	ReferenceICAO       *uint32  `codec:"reference_icao" json:"reference_icao"`
	SyncQuality         float64  `codec:"sync_quality" json:"sync_quality"`
	SyncStateUsable     bool     `codec:"sync_state_usable" json:"sync_state_usable"`
	SyncStatePresent    bool     `codec:"sync_state_present" json:"sync_state_present"`
	SyncPeriodS         *float64 `codec:"sync_period_s" json:"sync_period_s"`
	SyncPhaseEpochUS    *float64 `codec:"sync_phase_epoch_us" json:"sync_phase_epoch_us"`
	SyncPhaseOffsetDeg  *float64 `codec:"sync_phase_offset_deg" json:"sync_phase_offset_deg"`
	SyncJitterDeg       *float64 `codec:"sync_jitter_deg" json:"sync_jitter_deg"`
	SyncResidualEMADeg  *float64 `codec:"sync_residual_ema_deg" json:"sync_residual_ema_deg"`
	SyncLastResidualDeg *float64 `codec:"sync_last_residual_deg" json:"sync_last_residual_deg"`
	SyncNFrames         int      `codec:"sync_n_frames" json:"sync_n_frames"`
	SyncNRejectedFrames int      `codec:"sync_n_rejected_frames" json:"sync_n_rejected_frames"`
	SyncHoldover        bool     `codec:"sync_holdover" json:"sync_holdover"`
	SyncLastUpdated     *float64 `codec:"sync_last_updated" json:"sync_last_updated"`
	RetainedBurstSpan   float64  `codec:"retained_burst_span_s" json:"retained_burst_span_s"`
}

// SweepFrame is the stable exported frame DTO.
type SweepFrame struct {
	IID          uint8                   `codec:"iid" json:"iid"`
	FrameIndex   uint32                  `codec:"frame_index" json:"frame_index"`
	PeriodS      float64                 `codec:"period_s" json:"period_s"`
	RefICAO      uint32                  `codec:"ref_icao" json:"ref_icao"`
	RefLat       float64                 `codec:"ref_lat" json:"ref_lat"`
	RefLon       float64                 `codec:"ref_lon" json:"ref_lon"`
	RefArrivalUS float64                 `codec:"ref_arrival_us" json:"ref_arrival_us"`
	RefPosAgeS   *float32                `codec:"ref_pos_age_s" json:"ref_pos_age_s"`
	Quality      string                  `codec:"quality" json:"quality"`
	ExportedAt   float64                 `codec:"exported_at" json:"exported_at"`
	Observations []SweepFrameObservation `codec:"observations" json:"observations"`
}

// SweepFrameObservation is one non-reference aircraft within an exported frame.
type SweepFrameObservation struct {
	ICAO      uint32  `codec:"icao" json:"icao"`
	Lat       float64 `codec:"lat" json:"lat"`
	Lon       float64 `codec:"lon" json:"lon"`
	ArrivalUS float64 `codec:"arrival_us" json:"arrival_us"`
	NReplies  uint8   `codec:"n_replies" json:"n_replies"`
	PosAgeS   float32 `codec:"position_age_s" json:"position_age_s"`
}

// EvidenceEvent is the stable exported compact evidence DTO.
type EvidenceEvent struct {
	Kind               string   `codec:"kind" json:"kind"`
	IID                uint8    `codec:"iid" json:"iid"`
	ICAO               uint32   `codec:"icao" json:"icao"`
	ArrivalUS          float64  `codec:"arrival_us" json:"arrival_us"`
	SimpleCentroidUS   *float64 `codec:"simple_centroid_us" json:"simple_centroid_us"`
	WeightedCentroidUS *float64 `codec:"weighted_centroid_us" json:"weighted_centroid_us"`
	CentroidDeltaUS    *float64 `codec:"centroid_delta_us" json:"centroid_delta_us"`
	FirstReplyUS       *float64 `codec:"first_reply_us" json:"first_reply_us"`
	StrongestReplyUS   *float64 `codec:"strongest_reply_us" json:"strongest_reply_us"`
	MidStrongWindowUS  *float64 `codec:"mid_strong_window_us" json:"mid_strong_window_us"`
	LastReplyUS        *float64 `codec:"last_reply_us" json:"last_reply_us"`
	SpanUS             *float64 `codec:"span_us" json:"span_us"`
	PeakAmplitude      *float32 `codec:"peak_amplitude" json:"peak_amplitude"`
	WallTS             float64  `codec:"wall_ts" json:"wall_ts"`
	NReplies           uint8    `codec:"n_replies" json:"n_replies"`
	SignalDBFS         *float32 `codec:"signal_dbfs" json:"signal_dbfs"`
	TruthLat           *float64 `codec:"truth_lat" json:"truth_lat"`
	TruthLon           *float64 `codec:"truth_lon" json:"truth_lon"`
	PositionAgeS       *float32 `codec:"position_age_s" json:"position_age_s"`
	DominantFamily     bool     `codec:"dominant_family" json:"dominant_family"`
	// SyncEligible is the legacy compact/bootstrap sync-admission bit. It aliases
	// CompactSyncEligible for backward compatibility and does not mean refined
	// multi-aircraft sync is already usable.
	SyncEligible          bool    `codec:"sync_eligible" json:"sync_eligible"`
	CompactSyncEligible   bool    `codec:"compact_sync_eligible" json:"compact_sync_eligible"`
	RefinedSyncPresent    bool    `codec:"refined_sync_present" json:"refined_sync_present"`
	RefinedSyncUsable     bool    `codec:"refined_sync_usable" json:"refined_sync_usable"`
	AssociationConfidence float32 `codec:"association_confidence" json:"association_confidence"`
}

// TrackObservation is the stable exported Stage 3 observation DTO.
type TrackObservation struct {
	IID                   uint8    `codec:"iid" json:"iid"`
	ICAO                  uint32   `codec:"icao" json:"icao"`
	ArrivalUS             float64  `codec:"arrival_us" json:"arrival_us"`
	WallTS                float64  `codec:"wall_ts" json:"wall_ts"`
	SignalDBFS            *float32 `codec:"signal_dbfs" json:"signal_dbfs"`
	TruthLat              *float64 `codec:"truth_lat" json:"truth_lat"`
	TruthLon              *float64 `codec:"truth_lon" json:"truth_lon"`
	PositionAgeS          *float32 `codec:"position_age_s" json:"position_age_s"`
	AssociationConfidence float32  `codec:"association_confidence" json:"association_confidence"`
	DominantFamily        bool     `codec:"dominant_family" json:"dominant_family"`
	// SyncEligible is the legacy compact/bootstrap sync-admission bit. It aliases
	// CompactSyncEligible for backward compatibility and does not mean refined
	// multi-aircraft sync is already usable.
	SyncEligible        bool `codec:"sync_eligible" json:"sync_eligible"`
	CompactSyncEligible bool `codec:"compact_sync_eligible" json:"compact_sync_eligible"`
	RefinedSyncPresent  bool `codec:"refined_sync_present" json:"refined_sync_present"`
	RefinedSyncUsable   bool `codec:"refined_sync_usable" json:"refined_sync_usable"`
}

// StageProfile is one exported timing aggregate.
type StageProfile struct {
	Count   uint64  `codec:"count" json:"count"`
	TotalMS float64 `codec:"total_ms" json:"total_ms"`
	MaxMS   float64 `codec:"max_ms" json:"max_ms"`
	MeanMS  float64 `codec:"mean_ms" json:"mean_ms"`
}
