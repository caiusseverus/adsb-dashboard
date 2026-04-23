package protocol

// Message type constants — inbound (Python → radar-core).
const (
	MsgRadarEvent     uint8 = 1
	MsgPositionUpdate uint8 = 2
	MsgConfigUpdate   uint8 = 3
	MsgSnapshotReq    uint8 = 4
	MsgResetIID       uint8 = 5
	// 6 reserved (was SHUTDOWN — now handled via SIGTERM, not protocol)
)

// Message type constants — outbound (radar-core → Python).
const (
	MsgBurstFired      uint8 = 10
	MsgFrameReady      uint8 = 11
	MsgIIDState        uint8 = 12
	MsgSnapshotResp    uint8 = 13
	MsgHealth          uint8 = 14
	MsgFMFrameResult   uint8 = 15
	MsgFMState         uint8 = 16
	MsgMultiSyncState  uint8 = 17 // per-IID multi-aircraft sync refinement state
)

// --- Inbound messages ---

// RadarEvent is one decoded DF11 IID reply extracted from a Beast frame.
// arrival_us is the Beast hardware timestamp in microseconds (unwrapped monotonic).
type RadarEvent struct {
	MsgType    uint8    `codec:"t"`
	ArrivalUS  float64  `codec:"a"`
	IID        uint8    `codec:"i"`
	ICAO       uint32   `codec:"c"`
	SignalDBFS *float32 `codec:"s"`
}

// PositionUpdate carries the latest ADS-B position for one aircraft.
// ts is the wall-clock epoch when the position was last seen.
type PositionUpdate struct {
	MsgType uint8   `codec:"t"`
	ICAO    uint32  `codec:"c"`
	Lat     float64 `codec:"la"`
	Lon     float64 `codec:"lo"`
	AltFt   *int32  `codec:"al"`
	TS      float64 `codec:"ts"`
}

// ConfigUpdate sets one operational knob by name.
// Value is any msgpack-compatible scalar (string, int, float, bool).
type ConfigUpdate struct {
	MsgType uint8       `codec:"t"`
	Key     string      `codec:"k"`
	Value   interface{} `codec:"v"`
}

// SnapshotReq asks radar-core to return a SnapshotResp.
// Scope is "all", "iid:N" (N decimal), or "health".
type SnapshotReq struct {
	MsgType uint8  `codec:"t"`
	ReqID   uint32 `codec:"r"`
	Scope   string `codec:"sc"`
}

// ResetIID clears all per-IID operational state for one IID.
type ResetIID struct {
	MsgType uint8 `codec:"t"`
	IID     uint8 `codec:"i"`
}

// --- Outbound messages ---

// BurstFired is emitted when a burst window closes and the centroid is computed.
// Position fields are nil if no ADS-B fix was available within the freshness window.
type BurstFired struct {
	MsgType            uint8    `codec:"t"`
	IID                uint8    `codec:"i"`
	ICAO               uint32   `codec:"c"`
	CentroidUS         float64  `codec:"cu"`
	SimpleCentroidUS   *float64 `codec:"cs"`
	WeightedCentroidUS *float64 `codec:"cw"`
	CentroidDeltaUS    *float64 `codec:"cd"`
	FirstReplyUS       *float64 `codec:"cf"`
	StrongestReplyUS   *float64 `codec:"ct"`
	MidStrongWindowUS  *float64 `codec:"cm"`
	LastReplyUS        *float64 `codec:"cl"`
	SpanUS             *float64 `codec:"cp"`
	PeakAmplitude      *float32 `codec:"pk"`
	NReplies           uint8    `codec:"n"`
	SignalDBFS         *float32 `codec:"s"`
	Lat                *float64 `codec:"la"`
	Lon                *float64 `codec:"lo"`
	BearingDeg         *float32 `codec:"br"`
	RangeNM            *float32 `codec:"rn"`
	PosAgeS            *float32 `codec:"pa"`
	DominantFamily     bool     `codec:"df"`
	SyncEligible       bool     `codec:"se"`
}

// FrameObservation is one non-reference aircraft within a SweepFrame.
type FrameObservation struct {
	ICAO      uint32  `codec:"c"`
	Lat       float64 `codec:"la"`
	Lon       float64 `codec:"lo"`
	ArrivalUS float64 `codec:"a"`
	NReplies  uint8   `codec:"n"`
	PosAgeS   float32 `codec:"pa"`
}

// FrameReady is emitted when a sweep frame is complete. The same payload is
// consumed internally by radar-core's FM worker; Python may still subscribe to
// it for diagnostics or optional shadow compatibility, but it is no longer the
// default operational FM solve boundary.
//
// RefPosAgeS carries the wall-clock age (in seconds) of the reference
// aircraft's ADS-B position fix at the time the frame was opened.  Nil means
// the age was not available (receivers should not treat nil as "fresh").
type FrameReady struct {
	MsgType      uint8              `codec:"t"`
	IID          uint8              `codec:"i"`
	FrameIndex   uint32             `codec:"fi"`
	PeriodS      float64            `codec:"p"`
	RefICAO      uint32             `codec:"rc"`
	RefLat       float64            `codec:"rla"`
	RefLon       float64            `codec:"rlo"`
	RefArrivalUS float64            `codec:"ra"`
	RefPosAgeS   *float32           `codec:"rpa"`
	Observations []FrameObservation `codec:"obs"`
	Quality      string             `codec:"q"`
}

// IIDState is emitted after each rotation model update for one IID.
// Revision is a monotonically increasing counter per IID; Python discards
// messages with a revision lower than the last seen.
type IIDState struct {
	MsgType            uint8    `codec:"t"`
	IID                uint8    `codec:"i"`
	PeriodS            *float64 `codec:"p"`
	RPM                *float32 `codec:"rpm"`
	Status             string   `codec:"st"`
	RefICAO            *uint32  `codec:"rc"`
	SyncQuality        float32  `codec:"sq"`
	SyncStatePresent   bool     `codec:"sp"`
	SyncStateUsable    bool     `codec:"su"`
	SyncPeriodS        *float64 `codec:"sps"`
	SyncPhaseEpochUS   *float64 `codec:"sep"`
	SyncPhaseOffsetDeg *float64 `codec:"sod"`
	SyncJitterDeg      *float32 `codec:"sj"`
	SyncResidualEMA    *float32 `codec:"sre"`
	SyncLastResidual   *float32 `codec:"slr"`
	SyncNFrames        uint16   `codec:"snf"`
	SyncNRejected      uint16   `codec:"snr"`
	SyncHoldover       bool     `codec:"sh"`
	NBurstRecords      uint16   `codec:"nb"`
	LastUpdated        float64  `codec:"lu"`
	Revision           uint32   `codec:"rv"`
}

// SnapshotResp is the response to a SnapshotReq.
// Payload is a free-form msgpack map; exact shape depends on the requested scope.
type SnapshotResp struct {
	MsgType uint8                  `codec:"t"`
	ReqID   uint32                 `codec:"r"`
	Payload map[string]interface{} `codec:"pl"`
}

// Health is emitted every 10 seconds as a liveness signal.
type Health struct {
	MsgType       uint8   `codec:"t"`
	UptimeS       float64 `codec:"up"`
	EventsIn      uint64  `codec:"ei"`
	BurstsFired   uint64  `codec:"bf"`
	FramesEmitted uint64  `codec:"fe"`
	QueueDepth    uint16  `codec:"qd"`
	DropCount     uint32  `codec:"dc"`
	ActiveIIDs    uint8   `codec:"ai"`
}

// FMFrameResult is emitted after radar-core solves/adjudicates one sweep frame.
type FMFrameResult struct {
	MsgType                  uint8    `codec:"t"`
	IID                      uint8    `codec:"i"`
	FrameIndex               uint32   `codec:"fi"`
	SweepStartUS             float64  `codec:"su"`
	Success                  bool     `codec:"ok"`
	SolveStatus              string   `codec:"ss"`
	SolveReason              string   `codec:"sr"`
	CandidatePosition        bool     `codec:"cp"`
	AccumAccepted            bool     `codec:"aa"`
	RejectionReason          string   `codec:"rr"`
	AdmissionTier            string   `codec:"at"`
	Lat                      *float64 `codec:"la"`
	Lon                      *float64 `codec:"lo"`
	CEPM                     *float64 `codec:"cep"`
	NContributingArcs        uint16   `codec:"na"`
	AzimuthSpreadDeg         float32  `codec:"az"`
	Weight                   float64  `codec:"w"`
	PairwiseRMSDeg           float32  `codec:"rms"`
	ClusterMemberCount       uint16   `codec:"cm"`
	SecondClusterMemberCount uint16   `codec:"sm"`
	MemberDominanceRatio     float32  `codec:"md"`
	WeightDominanceRatio     float32  `codec:"wd"`
	SupportDominanceRatio    float32  `codec:"sd"`
	BestClusterSupportScore  float32  `codec:"bs"`
	AmbiguitySameLobeBypass  bool     `codec:"sl"`
	ProcessedAt              float64  `codec:"ts"`
}

// FMState is the compact per-IID accumulated FM state authored by radar-core.
type FMState struct {
	MsgType                       uint8             `codec:"t"`
	IID                           uint8             `codec:"i"`
	FramesReachingSolver          uint64            `codec:"fr"`
	SolverSuccess                 uint64            `codec:"sc"`
	CandidatePositions            uint64            `codec:"cp"`
	SolverNoCandidate             uint64            `codec:"nc"`
	AccumulationRejected          uint64            `codec:"ar"`
	AccumulationRejectionReasons  map[string]uint64 `codec:"rr"`
	AccumulationAccepted          uint64            `codec:"aa"`
	AccumulationAcceptanceTiers   map[string]uint64 `codec:"at"`
	AccumulationWritten           uint64            `codec:"aw"`
	LastRejectionReason           string            `codec:"lr"`
	LastAdmissionTier             string            `codec:"lt"`
	AccumulatedFramePositions     uint32            `codec:"af"`
	CentroidAvailable             bool              `codec:"ca"`
	CentroidLat                   *float64          `codec:"la"`
	CentroidLon                   *float64          `codec:"lo"`
	CentroidCEPM                  *float64          `codec:"cep"`
	CentroidInliers               uint32            `codec:"ci"`
	CentroidTotal                 uint32            `codec:"ct"`
	CentroidStage0Survivors       uint32            `codec:"cs"`
	CentroidRejectionCounts       map[string]int    `codec:"cr"`
	LastFrameSuccess              bool              `codec:"fs"`
	LastFrameReason               string            `codec:"frr"`
	LastFrameIndex                uint32            `codec:"fi"`
	LastFrameHadCandidatePosition bool              `codec:"fh"`
	LastSolveStatus               string            `codec:"lss"`
	LastSolveReason               string            `codec:"lsr"`
	LastAmbiguitySameLobeBypass   bool              `codec:"lsl"`
	LastClusterMemberCount        uint16            `codec:"lcm"`
	LastSecondClusterMemberCount  uint16            `codec:"lsm"`
	LastSupportDominanceRatio     float32           `codec:"lsd"`
	LastPairwiseRMSDeg            float32           `codec:"lpr"`
	LastFrameLat                  *float64          `codec:"fla"`
	LastFrameLon                  *float64          `codec:"flo"`
	LastFrameCEPM                 *float64          `codec:"fcep"`
	UpdatedAt                     float64           `codec:"ts"`
}

// MultiSyncState is emitted after each multi-aircraft sync solver run.
// It carries the full per-IID refined sync state so Python can mirror it
// as LiveSyncState(source="go_multi_aircraft_burst") without running the
// Python multi-aircraft sync solver.
type MultiSyncState struct {
	MsgType               uint8    `codec:"t"`
	IID                   uint8    `codec:"i"`
	Present               bool     `codec:"pr"`
	Usable                bool     `codec:"us"`
	PeriodS               float64  `codec:"p"`
	PeriodBaseS           float64  `codec:"pb"`
	PhaseEpochUS          float64  `codec:"pe"`
	PhaseOffsetDeg        float64  `codec:"po"`
	JitterDeg             float64  `codec:"jd"`
	ResidualEMADeg        float64  `codec:"re"`
	NSyncUpdates          uint32   `codec:"nu"`
	Holdover              bool     `codec:"ho"`
	PeriodReacquireActive bool     `codec:"ra"`
	PeriodReacquireReason string   `codec:"rr"`
	AnchorICAO            *uint32  `codec:"ai"`
	AnchorPhaseDeg        float64  `codec:"ap"`
	AnchorScore           float64  `codec:"as"`
	UpdatedAt             float64  `codec:"ts"`
	// Bootstrap / trust diagnostics (added for wrong-period escape tracking).
	BootstrapPeriodS        float64 `codec:"bp"`
	TrustedBasePeriodS      float64 `codec:"tb"`
	TrustUpdateStreak       uint16  `codec:"tu"`
	BaseClamped             bool    `codec:"bc"`
	BaseClampDiffPPM        float64 `codec:"bd"`
	WrongPeriodSuspect      bool    `codec:"ws"`
	ReacquireCandidatePeriod float64 `codec:"rcp"`
	ReacquireCandidateScore  float64 `codec:"rcs"`
}
