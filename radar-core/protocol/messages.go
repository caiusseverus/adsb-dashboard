package protocol

// Message type constants — inbound (Python → radar-core).
const (
	MsgRadarEvent    uint8 = 1
	MsgPositionUpdate uint8 = 2
	MsgConfigUpdate  uint8 = 3
	MsgSnapshotReq   uint8 = 4
	MsgResetIID      uint8 = 5
	// 6 reserved (was SHUTDOWN — now handled via SIGTERM, not protocol)
)

// Message type constants — outbound (radar-core → Python).
const (
	MsgBurstFired   uint8 = 10
	MsgFrameReady   uint8 = 11
	MsgIIDState     uint8 = 12
	MsgSnapshotResp uint8 = 13
	MsgHealth       uint8 = 14
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
	MsgType uint8    `codec:"t"`
	ICAO    uint32   `codec:"c"`
	Lat     float64  `codec:"la"`
	Lon     float64  `codec:"lo"`
	AltFt   *int32   `codec:"al"`
	TS      float64  `codec:"ts"`
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
	MsgType       uint8    `codec:"t"`
	IID           uint8    `codec:"i"`
	ICAO          uint32   `codec:"c"`
	CentroidUS    float64  `codec:"cu"`
	NReplies      uint8    `codec:"n"`
	SignalDBFS    *float32 `codec:"s"`
	Lat           *float64 `codec:"la"`
	Lon           *float64 `codec:"lo"`
	BearingDeg    *float32 `codec:"br"`
	RangeNM       *float32 `codec:"rn"`
	PosAgeS       *float32 `codec:"pa"`
	DominantFamily bool    `codec:"df"`
	SyncEligible  bool     `codec:"se"`
}

// FrameObservation is one non-reference aircraft within a SweepFrame.
type FrameObservation struct {
	ICAO      uint32   `codec:"c"`
	Lat       float64  `codec:"la"`
	Lon       float64  `codec:"lo"`
	ArrivalUS float64  `codec:"a"`
	NReplies  uint8    `codec:"n"`
	PosAgeS   float32  `codec:"pa"`
}

// FrameReady is emitted when a sweep frame is complete and ready for FM solve.
// This message is intentionally richer than other operational messages — it
// carries full per-aircraft position data to allow the Python FM solve to
// operate without changes. This is a temporary design compromise; see the
// design brief §2 for the planned long-term replacement.
type FrameReady struct {
	MsgType      uint8              `codec:"t"`
	IID          uint8              `codec:"i"`
	FrameIndex   uint32             `codec:"fi"`
	PeriodS      float64            `codec:"p"`
	RefICAO      uint32             `codec:"rc"`
	RefLat       float64            `codec:"rla"`
	RefLon       float64            `codec:"rlo"`
	RefArrivalUS float64            `codec:"ra"`
	Observations []FrameObservation `codec:"obs"`
	Quality      string             `codec:"q"`
}

// IIDState is emitted after each rotation model update for one IID.
// Revision is a monotonically increasing counter per IID; Python discards
// messages with a revision lower than the last seen.
type IIDState struct {
	MsgType       uint8    `codec:"t"`
	IID           uint8    `codec:"i"`
	PeriodS       *float64 `codec:"p"`
	RPM           *float32 `codec:"rpm"`
	Status        string   `codec:"st"`
	RefICAO       *uint32  `codec:"rc"`
	SyncQuality   float32  `codec:"sq"`
	NBurstRecords uint16   `codec:"nb"`
	LastUpdated   float64  `codec:"lu"`
	Revision      uint32   `codec:"rv"`
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
	MsgType      uint8   `codec:"t"`
	UptimeS      float64 `codec:"up"`
	EventsIn     uint64  `codec:"ei"`
	BurstsFired  uint64  `codec:"bf"`
	FramesEmitted uint64 `codec:"fe"`
	QueueDepth   uint16  `codec:"qd"`
	DropCount    uint32  `codec:"dc"`
	ActiveIIDs   uint8   `codec:"ai"`
}
