package protocol

import (
	"fmt"

	"github.com/ugorji/go/codec"
)

// mh is the shared msgpack handle. Configured once; safe for concurrent use.
var mh = func() *codec.MsgpackHandle {
	h := &codec.MsgpackHandle{}
	h.MapType = nil // decode maps as map[string]interface{}
	h.RawToString = true
	return h
}()

// Encode serialises any message struct to msgpack bytes.
func Encode(msg interface{}) ([]byte, error) {
	var buf []byte
	enc := codec.NewEncoderBytes(&buf, mh)
	if err := enc.Encode(msg); err != nil {
		return nil, err
	}
	return buf, nil
}

// Decode deserialises a raw msgpack payload into the correct concrete type,
// dispatching on the "t" field.
func Decode(payload []byte) (interface{}, error) {
	var peek struct {
		MsgType uint8 `codec:"t"`
	}
	dec := codec.NewDecoderBytes(payload, mh)
	if err := dec.Decode(&peek); err != nil {
		return nil, fmt.Errorf("protocol: peek failed: %w", err)
	}
	return decodeTyped(peek.MsgType, payload)
}

func decodeTyped(msgType uint8, payload []byte) (interface{}, error) {
	newDec := func() *codec.Decoder { return codec.NewDecoderBytes(payload, mh) }
	switch msgType {
	case MsgRadarEvent:
		var m RadarEvent
		return &m, newDec().Decode(&m)
	case MsgPositionUpdate:
		var m PositionUpdate
		return &m, newDec().Decode(&m)
	case MsgConfigUpdate:
		var m ConfigUpdate
		return &m, newDec().Decode(&m)
	case MsgSnapshotReq:
		var m SnapshotReq
		return &m, newDec().Decode(&m)
	case MsgResetIID:
		var m ResetIID
		return &m, newDec().Decode(&m)
	case MsgBurstFired:
		var m BurstFired
		return &m, newDec().Decode(&m)
	case MsgFrameReady:
		var m FrameReady
		return &m, newDec().Decode(&m)
	case MsgIIDState:
		var m IIDState
		return &m, newDec().Decode(&m)
	case MsgSnapshotResp:
		var m SnapshotResp
		return &m, newDec().Decode(&m)
	case MsgHealth:
		var m Health
		return &m, newDec().Decode(&m)
	case MsgFMFrameResult:
		var m FMFrameResult
		return &m, newDec().Decode(&m)
	case MsgFMState:
		var m FMState
		return &m, newDec().Decode(&m)
	default:
		return nil, fmt.Errorf("protocol: unknown message type %d", msgType)
	}
}
