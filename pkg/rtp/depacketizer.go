package rtp

import (
	"errors"
	"fmt"

	"github.com/pion/rtp"
)

// MediaType represents either Audio or Video track data.
type MediaType int

const (
	MediaAudio MediaType = iota
	MediaVideo
)

func (m MediaType) String() string {
	switch m {
	case MediaAudio:
		return "audio"
	case MediaVideo:
		return "video"
	default:
		return "unknown"
	}
}

// Depacketizer interface abstracts media unmarshalling from RTP packets.
type Depacketizer interface {
	Depacketize(packet *rtp.Packet) ([]byte, bool, error)
	Codec() string
	Type() MediaType
}

// VP8Depacketizer handles VP8 RTP payload extraction (RFC 7741).
type VP8Depacketizer struct{}

// NewVP8Depacketizer creates a VP8 RTP payload parser.
func NewVP8Depacketizer() *VP8Depacketizer {
	return &VP8Depacketizer{}
}

func (d *VP8Depacketizer) Codec() string {
	return "VP8"
}

func (d *VP8Depacketizer) Type() MediaType {
	return MediaVideo
}

// Depacketize unpacks VP8 payload, checking for Keyframe indicators.
// Returns payload, isKeyframe, error.
func (d *VP8Depacketizer) Depacketize(packet *rtp.Packet) ([]byte, bool, error) {
	if packet == nil || len(packet.Payload) < 1 {
		return nil, false, errors.New("empty packet payload")
	}

	payload := packet.Payload
	// VP8 Payload Descriptor
	// First byte: X|R|N|S|R|PID (S=Start of VP8 partition)
	firstByte := payload[0]
	hasExtension := (firstByte & 0x80) != 0
	isStartOfPartition := (firstByte & 0x10) != 0

	index := 1
	if hasExtension && len(payload) > index {
		extByte := payload[index]
		index++
		hasPictureID := (extByte & 0x80) != 0
		hasTL0PICIDX := (extByte & 0x40) != 0
		hasTIDOrKEYIDX := (extByte & 0x20) != 0 || (extByte & 0x10) != 0

		if hasPictureID && len(payload) > index {
			if (payload[index] & 0x80) != 0 {
				index += 2 // 15-bit PictureID
			} else {
				index += 1 // 7-bit PictureID
			}
		}

		if hasTL0PICIDX && len(payload) > index {
			index++
		}

		if hasTIDOrKEYIDX && len(payload) > index {
			index++
		}
	}

	if index >= len(payload) {
		return nil, false, errors.New("malformed VP8 payload descriptor")
	}

	rawPayload := payload[index:]
	isKeyframe := false

	// If start of VP8 partition, inspect frame header
	// Keyframe: bit 0 of 3-byte uncompressed header is 0 (P-bit = 0)
	if isStartOfPartition && len(rawPayload) >= 3 {
		isKeyframe = (rawPayload[0] & 0x01) == 0
	}

	return rawPayload, isKeyframe, nil
}

// OpusDepacketizer handles Opus audio depacketization (RFC 7587).
type OpusDepacketizer struct{}

// NewOpusDepacketizer returns an Opus RTP payload parser.
func NewOpusDepacketizer() *OpusDepacketizer {
	return &OpusDepacketizer{}
}

func (d *OpusDepacketizer) Codec() string {
	return "OPUS"
}

func (d *OpusDepacketizer) Type() MediaType {
	return MediaAudio
}

// Depacketize extracts raw Opus frame from payload.
func (d *OpusDepacketizer) Depacketize(packet *rtp.Packet) ([]byte, bool, error) {
	if packet == nil || len(packet.Payload) == 0 {
		return nil, false, errors.New("empty opus payload")
	}
	// Opus packets are self-contained frames, treat as keyframes/independent frames
	return packet.Payload, true, nil
}

// H264Depacketizer handles H264 NALU payload unpacking (RFC 6184).
type H264Depacketizer struct{}

// NewH264Depacketizer returns an H264 RTP payload parser.
func NewH264Depacketizer() *H264Depacketizer {
	return &H264Depacketizer{}
}

func (d *H264Depacketizer) Codec() string {
	return "H264"
}

func (d *H264Depacketizer) Type() MediaType {
	return MediaVideo
}

// Depacketize extracts H264 NAL units and identifies IDR Keyframes (NAL Type 5).
func (d *H264Depacketizer) Depacketize(packet *rtp.Packet) ([]byte, bool, error) {
	if packet == nil || len(packet.Payload) < 1 {
		return nil, false, errors.New("empty h264 payload")
	}

	payload := packet.Payload
	naluType := payload[0] & 0x1F
	isKeyframe := false

	switch naluType {
	case 1, 2, 3, 4:
		// Non-IDR Slice
		isKeyframe = false
		return payload, isKeyframe, nil
	case 5:
		// IDR Keyframe Slice
		isKeyframe = true
		return payload, isKeyframe, nil
	case 7, 8:
		// SPS / PPS parameter sets indicate keyframe stream reset
		isKeyframe = true
		return payload, isKeyframe, nil
	case 28:
		// FU-A Fragmented Unit
		if len(payload) < 2 {
			return nil, false, errors.New("malformed FU-A payload")
		}
		fuHeader := payload[1]
		isStart := (fuHeader & 0x80) != 0
		fuType := fuHeader & 0x1F
		if isStart && (fuType == 5 || fuType == 7) {
			isKeyframe = true
		}
		return payload[2:], isKeyframe, nil
	default:
		return payload, false, nil
	}
}

// GetDepacketizer factory returns the appropriate depacketizer for codec name.
func GetDepacketizer(codec string) (Depacketizer, error) {
	switch codec {
	case "vp8", "VP8":
		return NewVP8Depacketizer(), nil
	case "opus", "OPUS":
		return NewOpusDepacketizer(), nil
	case "h264", "H264":
		return NewH264Depacketizer(), nil
	default:
		return nil, fmt.Errorf("unsupported codec: %s", codec)
	}
}
