package rtp

import (
	"sync"
	"time"

	"github.com/pion/rtcp"
)

// RTCPHandler handles inbound and outbound RTCP feedback (NACK, PLI, FIR, REMB, RR).
type RTCPHandler struct {
	mu               sync.Mutex
	ssrc             uint32
	clockRate        uint32
	lastPliSent      time.Time
	pliThrottle      time.Duration
	lastNackSent     map[uint16]time.Time
	nackThrottle     time.Duration
	rtt              time.Duration
	lossFraction     uint8
	onPacketCallback func(pkt rtcp.Packet)
}

// RTCPHandlerConfig provides configuration parameters for feedback throttling.
type RTCPHandlerConfig struct {
	SSRC         uint32
	ClockRate    uint32
	PLIThrottle  time.Duration
	NACKThrottle time.Duration
	OnPacket     func(pkt rtcp.Packet)
}

// NewRTCPHandler constructs a production RTCP feedback controller.
func NewRTCPHandler(cfg RTCPHandlerConfig) *RTCPHandler {
	if cfg.PLIThrottle == 0 {
		cfg.PLIThrottle = 500 * time.Millisecond // Limit keyframe requests to 2/sec max
	}
	if cfg.NACKThrottle == 0 {
		cfg.NACKThrottle = 40 * time.Millisecond // Minimum interval before re-requesting same seq
	}
	if cfg.ClockRate == 0 {
		cfg.ClockRate = 90000 // default video clock rate
	}

	return &RTCPHandler{
		ssrc:             cfg.SSRC,
		clockRate:        cfg.ClockRate,
		pliThrottle:      cfg.PLIThrottle,
		nackThrottle:     cfg.NACKThrottle,
		lastNackSent:     make(map[uint16]time.Time),
		onPacketCallback: cfg.OnPacket,
	}
}

// BuildPLI creates a Picture Loss Indication (PLI) to request a keyframe from the publisher.
// Returns nil if throttled.
func (h *RTCPHandler) BuildPLI(senderSSRC uint32) *rtcp.PictureLossIndication {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	if now.Sub(h.lastPliSent) < h.pliThrottle {
		return nil // Throttled
	}

	h.lastPliSent = now
	pli := &rtcp.PictureLossIndication{
		SenderSSRC: senderSSRC,
		MediaSSRC:  h.ssrc,
	}

	if h.onPacketCallback != nil {
		h.onPacketCallback(pli)
	}
	return pli
}

// BuildFIR creates a Full Intra Request (FIR) for keyframe generation.
func (h *RTCPHandler) BuildFIR(senderSSRC uint32, seqNum uint8) *rtcp.FullIntraRequest {
	h.mu.Lock()
	defer h.mu.Unlock()

	fir := &rtcp.FullIntraRequest{
		SenderSSRC: senderSSRC,
		MediaSSRC:  h.ssrc,
		FIR: []rtcp.FIREntry{
			{
				SSRC:           h.ssrc,
				SequenceNumber: seqNum,
			},
		},
	}

	if h.onPacketCallback != nil {
		h.onPacketCallback(fir)
	}
	return fir
}

// BuildNACK generates TransportLayerNack packets for missing sequences, filtering throttled sequences.
func (h *RTCPHandler) BuildNACK(senderSSRC uint32, missingSeqs []uint16) *rtcp.TransportLayerNack {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(missingSeqs) == 0 {
		return nil
	}

	now := time.Now()
	var filtered []uint16

	// Clean old nack records
	for s, t := range h.lastNackSent {
		if now.Sub(t) > 2*time.Second {
			delete(h.lastNackSent, s)
		}
	}

	for _, seq := range missingSeqs {
		if lastTime, exists := h.lastNackSent[seq]; !exists || now.Sub(lastTime) >= h.nackThrottle {
			filtered = append(filtered, seq)
			h.lastNackSent[seq] = now
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	// Group into NACK pairs
	pairs := rtcp.NackPairsFromSequenceNumbers(filtered)
	nack := &rtcp.TransportLayerNack{
		SenderSSRC: senderSSRC,
		MediaSSRC:  h.ssrc,
		Nacks:      pairs,
	}

	if h.onPacketCallback != nil {
		h.onPacketCallback(nack)
	}
	return nack
}

// BuildREMB creates a Receiver Estimated Maximum Bitrate packet.
func (h *RTCPHandler) BuildREMB(senderSSRC uint32, bitrateBps uint64, ssrcs []uint32) *rtcp.ReceiverEstimatedMaximumBitrate {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(ssrcs) == 0 {
		ssrcs = []uint32{h.ssrc}
	}

	remb := &rtcp.ReceiverEstimatedMaximumBitrate{
		SenderSSRC: senderSSRC,
		Bitrate:    float32(bitrateBps),
		SSRCs:      ssrcs,
	}

	if h.onPacketCallback != nil {
		h.onPacketCallback(remb)
	}
	return remb
}

// ProcessIncomingRTCP parses RTCP packets received from the peer (Receiver Reports, REMB, etc.)
func (h *RTCPHandler) ProcessIncomingRTCP(packets []rtcp.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, p := range packets {
		switch pkt := p.(type) {
		case *rtcp.ReceiverReport:
			for _, report := range pkt.Reports {
				if report.SSRC == h.ssrc {
					h.lossFraction = report.FractionLost
					// If DLRR is available, calculate RTT
					if report.LastSenderReport != 0 && report.Delay != 0 {
						// Calculate RTT from NTP timestamp
						h.rtt = time.Duration(report.Delay) * (time.Second / 65536)
					}
				}
			}
		case *rtcp.ReceiverEstimatedMaximumBitrate:
			// Processed by BandwidthEstimator
		}
	}
}

// GetLossFraction returns the most recent fractional packet loss reported.
func (h *RTCPHandler) GetLossFraction() uint8 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lossFraction
}

// GetRTT returns estimated round-trip time.
func (h *RTCPHandler) GetRTT() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rtt
}
