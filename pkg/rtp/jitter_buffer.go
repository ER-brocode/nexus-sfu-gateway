package rtp

import (
	"errors"
	"sync"
	"time"

	"github.com/pion/rtp"
)

var (
	ErrBufferFull     = errors.New("jitter buffer is full")
	ErrPacketTooLate  = errors.New("packet arrived too late")
	ErrBufferEmpty    = errors.New("jitter buffer is empty")
	ErrBufferFlushed  = errors.New("jitter buffer has been flushed")
)

// JitterBufferStats tracks packet metrics inside the buffer.
type JitterBufferStats struct {
	TotalPacketsReceived uint64
	TotalPacketsEmitted  uint64
	TotalPacketsDropped  uint64
	LossRate             float64
	CurrentDelayMs       int64
	BufferOccupancy      int
}

// JitterBuffer reorders out-of-order RTP packets and smooths out arrival jitter
// before depacketizing or forwarding downstream.
type JitterBuffer struct {
	mu            sync.Mutex
	capacity      int
	targetDelay   time.Duration
	maxDelay      time.Duration
	storage       map[uint16]*bufferedPacket
	headSeq       uint16
	tailSeq       uint16
	initialized   bool
	closed        bool
	stats         JitterBufferStats
	lastPoppedTs  uint32
}

type bufferedPacket struct {
	pkt       *rtp.Packet
	arrival   time.Time
	emitted   bool
}

// NewJitterBuffer creates an adaptive jitter buffer.
func NewJitterBuffer(capacity int, targetDelay, maxDelay time.Duration) *JitterBuffer {
	if capacity <= 0 {
		capacity = 256
	}
	if targetDelay <= 0 {
		targetDelay = 20 * time.Millisecond
	}
	if maxDelay <= targetDelay {
		maxDelay = 200 * time.Millisecond
	}

	return &JitterBuffer{
		capacity:    capacity,
		targetDelay: targetDelay,
		maxDelay:    maxDelay,
		storage:     make(map[uint16]*bufferedPacket, capacity),
	}
}

// Push adds an RTP packet to the buffer, placing it in sequence order.
func (jb *JitterBuffer) Push(pkt *rtp.Packet) error {
	if pkt == nil {
		return errors.New("nil packet")
	}

	jb.mu.Lock()
	defer jb.mu.Unlock()

	if jb.closed {
		return ErrBufferFlushed
	}

	jb.stats.TotalPacketsReceived++

	seq := pkt.SequenceNumber

	if !jb.initialized {
		jb.headSeq = seq
		jb.tailSeq = seq
		jb.initialized = true
		jb.storage[seq] = &bufferedPacket{
			pkt:     pkt,
			arrival: time.Now(),
		}
		jb.stats.BufferOccupancy = len(jb.storage)
		return nil
	}

	// Calculate sequence difference relative to headSeq
	diff := int16(seq - jb.headSeq)
	if diff < 0 {
		// Packet has already passed playout threshold
		jb.stats.TotalPacketsDropped++
		return ErrPacketTooLate
	}

	if len(jb.storage) >= jb.capacity {
		jb.stats.TotalPacketsDropped++
		return ErrBufferFull
	}

	// Update tail sequence if packet extends forward horizon
	if int16(seq - jb.tailSeq) > 0 {
		jb.tailSeq = seq
	}

	jb.storage[seq] = &bufferedPacket{
		pkt:     pkt,
		arrival: time.Now(),
	}
	jb.stats.BufferOccupancy = len(jb.storage)
	return nil
}

// Pop extracts the next continuous in-order packet if ready, or nil if waiting.
func (jb *JitterBuffer) Pop(force bool) (*rtp.Packet, error) {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	if !jb.initialized || len(jb.storage) == 0 {
		return nil, ErrBufferEmpty
	}

	now := time.Now()
	bp, found := jb.storage[jb.headSeq]

	if found {
		if !force && now.Sub(bp.arrival) < jb.targetDelay {
			// Not yet ready for emission according to playout target
			return nil, nil
		}
		delete(jb.storage, jb.headSeq)
		jb.headSeq++
		jb.stats.TotalPacketsEmitted++
		jb.stats.BufferOccupancy = len(jb.storage)
		jb.stats.CurrentDelayMs = now.Sub(bp.arrival).Milliseconds()
		jb.lastPoppedTs = bp.pkt.Timestamp
		return bp.pkt, nil
	}

	// The head packet is missing (gap in sequence)
	if force {
		// Advance head to skip the hole
		jb.stats.TotalPacketsDropped++
		jb.headSeq++
		// Recursively attempt next packet
		if nextBp, nextFound := jb.storage[jb.headSeq]; nextFound {
			delete(jb.storage, jb.headSeq)
			jb.headSeq++
			jb.stats.TotalPacketsEmitted++
			jb.stats.BufferOccupancy = len(jb.storage)
			return nextBp.pkt, nil
		}
		return nil, ErrBufferEmpty
	}

	// Check if the oldest buffered packet has exceeded max delay
	for _, p := range jb.storage {
		if now.Sub(p.arrival) > jb.maxDelay {
			// Skip current head sequence hole
			jb.stats.TotalPacketsDropped++
			jb.headSeq++
			break
		}
	}

	return nil, nil
}

// MissingSequenceNumbers returns gaps between head and tail to facilitate NACK generation.
func (jb *JitterBuffer) MissingSequenceNumbers() []uint16 {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	if !jb.initialized || len(jb.storage) == 0 {
		return nil
	}

	var missing []uint16
	for seq := jb.headSeq; seq != jb.tailSeq; seq++ {
		if _, ok := jb.storage[seq]; !ok {
			missing = append(missing, seq)
		}
	}
	return missing
}

// Flush clears internal packet state.
func (jb *JitterBuffer) Flush() {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	jb.storage = make(map[uint16]*bufferedPacket, jb.capacity)
	jb.initialized = false
}

// Close marks the jitter buffer closed.
func (jb *JitterBuffer) Close() error {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	jb.closed = true
	jb.storage = nil
	return nil
}

// GetStats returns current buffer operational metrics.
func (jb *JitterBuffer) GetStats() JitterBufferStats {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	stats := jb.stats
	if stats.TotalPacketsReceived > 0 {
		stats.LossRate = float64(stats.TotalPacketsDropped) / float64(stats.TotalPacketsReceived)
	}
	stats.BufferOccupancy = len(jb.storage)
	return stats
}
