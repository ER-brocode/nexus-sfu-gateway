package sfu

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
)

var (
	ErrForwarderClosed = errors.New("packet forwarder closed")
	ErrDownlinkFull    = errors.New("downlink packet queue full")
)

// DownlinkSubscriber represents an egress track consumer receiving forwarded packets.
type DownlinkSubscriber struct {
	ID             string
	Track          *webrtc.TrackLocalStaticRTP
	SequenceOffset uint16
	LastSequence   uint16
	TimestampOffset uint32
	LastTimestamp  uint32
	PacketQueue    chan *rtp.Packet
	droppedPackets uint64
	sentPackets    uint64
	closed         atomic.Bool
	stopChan       chan struct{}
}

// NewDownlinkSubscriber creates an active downlink consumer with isolated queue.
func NewDownlinkSubscriber(id string, track *webrtc.TrackLocalStaticRTP, queueSize int) *DownlinkSubscriber {
	if queueSize <= 0 {
		queueSize = 512
	}
	sub := &DownlinkSubscriber{
		ID:          id,
		Track:       track,
		PacketQueue: make(chan *rtp.Packet, queueSize),
		stopChan:    make(chan struct{}),
	}
	if track != nil {
		go sub.writeLoop()
	}
	return sub
}

func (s *DownlinkSubscriber) writeLoop() {
	for {
		select {
		case <-s.stopChan:
			return
		case pkt, ok := <-s.PacketQueue:
			if !ok {
				return
			}
			if s.Track != nil {
				if err := s.Track.WriteRTP(pkt); err == nil {
					atomic.AddUint64(&s.sentPackets, 1)
				}
			}
		}
	}
}

// Close gracefully closes the subscriber queue.
func (s *DownlinkSubscriber) Close() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.stopChan)
	}
}

// PacketForwarder coordinates forwarding RTP packets from an ingress track to multiple downlinks.
type PacketForwarder struct {
	mu           sync.RWMutex
	trackID      string
	codec        string
	subscribers  map[string]*DownlinkSubscriber
	closed       bool
	packetCount  uint64
	byteCount    uint64
	lastActivity time.Time
}

// NewPacketForwarder creates a high-performance fan-out RTP forwarder.
func NewPacketForwarder(trackID, codec string) *PacketForwarder {
	return &PacketForwarder{
		trackID:      trackID,
		codec:        codec,
		subscribers:  make(map[string]*DownlinkSubscriber),
		lastActivity: time.Now(),
	}
}

// AddSubscriber registers a consumer to receive forwarded RTP packets.
func (pf *PacketForwarder) AddSubscriber(sub *DownlinkSubscriber) {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	if pf.closed {
		sub.Close()
		return
	}

	pf.subscribers[sub.ID] = sub
}

// RemoveSubscriber unregisters a consumer.
func (pf *PacketForwarder) RemoveSubscriber(subscriberID string) {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	if sub, exists := pf.subscribers[subscriberID]; exists {
		sub.Close()
		delete(pf.subscribers, subscriberID)
	}
}

// Forward distributes an RTP packet across all attached subscribers with zero allocation copy.
func (pf *PacketForwarder) Forward(pkt *rtp.Packet) error {
	if pkt == nil {
		return errors.New("nil packet")
	}

	pf.mu.RLock()
	defer pf.mu.RUnlock()

	if pf.closed {
		return ErrForwarderClosed
	}

	atomic.AddUint64(&pf.packetCount, 1)
	atomic.AddUint64(&pf.byteCount, uint64(len(pkt.Payload)))

	for _, sub := range pf.subscribers {
		// Clone packet header & shallow copy payload to prevent data races across tracks
		cloned := &rtp.Packet{
			Header:  pkt.Header,
			Payload: pkt.Payload,
		}

		select {
		case sub.PacketQueue <- cloned:
		default:
			// Non-blocking drop on congested downlink
			atomic.AddUint64(&sub.droppedPackets, 1)
		}
	}

	return nil
}

// SubscriberCount returns active egress stream listeners.
func (pf *PacketForwarder) SubscriberCount() int {
	pf.mu.RLock()
	defer pf.mu.RUnlock()
	return len(pf.subscribers)
}

// Stats provides real-time throughput metrics.
func (pf *PacketForwarder) Stats() (packets uint64, bytes uint64, subs int) {
	pf.mu.RLock()
	defer pf.mu.RUnlock()
	return atomic.LoadUint64(&pf.packetCount), atomic.LoadUint64(&pf.byteCount), len(pf.subscribers)
}

// Close shuts down all downstream queues.
func (pf *PacketForwarder) Close() {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	if pf.closed {
		return
	}
	pf.closed = true

	for id, sub := range pf.subscribers {
		sub.Close()
		delete(pf.subscribers, id)
	}
}
