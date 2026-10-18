package tests

import (
	"testing"
	"time"

	"github.com/ER-brocode/nexus-sfu-gateway/pkg/rtp"
	"github.com/ER-brocode/nexus-sfu-gateway/pkg/sfu"
	rtppkt "github.com/pion/rtp"
	"github.com/pion/webrtc/v3"
)

func TestStreamForwardingAndFanOut(t *testing.T) {
	// Initialize PacketForwarder for VP8
	forwarder := sfu.NewPacketForwarder("video-track-1", webrtc.MimeTypeVP8)
	defer forwarder.Close()

	// Create 3 downlink subscribers
	var subs []*sfu.DownlinkSubscriber
	for i := 1; i <= 3; i++ {
		sub := sfu.NewDownlinkSubscriber(
			"subscriber-"+string(rune('0'+i)),
			nil, // testing internal queue delivery directly
			128,
		)
		subs = append(subs, sub)
		forwarder.AddSubscriber(sub)
	}

	if forwarder.SubscriberCount() != 3 {
		t.Fatalf("Expected 3 subscribers, got %d", forwarder.SubscriberCount())
	}

	// Forward 10 RTP packets
	for seq := uint16(1); seq <= 10; seq++ {
		pkt := &rtppkt.Packet{
			Header: rtppkt.Header{
				Version:        2,
				PayloadType:    96,
				SequenceNumber: seq,
				Timestamp:      uint32(seq * 3000),
				SSRC:           0x12345678,
			},
			Payload: []byte{0x10, 0x00, 0x00, byte(seq)},
		}

		if err := forwarder.Forward(pkt); err != nil {
			t.Fatalf("Failed to forward packet seq %d: %v", seq, err)
		}
	}

	// Verify all 3 downlinks received the 10 packets in queue
	for i, sub := range subs {
		count := 0
		timeout := time.After(500 * time.Millisecond)

	drainLoop:
		for {
			select {
			case pkt := <-sub.PacketQueue:
				count++
				if pkt.SequenceNumber != uint16(count) {
					t.Errorf("Sub %d out-of-order sequence: expected %d got %d", i, count, pkt.SequenceNumber)
				}
				if count == 10 {
					break drainLoop
				}
			case <-timeout:
				t.Fatalf("Sub %d timed out waiting for packets, received only %d", i, count)
			}
		}

		if count != 10 {
			t.Errorf("Sub %d expected 10 packets, got %d", i, count)
		}
		sub.Close()
	}

	packets, bytes, _ := forwarder.Stats()
	if packets != 10 {
		t.Errorf("Expected 10 total forwarded packets, got %d", packets)
	}
	if bytes != 40 {
		t.Errorf("Expected 40 payload bytes, got %d", bytes)
	}
}

func TestJitterBufferReorderingAndLoss(t *testing.T) {
	jb := rtp.NewJitterBuffer(64, 10*time.Millisecond, 100*time.Millisecond)
	defer jb.Close()

	// Push packet seq 1
	p1 := &rtppkt.Packet{
		Header:  rtppkt.Header{SequenceNumber: 1, Timestamp: 1000},
		Payload: []byte{1},
	}
	// Push packet seq 3 (seq 2 missing)
	p3 := &rtppkt.Packet{
		Header:  rtppkt.Header{SequenceNumber: 3, Timestamp: 3000},
		Payload: []byte{3},
	}

	if err := jb.Push(p1); err != nil {
		t.Fatalf("Failed to push seq 1: %v", err)
	}
	if err := jb.Push(p3); err != nil {
		t.Fatalf("Failed to push seq 3: %v", err)
	}

	// Check missing sequences for NACK
	missing := jb.MissingSequenceNumbers()
	if len(missing) != 1 || missing[0] != 2 {
		t.Errorf("Expected missing sequence [2], got %v", missing)
	}

	// Pop immediate (force=true) -> should yield seq 1
	popped, err := jb.Pop(true)
	if err != nil || popped == nil || popped.SequenceNumber != 1 {
		t.Fatalf("Expected popped seq 1, got %v err %v", popped, err)
	}

	// Now insert missing seq 2
	p2 := &rtppkt.Packet{
		Header:  rtppkt.Header{SequenceNumber: 2, Timestamp: 2000},
		Payload: []byte{2},
	}
	if err := jb.Push(p2); err != nil {
		t.Fatalf("Failed to push recovered seq 2: %v", err)
	}

	// Missing sequences should now be empty
	if len(jb.MissingSequenceNumbers()) != 0 {
		t.Errorf("Expected 0 missing after insertion, got %v", jb.MissingSequenceNumbers())
	}

	// Pop seq 2
	popped2, err := jb.Pop(true)
	if err != nil || popped2 == nil || popped2.SequenceNumber != 2 {
		t.Fatalf("Expected popped seq 2, got %v err %v", popped2, err)
	}

	// Pop seq 3
	popped3, err := jb.Pop(true)
	if err != nil || popped3 == nil || popped3.SequenceNumber != 3 {
		t.Fatalf("Expected popped seq 3, got %v err %v", popped3, err)
	}
}

func TestBandwidthEstimatorAIMD(t *testing.T) {
	bwe := sfu.NewBandwidthEstimator(sfu.BWEConfig{
		InitialBitrate: 1_000_000,
		MinBitrate:     200_000,
		MaxBitrate:     5_000_000,
		IncreaseFactor: 1.10,
		DecreaseFactor: 0.80,
	})

	// Normal condition (0 loss): rate should increase
	r1 := bwe.UpdateFromLoss(0, 30*time.Millisecond)
	if r1 <= 1_000_000 {
		t.Errorf("Expected bitrate increase under zero loss, got %d", r1)
	}
	if bwe.GetState() != sfu.StateNormal {
		t.Errorf("Expected StateNormal, got %v", bwe.GetState())
	}

	// High congestion (30% loss -> 77 / 256): rate should decrease significantly
	r2 := bwe.UpdateFromLoss(77, 120*time.Millisecond)
	if r2 >= r1 {
		t.Errorf("Expected bitrate decrease under high loss, got %d", r2)
	}
	if bwe.GetState() != sfu.StateCongested {
		t.Errorf("Expected StateCongested, got %v", bwe.GetState())
	}
}

func TestDepacketizerFormats(t *testing.T) {
	// VP8 Depacketizer test
	vp8Depack, err := rtp.GetDepacketizer("VP8")
	if err != nil {
		t.Fatalf("Failed to get VP8 depacketizer: %v", err)
	}
	// VP8 packet: First byte 0x10 (S bit set, no ext), frame header bit 0 = 0 (Keyframe)
	vp8Pkt := &rtppkt.Packet{
		Payload: []byte{0x10, 0x00, 0x00, 0x00},
	}
	payload, isKeyframe, err := vp8Depack.Depacketize(vp8Pkt)
	if err != nil {
		t.Fatalf("VP8 depacketize failed: %v", err)
	}
	if !isKeyframe {
		t.Errorf("Expected VP8 keyframe true, got false")
	}
	if len(payload) != 3 {
		t.Errorf("Expected 3 payload bytes, got %d", len(payload))
	}

	// Opus Depacketizer test
	opusDepack, err := rtp.GetDepacketizer("OPUS")
	if err != nil {
		t.Fatalf("Failed to get Opus depacketizer: %v", err)
	}
	opusPkt := &rtppkt.Packet{
		Payload: []byte{0x78, 0x01, 0x02, 0x03},
	}
	oPayload, isKey, err := opusDepack.Depacketize(opusPkt)
	if err != nil || !isKey || len(oPayload) != 4 {
		t.Errorf("Opus depacketize unexpected result: %v, %v, %v", oPayload, isKey, err)
	}
}
