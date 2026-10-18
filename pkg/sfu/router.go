package sfu

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ER-brocode/nexus-sfu-gateway/pkg/rtp"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"
)

var (
	ErrPeerNotFound  = errors.New("peer not found")
	ErrTrackNotFound = errors.New("track not found")
	ErrRouterClosed  = errors.New("sfu router closed")
)

// Peer represents an active participant in an SFU session.
type Peer struct {
	ID             string
	PeerConnection *webrtc.PeerConnection
	BWE            *BandwidthEstimator
	RTCPHandlers   map[uint32]*rtp.RTCPHandler
	PublishedTracks map[string]*webrtc.TrackRemote
	Downlinks      map[string]*DownlinkSubscriber
	mu             sync.RWMutex
	closed         bool
}

// SFURouter coordinates pub/sub routing between WebRTC peer connections.
type SFURouter struct {
	mu          sync.RWMutex
	peers       map[string]*Peer
	forwarders  map[string]*PacketForwarder
	jitterPool  map[string]*rtp.JitterBuffer
	mediaEngine *webrtc.MediaEngine
	webrtcAPI   *webrtc.API
	closed      bool
	onTrackAdd  func(peerID string, track *webrtc.TrackRemote)
}

// RouterConfig configures WebRTC settings (ICE servers, codecs).
type RouterConfig struct {
	ICEServers []webrtc.ICEServer
}

// NewSFURouter initializes media engines, codecs, and routing tables.
func NewSFURouter() (*SFURouter, error) {
	m := &webrtc.MediaEngine{}

	// Register VP8 Video Codec
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeVP8,
			ClockRate:    90000,
			Channels:     0,
			SDPFmtpLine:  "",
			RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: "goog-remb", Parameter: ""},
				{Type: "ccm", Parameter: "fir"},
				{Type: "nack", Parameter: ""},
				{Type: "nack", Parameter: "pli"},
			},
		},
		PayloadType: 96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("register vp8: %w", err)
	}

	// Register Opus Audio Codec
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeOpus,
			ClockRate:    48000,
			Channels:     2,
			SDPFmtpLine:  "minptime=10;useinbandfec=1",
			RTCPFeedback: nil,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register opus: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))

	return &SFURouter{
		peers:       make(map[string]*Peer),
		forwarders:  make(map[string]*PacketForwarder),
		jitterPool:  make(map[string]*rtp.JitterBuffer),
		mediaEngine: m,
		webrtcAPI:   api,
	}, nil
}

// AddPeer registers a new peer connection in the SFU router.
func (r *SFURouter) AddPeer(peerID string, pc *webrtc.PeerConnection) *Peer {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer := &Peer{
		ID:              peerID,
		PeerConnection:  pc,
		BWE:             NewBandwidthEstimator(DefaultBWEConfig()),
		RTCPHandlers:    make(map[uint32]*rtp.RTCPHandler),
		PublishedTracks: make(map[string]*webrtc.TrackRemote),
		Downlinks:       make(map[string]*DownlinkSubscriber),
	}

	r.peers[peerID] = peer
	return peer
}

// GetPeer retrieves an active peer.
func (r *SFURouter) GetPeer(peerID string) (*Peer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peer, exists := r.peers[peerID]
	if !exists {
		return nil, ErrPeerNotFound
	}
	return peer, nil
}

// RemovePeer gracefully unbinds a peer and disconnects all their downlinks and tracks.
func (r *SFURouter) RemovePeer(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer, exists := r.peers[peerID]
	if !exists {
		return
	}

	peer.mu.Lock()
	peer.closed = true

	// Clean up downlinks from forwarders
	for trackID, downlink := range peer.Downlinks {
		if f, ok := r.forwarders[trackID]; ok {
			f.RemoveSubscriber(downlink.ID)
		}
		downlink.Close()
	}

	// Clean up published forwarders
	for trackID := range peer.PublishedTracks {
		if f, ok := r.forwarders[trackID]; ok {
			f.Close()
			delete(r.forwarders, trackID)
		}
		if jb, ok := r.jitterPool[trackID]; ok {
			_ = jb.Close()
			delete(r.jitterPool, trackID)
		}
	}
	peer.mu.Unlock()

	delete(r.peers, peerID)
}

// BindIngressTrack attaches an incoming remote track, creates a forwarder, and starts the ingest pump.
func (r *SFURouter) BindIngressTrack(peerID string, remoteTrack *webrtc.TrackRemote) (*PacketForwarder, error) {
	r.mu.Lock()
	peer, exists := r.peers[peerID]
	if !exists {
		r.mu.Unlock()
		return nil, ErrPeerNotFound
	}

	trackID := remoteTrack.ID()
	forwarder := NewPacketForwarder(trackID, remoteTrack.Codec().MimeType)
	r.forwarders[trackID] = forwarder

	jb := rtp.NewJitterBuffer(256, 15*time.Millisecond, 150*time.Millisecond)
	r.jitterPool[trackID] = jb

	peer.mu.Lock()
	peer.PublishedTracks[trackID] = remoteTrack
	peer.mu.Unlock()

	// Setup RTCP Handler for remote track
	ssrc := uint32(remoteTrack.SSRC())
	rtcpH := rtp.NewRTCPHandler(rtp.RTCPHandlerConfig{
		SSRC:      ssrc,
		ClockRate: remoteTrack.Codec().ClockRate,
		OnPacket: func(pkt rtcp.Packet) {
			if peer.PeerConnection != nil {
				_ = peer.PeerConnection.WriteRTCP([]rtcp.Packet{pkt})
			}
		},
	})
	peer.mu.Lock()
	peer.RTCPHandlers[ssrc] = rtcpH
	peer.mu.Unlock()

	r.mu.Unlock()

	// Launch ingestion pump
	go r.ingestPump(peerID, remoteTrack, forwarder, jb, rtcpH)

	return forwarder, nil
}

func (r *SFURouter) ingestPump(
	peerID string,
	remoteTrack *webrtc.TrackRemote,
	forwarder *PacketForwarder,
	jb *rtp.JitterBuffer,
	rtcpH *rtp.RTCPHandler,
) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	// Background routine for jitter buffer popping and downstream emission
	stopChan := make(chan struct{})
	defer close(stopChan)

	go func() {
		for {
			select {
			case <-stopChan:
				return
			case <-ticker.C:
				for {
					pkt, err := jb.Pop(false)
					if err != nil || pkt == nil {
						break
					}
					_ = forwarder.Forward(pkt)
				}
			}
		}
	}()

	for {
		pkt, _, err := remoteTrack.ReadRTP()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			return
		}

		if err := jb.Push(pkt); err == nil {
			// Check for missing sequences to dispatch NACKs
			missing := jb.MissingSequenceNumbers()
			if len(missing) > 0 {
				nack := rtcpH.BuildNACK(uint32(remoteTrack.SSRC()), missing)
				if nack != nil {
					_ = rtcpH
				}
			}
		}
	}
}

// SubscribeTrack subscribes an egress peer to a source track ID, creating a local static track.
func (r *SFURouter) SubscribeTrack(subscriberID, sourceTrackID string) (*webrtc.TrackLocalStaticRTP, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	forwarder, foundForwarder := r.forwarders[sourceTrackID]
	if !foundForwarder {
		return nil, ErrTrackNotFound
	}

	subscriberPeer, foundPeer := r.peers[subscriberID]
	if !foundPeer {
		return nil, ErrPeerNotFound
	}

	capability := webrtc.RTPCodecCapability{
		MimeType: forwarder.codec,
	}
	if capability.MimeType == webrtc.MimeTypeVP8 {
		capability.ClockRate = 90000
	} else {
		capability.ClockRate = 48000
		capability.Channels = 2
	}

	localTrack, err := webrtc.NewTrackLocalStaticRTP(
		capability,
		sourceTrackID,
		fmt.Sprintf("nexus-stream-%s", sourceTrackID),
	)
	if err != nil {
		return nil, fmt.Errorf("create local track: %w", err)
	}

	downlink := NewDownlinkSubscriber(subscriberID, localTrack, 512)
	forwarder.AddSubscriber(downlink)

	subscriberPeer.mu.Lock()
	subscriberPeer.Downlinks[sourceTrackID] = downlink
	subscriberPeer.mu.Unlock()

	return localTrack, nil
}

// RequestKeyframe dispatches a PLI to publisher of trackID.
func (r *SFURouter) RequestKeyframe(trackID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, peer := range r.peers {
		peer.mu.RLock()
		remoteTrack, exists := peer.PublishedTracks[trackID]
		if exists {
			ssrc := uint32(remoteTrack.SSRC())
			if handler, ok := peer.RTCPHandlers[ssrc]; ok {
				_ = handler.BuildPLI(ssrc)
			}
			peer.mu.RUnlock()
			return nil
		}
		peer.mu.RUnlock()
	}
	return ErrTrackNotFound
}

// GetAPI returns Pion WebRTC API instance.
func (r *SFURouter) GetAPI() *webrtc.API {
	return r.webrtcAPI
}
