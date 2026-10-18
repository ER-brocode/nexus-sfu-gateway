package sfu

import (
	"sync"
	"time"
)

// NetworkCongestionState reflects the estimated network condition.
type NetworkCongestionState int

const (
	StateNormal NetworkCongestionState = iota
	StateWarning
	StateCongested
)

func (s NetworkCongestionState) String() string {
	switch s {
	case StateNormal:
		return "NORMAL"
	case StateWarning:
		return "WARNING"
	case StateCongested:
		return "CONGESTED"
	default:
		return "UNKNOWN"
	}
}

// BWEConfig configures Google Congestion Control / AIMD parameters.
type BWEConfig struct {
	InitialBitrate uint64 // bps
	MinBitrate     uint64 // bps
	MaxBitrate     uint64 // bps
	IncreaseFactor float64
	DecreaseFactor float64
}

// DefaultBWEConfig provides balanced default settings.
func DefaultBWEConfig() BWEConfig {
	return BWEConfig{
		InitialBitrate: 2_000_000, // 2 Mbps
		MinBitrate:     300_000,   // 300 Kbps
		MaxBitrate:     10_000_000, // 10 Mbps
		IncreaseFactor: 1.08,      // 8% additive increase
		DecreaseFactor: 0.85,      // 15% multiplicative decrease on loss
	}
}

// BandwidthEstimator performs AIMD estimation using RTCP loss reports and REMB updates.
type BandwidthEstimator struct {
	mu             sync.RWMutex
	cfg            BWEConfig
	targetBitrate  uint64
	state          NetworkCongestionState
	lastLossRatio  float64
	lastUpdateTime time.Time
	rtt            time.Duration
}

// NewBandwidthEstimator constructs a bandwidth estimator with configuration.
func NewBandwidthEstimator(cfg BWEConfig) *BandwidthEstimator {
	if cfg.InitialBitrate == 0 {
		cfg = DefaultBWEConfig()
	}
	return &BandwidthEstimator{
		cfg:            cfg,
		targetBitrate:  cfg.InitialBitrate,
		state:          StateNormal,
		lastUpdateTime: time.Now(),
		rtt:            50 * time.Millisecond,
	}
}

// UpdateFromLoss adjusts target bitrate based on RTCP fraction lost (0..255).
// RFC 3550 fraction lost: 256ths of packets lost since last report.
func (b *BandwidthEstimator) UpdateFromLoss(fractionLost uint8, rtt time.Duration) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	lossRatio := float64(fractionLost) / 256.0
	b.lastLossRatio = lossRatio
	b.rtt = rtt
	b.lastUpdateTime = time.Now()

	// GCC Loss-based adjustment logic
	if lossRatio > 0.10 {
		// High loss (>10%): Multiplicative decrease
		b.state = StateCongested
		newRate := uint64(float64(b.targetBitrate) * (1.0 - 0.5*lossRatio))
		if newRate < b.cfg.MinBitrate {
			newRate = b.cfg.MinBitrate
		}
		b.targetBitrate = newRate
	} else if lossRatio > 0.02 {
		// Moderate loss (2% - 10%): Hold rate or slight decrease
		b.state = StateWarning
		newRate := uint64(float64(b.targetBitrate) * b.cfg.DecreaseFactor)
		if newRate < b.cfg.MinBitrate {
			newRate = b.cfg.MinBitrate
		}
		b.targetBitrate = newRate
	} else {
		// Low or zero loss (<2%): Additive / Proportional increase
		b.state = StateNormal
		newRate := uint64(float64(b.targetBitrate) * b.cfg.IncreaseFactor)
		if newRate > b.cfg.MaxBitrate {
			newRate = b.cfg.MaxBitrate
		}
		b.targetBitrate = newRate
	}

	return b.targetBitrate
}

// UpdateFromREMB clamps the bandwidth estimate to peer REMB if lower.
func (b *BandwidthEstimator) UpdateFromREMB(rembBitrate uint64) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	if rembBitrate > 0 {
		if rembBitrate < b.cfg.MinBitrate {
			rembBitrate = b.cfg.MinBitrate
		} else if rembBitrate > b.cfg.MaxBitrate {
			rembBitrate = b.cfg.MaxBitrate
		}

		if rembBitrate < b.targetBitrate {
			b.targetBitrate = rembBitrate
		}
	}
	return b.targetBitrate
}

// GetTargetBitrate returns current estimated target bitrate.
func (b *BandwidthEstimator) GetTargetBitrate() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.targetBitrate
}

// GetState returns current congestion state.
func (b *BandwidthEstimator) GetState() NetworkCongestionState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

// GetLastLoss returns the latest loss ratio.
func (b *BandwidthEstimator) GetLastLoss() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastLossRatio
}
