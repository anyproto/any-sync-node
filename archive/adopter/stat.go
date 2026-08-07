package adopter

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

type adopterStat struct {
	// adopted is the number of spaces copied into our prefix and registered
	adopted atomic.Uint32
	// alreadyHaveSame: requests ACKed because our copy matches the offered heads
	alreadyHaveSame atomic.Uint32
	// alreadyHaveDiverged: requests answered with a diverged copy (sender must converge)
	alreadyHaveDiverged atomic.Uint32
	// rejected: requests refused or failed (deleted/pending spaces, non-owners,
	// non-node peers, transfer errors)
	rejected atomic.Uint32
}

func registerMetric(s *adopterStat, registry *prometheus.Registry) {
	gauge := func(name, help string, value func() float64) {
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "node",
			Subsystem: "adopter",
			Name:      name,
			Help:      help,
		}, value))
	}
	gauge("adopted", "spaces adopted (copied into our prefix and registered) since start", func() float64 {
		return float64(s.adopted.Load())
	})
	gauge("already_have_same", "adopt requests ACKed with an equal local copy since start", func() float64 {
		return float64(s.alreadyHaveSame.Load())
	})
	gauge("already_have_diverged", "adopt requests answered with a diverged local copy since start", func() float64 {
		return float64(s.alreadyHaveDiverged.Load())
	})
	gauge("rejected", "adopt requests refused or failed since start", func() float64 {
		return float64(s.rejected.Load())
	})
}
