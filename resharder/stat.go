package resharder

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// resharder states exported via the node_resharder_state gauge.
const (
	// stateDisabled: the archive store is not shared, the machinery is off
	stateDisabled uint32 = 0
	// stateIdle: nothing to drain (resharding complete or never needed)
	stateIdle uint32 = 1
	// stateDraining: the node holds spaces it is no longer responsible for
	stateDraining uint32 = 2
)

type resharderStat struct {
	// state is one of stateDisabled/stateIdle/stateDraining
	state atomic.Uint32
	// epoch is the network configuration epoch of the last drain cycle
	epoch atomic.Uint64
	// lastCycleUnix is when the last drain cycle finished
	lastCycleUnix atomic.Int64
	// draining is the number of local spaces this node is no longer
	// responsible for and still holds (0 when resharding is complete)
	draining atomic.Uint32
	// moved is the number of spaces handed off and deleted locally
	moved atomic.Uint32
	// parked is the number of handoffs postponed to the next cycle
	parked atomic.Uint32
	// errors is the number of failed drain attempts
	errors atomic.Uint32
}

func registerMetric(s *resharderStat, registry *prometheus.Registry) {
	gauge := func(name, help string, value func() float64) {
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "node",
			Subsystem: "resharder",
			Name:      name,
			Help:      help,
		}, value))
	}
	gauge("state", "resharding state: 0 disabled (archive store not shared), 1 idle, 2 draining", func() float64 {
		return float64(s.state.Load())
	})
	gauge("epoch", "network configuration epoch of the last drain cycle", func() float64 {
		return float64(s.epoch.Load())
	})
	gauge("last_cycle_unix", "unix time the last drain cycle finished, 0 if none ran yet", func() float64 {
		return float64(s.lastCycleUnix.Load())
	})
	gauge("draining", "spaces this node is no longer responsible for and still holds", func() float64 {
		return float64(s.draining.Load())
	})
	gauge("moved", "spaces handed off and deleted locally since start", func() float64 {
		return float64(s.moved.Load())
	})
	gauge("parked", "handoffs postponed to the next cycle since start", func() float64 {
		return float64(s.parked.Load())
	})
	gauge("errors", "failed drain attempts since start", func() float64 {
		return float64(s.errors.Load())
	})
}
