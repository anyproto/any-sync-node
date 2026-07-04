package resharder

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

type resharderStat struct {
	// draining is the number of local spaces this node is no longer
	// responsible for and still holds (0 when resharding is complete)
	draining atomic.Uint32
	// moved is the number of spaces handed off and deleted locally
	moved atomic.Uint32
	// parked is the number of handoffs postponed to the next cycle
	parked atomic.Uint32
}

func registerMetric(s *resharderStat, registry *prometheus.Registry) {
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node",
		Subsystem: "resharder",
		Name:      "draining",
	}, func() float64 {
		return float64(s.draining.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node",
		Subsystem: "resharder",
		Name:      "moved",
	}, func() float64 {
		return float64(s.moved.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node",
		Subsystem: "resharder",
		Name:      "parked",
	}, func() float64 {
		return float64(s.parked.Load())
	}))
}
