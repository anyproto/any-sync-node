package archive

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

type archiveStat struct {
	archived      atomic.Uint32
	archiveError  atomic.Uint32
	restored      atomic.Uint32
	forceArchived atomic.Uint32
	adopted       atomic.Uint32
}

func registerMetric(s *archiveStat, registry *prometheus.Registry) {
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node",
		Subsystem: "archive",
		Name:      "archived",
	}, func() float64 {
		return float64(s.archived.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node",
		Subsystem: "archive",
		Name:      "restored",
	}, func() float64 {
		return float64(s.restored.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node",
		Subsystem: "archive",
		Name:      "error",
	}, func() float64 {
		return float64(s.archiveError.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node",
		Subsystem: "archive",
		Name:      "force_archived",
	}, func() float64 {
		return float64(s.forceArchived.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "node",
		Subsystem: "archive",
		Name:      "adopted",
	}, func() float64 {
		return float64(s.adopted.Load())
	}))
}
