package nodesync

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type SyncStat struct {
	LastStartTime atomic.Uint64
	LastDuration  atomic.Uint64

	InProgress atomic.Bool

	ColdSyncHandled atomic.Uint32
	ColdSyncErrors  atomic.Uint32

	HotSyncHandled atomic.Uint32
	HotSyncErrors  atomic.Uint32

	PartsHandled atomic.Uint32
	PartsErrors  atomic.Uint32
	PartsTotal   atomic.Uint32

	SyncsDone atomic.Uint32
}

func registerMetric(s *SyncStat, registry *prometheus.Registry) {
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "inprogress",
		Name:      "is",
	}, func() float64 {
		if s.InProgress.Load() {
			return 1
		}
		return 0
	}))

	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "coldsync",
		Name:      "handled_count",
	}, func() float64 {
		return float64(s.ColdSyncHandled.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "coldsync",
		Name:      "errors_count",
	}, func() float64 {
		return float64(s.ColdSyncErrors.Load())
	}))

	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "hotsync",
		Name:      "handled_count",
	}, func() float64 {
		return float64(s.HotSyncHandled.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "hotsync",
		Name:      "errors_count",
	}, func() float64 {
		return float64(s.HotSyncErrors.Load())
	}))

	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "parts",
		Name:      "handled_count",
	}, func() float64 {
		return float64(s.PartsHandled.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "parts",
		Name:      "errors_count",
	}, func() float64 {
		return float64(s.PartsErrors.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "parts",
		Name:      "total_count",
	}, func() float64 {
		return float64(s.PartsTotal.Load())
	}))

	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "syncs",
		Name:      "done_count",
	}, func() float64 {
		return float64(s.SyncsDone.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "lastsync",
		Name:      "start_unix",
	}, func() float64 {
		return float64(s.LastStartTime.Load())
	}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodesync",
		Subsystem: "lastsync",
		Name:      "duration_ms",
	}, func() float64 {
		ms := time.Duration(s.LastDuration.Load()) / time.Millisecond
		return float64(ms)
	}))
}
