package repairer

import (
	"errors"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

var errNoValidCopy = errors.New("no responsible node could provide a valid copy")

type repairerStat struct {
	// errored is the number of responsible spaces in Error status after the
	// last repair cycle (0 = nothing left to repair)
	errored atomic.Uint32
	// repairedInPlace: transient errors cleared without touching data
	repairedInPlace atomic.Uint32
	// quarantined: corrupted dbs parked under <root>/.quarantine
	quarantined atomic.Uint32
	// repaired: valid copies pulled from responsible neighbors
	repaired atomic.Uint32
}

func registerMetric(s *repairerStat, registry *prometheus.Registry) {
	gauge := func(name, help string, value func() float64) {
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "node",
			Subsystem: "repairer",
			Name:      name,
			Help:      help,
		}, value))
	}
	gauge("errored", "responsible spaces still in Error status after the last repair cycle", func() float64 {
		return float64(s.errored.Load())
	})
	gauge("repaired_in_place", "transient errors cleared without touching data, since start", func() float64 {
		return float64(s.repairedInPlace.Load())
	})
	gauge("quarantined", "corrupted dbs parked under .quarantine, since start", func() float64 {
		return float64(s.quarantined.Load())
	})
	gauge("repaired", "valid copies pulled from responsible neighbors, since start", func() float64 {
		return float64(s.repaired.Load())
	})
}
