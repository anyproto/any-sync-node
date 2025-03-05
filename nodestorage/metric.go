package nodestorage

import (
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/prometheus/client_golang/prometheus"
)

type StorageStat struct {
	cache ocache.OCache
}

func (s *StorageStat) length() int {
	return s.cache.Len()
}

func registerMetric(s *StorageStat, registry *prometheus.Registry) {
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodestorage",
		Subsystem: "anystore",
		Name:      "count",
		Help:      "storages count",
	}, func() float64 {
		return float64(s.length())
	}))
}
