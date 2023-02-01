package nodehead

import (
	"context"
	"errors"
	"github.com/anytypeio/any-sync-node/storage"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/ldiff"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/spacestorage"
	"github.com/anytypeio/any-sync/metric"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"sync"
	"time"
)

const CName = "node.nodespace.nodehead"

var log = logger.NewNamed(CName)

var (
	ErrSpaceNotFound = errors.New("space not found")
)

func New() NodeHead {
	return new(nodeHead)
}

// NodeHead keeps current state of all spaces by partitions
type NodeHead interface {
	SetHead(spaceId, head string) (part int, err error)
	GetHead(spaceId string) (head string, err error)
	Ranges(ctx context.Context, part int, ranges []ldiff.Range, resBuf []ldiff.RangeResult) (results []ldiff.RangeResult, err error)
	app.ComponentRunnable
}

type nodeHead struct {
	mu         sync.Mutex
	partitions map[int]ldiff.Diff
	nodeconf   nodeconf.Configuration
	spaceStore storage.NodeStorage
}

func (n *nodeHead) Init(a *app.App) (err error) {
	n.partitions = map[int]ldiff.Diff{}
	n.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service).GetLast()
	n.spaceStore = a.MustComponent(spacestorage.CName).(storage.NodeStorage)
	n.spaceStore.OnWriteHash(func(_ context.Context, spaceId, hash string) {
		if _, e := n.SetHead(spaceId, hash); e != nil {
			log.Error("can't set head", zap.Error(e))
		}
	})
	if m := a.Component(metric.CName); m != nil {
		n.registerMetrics(m.(metric.Metric))
	}
	return
}

func (n *nodeHead) Name() (name string) {
	return CName
}

func (n *nodeHead) Run(ctx context.Context) (err error) {
	st := time.Now()
	allSpaceIds, err := n.spaceStore.AllSpaceIds()
	if err != nil {
		return
	}
	log.Info("start loading heads...", zap.Int("spaces", len(allSpaceIds)))
	for _, spaceId := range allSpaceIds {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if e := n.loadHeadFromStore(ctx, spaceId); e != nil {
			log.Warn("loadHeadFromStore error", zap.String("spaceId", spaceId), zap.Error(e))
		}
	}
	log.Info("space heads loaded", zap.Int("spaces", len(allSpaceIds)), zap.Duration("dur", time.Since(st)))
	return
}

func (n *nodeHead) loadHeadFromStore(ctx context.Context, spaceId string) (err error) {
	ss, err := n.spaceStore.SpaceStorage(spaceId)
	if err != nil {
		return
	}
	defer func() {
		_ = ss.Close()
	}()
	hash, err := ss.ReadSpaceHash()
	if err != nil {
		return
	}
	if _, err = n.SetHead(spaceId, hash); err != nil {
		return
	}
	return
}

func (n *nodeHead) SetHead(spaceId, head string) (part int, err error) {
	part = n.nodeconf.Partition(spaceId)
	n.mu.Lock()
	defer n.mu.Unlock()
	ld, ok := n.partitions[part]
	if !ok {
		ld = ldiff.New(16, 16)
		n.partitions[part] = ld
	}
	ld.Set(ldiff.Element{Id: spaceId, Head: head})
	return
}

func (n *nodeHead) Ranges(ctx context.Context, part int, ranges []ldiff.Range, resBuf []ldiff.RangeResult) (results []ldiff.RangeResult, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	ld, ok := n.partitions[part]
	if !ok {
		ld = ldiff.New(16, 16)
		n.partitions[part] = ld
	}
	return ld.Ranges(ctx, ranges, resBuf)
}

func (n *nodeHead) GetHead(spaceId string) (hash string, err error) {
	part := n.nodeconf.Partition(spaceId)
	n.mu.Lock()
	ld, ok := n.partitions[part]
	if !ok {
		n.mu.Unlock()
		return "", ErrSpaceNotFound
	}
	n.mu.Unlock()
	el, err := ld.Element(spaceId)
	if err != nil {
		return
	}
	return el.Head, nil
}

func (n *nodeHead) registerMetrics(m metric.Metric) {
	m.Registry().MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodehead",
		Subsystem: "partition",
		Name:      "count",
		Help:      "partitions count",
	}, func() float64 {
		n.mu.Lock()
		defer n.mu.Unlock()
		return float64(len(n.partitions))
	}))
	m.Registry().MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nodehead",
		Subsystem: "space",
		Name:      "count",
		Help:      "space count",
	}, func() float64 {
		n.mu.Lock()
		defer n.mu.Unlock()
		var l int
		for _, ld := range n.partitions {
			if ld != nil {
				l += ld.Len()
			}
		}
		return float64(l)
	}))
}

func (n *nodeHead) Close(ctx context.Context) (err error) {
	return
}
