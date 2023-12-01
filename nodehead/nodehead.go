//go:generate mockgen -destination mock_nodehead/mock_nodehead.go github.com/anyproto/any-sync-node/nodehead NodeHead
package nodehead

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/ldiff"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodestorage"
)

const CName = "node.nodespace.nodehead"

var log = logger.NewNamed(CName)

var (
	ErrSpaceNotFound = errors.New("space not found")

	emptyLd = ldiff.New(16, 16)
)

func New() NodeHead {
	return new(nodeHead)
}

// NodeHead keeps current state of all spaces by partitions
type NodeHead interface {
	SetHead(spaceId, head string) (part int, err error)
	GetHead(spaceId string) (head string, err error)
	SetOldHead(spaceId, head string) (part int, err error)
	GetOldHead(spaceId string) (head string, err error)
	DeleteHeads(spaceId string) error
	ReloadHeadFromStore(spaceId string) error
	LDiff(partId int) ldiff.Diff
	Ranges(ctx context.Context, part int, ranges []ldiff.Range, resBuf []ldiff.RangeResult) (results []ldiff.RangeResult, err error)
	app.ComponentRunnable
}

type nodeHead struct {
	mu         sync.Mutex
	partitions map[int]ldiff.Diff
	oldHashes  map[string]string
	nodeconf   nodeconf.NodeConf
	spaceStore nodestorage.NodeStorage
}

func (n *nodeHead) Init(a *app.App) (err error) {
	n.partitions = map[int]ldiff.Diff{}
	n.oldHashes = map[string]string{}
	n.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.NodeConf)
	n.spaceStore = a.MustComponent(spacestorage.CName).(nodestorage.NodeStorage)
	n.spaceStore.OnWriteHash(func(_ context.Context, spaceId, hash string) {
		if _, e := n.SetHead(spaceId, hash); e != nil {
			log.Error("can't set head", zap.Error(e))
		}
	})
	n.spaceStore.OnWriteOldHash(func(_ context.Context, spaceId, hash string) {
		if _, e := n.SetOldHead(spaceId, hash); e != nil {
			log.Error("can't set old head", zap.Error(e))
		}
	})
	n.spaceStore.OnDeleteStorage(func(_ context.Context, spaceId string) {
		if e := n.DeleteHeads(spaceId); e != nil {
			log.Error("can't delete space from nodehead", zap.Error(e))
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
		if e := n.loadHeadFromStore(spaceId); e != nil {
			log.Warn("loadHeadFromStore error", zap.String("spaceId", spaceId), zap.Error(e))
		}
	}
	log.Info("space heads loaded", zap.Int("spaces", len(allSpaceIds)), zap.Duration("dur", time.Since(st)))
	return
}

func (n *nodeHead) loadHeadFromStore(spaceId string) (err error) {
	ss, err := n.spaceStore.SpaceStorage(spaceId)
	if err != nil {
		return
	}
	defer func() {
		_ = ss.Close(context.Background())
	}()
	hash, err := ss.ReadSpaceHash()
	if err != nil {
		return
	}
	if _, err = n.SetHead(spaceId, hash); err != nil {
		return
	}
	oldHash, err := ss.ReadOldSpaceHash()
	if err != nil {
		return
	}
	// that means that the hash was not set before
	if oldHash == "" {
		oldHash = hash
	}
	if _, err = n.SetOldHead(spaceId, oldHash); err != nil {
		return
	}
	return
}

func (n *nodeHead) DeleteHeads(spaceId string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.oldHashes, spaceId)
	part := n.nodeconf.Partition(spaceId)
	if ld, ok := n.partitions[part]; ok {
		return ld.RemoveId(spaceId)
	}
	return nil
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

func (n *nodeHead) SetOldHead(spaceId, head string) (part int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.oldHashes[spaceId] = head
	return
}

func (n *nodeHead) Ranges(ctx context.Context, part int, ranges []ldiff.Range, resBuf []ldiff.RangeResult) (results []ldiff.RangeResult, err error) {
	return n.LDiff(part).Ranges(ctx, ranges, resBuf)
}

func (n *nodeHead) LDiff(part int) (ld ldiff.Diff) {
	n.mu.Lock()
	ld, ok := n.partitions[part]
	if !ok {
		n.mu.Unlock()
		return emptyLd
	}
	n.mu.Unlock()
	return ld
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
		if errors.Is(err, ldiff.ErrElementNotFound) {
			return "", ErrSpaceNotFound
		}
		return
	}
	return el.Head, nil
}

func (n *nodeHead) GetOldHead(spaceId string) (hash string, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if hash, ok := n.oldHashes[spaceId]; ok {
		return hash, nil
	}
	return "", ErrSpaceNotFound
}

func (n *nodeHead) ReloadHeadFromStore(spaceId string) error {
	return n.loadHeadFromStore(spaceId)
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
