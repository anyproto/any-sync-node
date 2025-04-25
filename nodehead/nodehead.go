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
	SetHead(spaceId, oldHead, newHead string) (part int, err error)
	GetHead(spaceId string) (head string, err error)
	GetOldHead(spaceId string) (head string, err error)
	DeleteHeads(spaceId string) error
	ReloadHeadFromStore(ctx context.Context, spaceId string) error
	LDiff(partId int) ldiff.Diff
	Ranges(ctx context.Context, part int, ranges []ldiff.Range, resBuf []ldiff.RangeResult) (results []ldiff.RangeResult, err error)
	app.ComponentRunnable
}

type nodeStorage interface {
	ForceRemove(id string) error
	nodestorage.NodeStorage
}

type nodeHead struct {
	mu         sync.Mutex
	partitions map[int]ldiff.Diff
	oldHashes  map[string]string
	nodeconf   nodeconf.NodeConf
	spaceStore nodeStorage
}

func (n *nodeHead) Init(a *app.App) (err error) {
	n.partitions = map[int]ldiff.Diff{}
	n.oldHashes = map[string]string{}
	n.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.NodeConf)
	n.spaceStore = a.MustComponent(spacestorage.CName).(nodeStorage)
	n.spaceStore.OnWriteHash(func(_ context.Context, spaceId, oldHash, newHash string) {
		if _, e := n.SetHead(spaceId, oldHash, newHash); e != nil {
			log.Error("can't set head", zap.Error(e))
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
	err = n.spaceStore.IndexStorage().ReadHashes(ctx, func(update nodestorage.SpaceUpdate) (bool, error) {
		if _, e := n.SetHead(update.SpaceId, update.OldHash, update.NewHash); e != nil {
			log.Error("can't set head", zap.Error(e))
			return false, e
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	allSpaceIds, err := n.spaceStore.AllSpaceIds()
	if err != nil {
		return err
	}
	log.Info("space heads loaded", zap.Int("spaces", len(allSpaceIds)), zap.Duration("dur", time.Since(st)))
	return
}

func (n *nodeHead) loadHeadFromStore(ctx context.Context, spaceId string) (err error) {
	ss, err := n.spaceStore.SpaceStorage(ctx, spaceId)
	if err != nil {
		return
	}
	defer func() {
		log.Debug("close space storage", zap.String("spaceId", spaceId))
		anyStore := ss.AnyStore()
		err := n.spaceStore.ForceRemove(ss.Id())
		if err != nil {
			log.Error("can't remove space storage", zap.Error(err))
		}
		anyStore.Close()
	}()
	state, err := ss.StateStorage().GetState(ctx)
	if err != nil {
		return
	}
	if state.OldHash != "" && state.NewHash != "" {
		_, err = n.SetHead(spaceId, state.OldHash, state.NewHash)
	} else {
		_, err = n.SetHead(spaceId, state.LegacyHash, state.LegacyHash)
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

func (n *nodeHead) SetHead(spaceId, oldHead, newHead string) (part int, err error) {
	part = n.nodeconf.Partition(spaceId)
	n.mu.Lock()
	defer n.mu.Unlock()
	ld, ok := n.partitions[part]
	if !ok {
		ld = ldiff.New(16, 16)
		n.partitions[part] = ld
	}
	ld.Set(ldiff.Element{Id: spaceId, Head: newHead})
	n.oldHashes[spaceId] = oldHead
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
	hash, ok := n.oldHashes[spaceId]
	if !ok {
		err = ErrSpaceNotFound
	}
	return
}

func (n *nodeHead) ReloadHeadFromStore(ctx context.Context, spaceId string) error {
	return n.loadHeadFromStore(ctx, spaceId)
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
	return n.updater.Close()
}
