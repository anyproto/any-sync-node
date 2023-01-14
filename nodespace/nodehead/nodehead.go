package nodehead

import (
	"context"
	"github.com/anytypeio/any-sync-node/storage"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/ldiff"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/spacestorage"
	"github.com/anytypeio/any-sync/nodeconf"
	"go.uber.org/zap"
	"sync"
)

const CName = "node.nodespace.nodehead"

var log = logger.NewNamed(CName)

func New() NodeHead {
	return new(nodeHead)
}

type NodeHead interface {
	SetHead(ctx context.Context, spaceId, head string) (part int, err error)
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
	n.spaceStore.OnWriteHash(func(ctx context.Context, spaceId, hash string) {
		if _, e := n.SetHead(ctx, spaceId, hash); e != nil {
			log.Error("can't set head", zap.Error(e))
		}
	})
	return
}

func (n *nodeHead) Name() (name string) {
	return CName
}

func (n *nodeHead) Run(ctx context.Context) (err error) {
	return nil
}

func (n *nodeHead) SetHead(ctx context.Context, spaceId, head string) (part int, err error) {
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

func (n *nodeHead) Close(ctx context.Context) (err error) {
	return nil
}
