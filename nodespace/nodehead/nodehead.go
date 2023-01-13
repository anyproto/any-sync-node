package nodehead

import (
	"context"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/ldiff"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/nodeconf"
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
	app.Component
}

type nodeHead struct {
	mu         sync.Mutex
	partitions map[int]ldiff.Diff
	nodeconf   nodeconf.Configuration
}

func (n *nodeHead) Init(a *app.App) (err error) {
	n.partitions = map[int]ldiff.Diff{}
	n.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service).GetLast()
	return
}

func (n *nodeHead) Name() (name string) {
	return CName
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
