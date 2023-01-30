package peermanager

import (
	"context"
	"github.com/anytypeio/any-sync-node/nodespace"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/peermanager"
	"github.com/anytypeio/any-sync/net/pool"
	"github.com/anytypeio/any-sync/net/streampool"
	"github.com/anytypeio/any-sync/nodeconf"
)

func New() peermanager.PeerManagerProvider {
	return &provider{}
}

const CName = peermanager.CName

var log = logger.NewNamed(CName)

type provider struct {
	nodeconf   nodeconf.Service
	pool       pool.Pool
	streamPool streampool.StreamPool
	commonPool pool.Pool
}

func (p *provider) Init(a *app.App) (err error) {
	p.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	commonPool := a.MustComponent(pool.CName).(pool.Service)
	p.commonPool = commonPool
	p.pool = commonPool.NewPool("space_stream")
	p.streamPool = a.MustComponent(nodespace.CName).(nodespace.Service).StreamPool()
	return nil
}

func (p *provider) Name() (name string) {
	return CName
}

func (p *provider) NewPeerManager(ctx context.Context, spaceId string) (sm peermanager.PeerManager, err error) {
	pm := &nodePeerManager{p: p, spaceId: spaceId}
	pm.init()
	return pm, nil
}
