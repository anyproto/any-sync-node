package peermanager

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/peermanager"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/nodeconf"
)

func New() peermanager.PeerManagerProvider {
	return &provider{}
}

const CName = peermanager.CName

var log = logger.NewNamed(CName)

type provider struct {
	nodeconf nodeconf.Service
	pool     pool.Pool
}

func (p *provider) Init(a *app.App) (err error) {
	p.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	p.pool = a.MustComponent(pool.CName).(pool.Service)
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
