package nodesync

import (
	"context"
	commonaccount "github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/anytypeio/go-chash"
)

const CName = "node.nodesync"

var log = logger.NewNamed(CName)

func New() NodeSync {
	return new(nodeSync)
}

type NodeSync interface {
	Sync(ctx context.Context) (err error)
	app.ComponentRunnable
}

type nodeSync struct {
	nodeconf nodeconf.Service
	conf     Config
	peerId   string
}

func (n *nodeSync) Init(a *app.App) (err error) {
	n.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	n.peerId = a.MustComponent(commonaccount.CName).(commonaccount.Service).Account().PeerId
	n.conf = a.MustComponent("config").(configGetter).GetNodeSync()
	return
}

func (n *nodeSync) Name() (name string) {
	return CName
}

func (n *nodeSync) Run(ctx context.Context) (err error) {
	if n.conf.SyncOnStart {
		return n.Sync(ctx)
	}
	return nil
}

func (n *nodeSync) Sync(ctx context.Context) (err error) {
	return nil
}

func (n *nodeSync) getRelatePartitions() (parts []part, err error) {
	ch := n.nodeconf.GetLast().CHash()
	for i := 0; i < ch.PartitionCount(); i++ {
		memb, e := ch.GetPartitionMembers(i)
		if e != nil {
			return nil, e
		}
		if peers := n.getRelateMembers(memb); len(peers) > 0 {
			parts = append(parts, part{
				partId: i,
				peers:  peers,
			})
		}
	}
	return
}

func (n *nodeSync) getRelateMembers(memb []chash.Member) (ids []string) {
	var isRelates bool
	for _, m := range memb {
		if m.Id() == n.peerId {
			isRelates = true
		} else {
			ids = append(ids, m.Id())
		}
	}
	if !isRelates {
		return nil
	}
	return
}

func (n *nodeSync) Close(ctx context.Context) (err error) {
	return nil
}

type part struct {
	partId int
	peers  []string
}
