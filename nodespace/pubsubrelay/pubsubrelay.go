// Package pubsubrelay wires the any-sync commonspace/pubsub engine into the node
// as a relay: it authorizes subscribe/publish against the hosted space ACL,
// forwards client-originated messages to the other responsible nodes, and evicts
// removed members when a space's ACL changes.
package pubsubrelay

import (
	"context"
	"errors"
	"slices"

	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/pubsub"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodespace"
)

const CName = "node.pubsubrelay"

var log = logger.NewNamed(CName)

var errNotMember = errors.New("account is not a space member")

func New() app.ComponentRunnable {
	return &relay{}
}

// relay implements the pubsub engine's Relay and MembershipChecker deps and owns
// the engine's lifecycle. The engine itself is not registered in the app graph;
// this component drives its Init/Run/Close.
type relay struct {
	nodeconf   nodeconf.Service
	pool       pool.Pool
	nodeSpace  nodespace.Service
	account    *accountdata.AccountKeys
	engine     pubsub.Service
	selfPeerId string
	ctx        context.Context
	ctxCancel  context.CancelFunc
}

func (r *relay) Init(a *app.App) (err error) {
	r.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	r.pool = a.MustComponent(pool.CName).(pool.Service)
	r.nodeSpace = a.MustComponent(nodespace.CName).(nodespace.Service)
	r.account = a.MustComponent(accountservice.CName).(accountservice.Service).Account()
	r.selfPeerId = r.account.PeerId
	r.ctx, r.ctxCancel = context.WithCancel(context.Background())

	m, _ := a.Component(metric.CName).(metric.Metric)
	r.engine = pubsub.New(pubsub.Deps{
		Relay:      r,
		Membership: r,
		Metric:     m,
		// Crypto is nil: the node relays ciphertext it cannot read.
		// Peers is nil: the node routes via Relay, not a client peer provider.
	})
	if err = r.engine.Init(a); err != nil {
		return err
	}
	return pubsub.RegisterRpc(a.MustComponent(server.CName).(server.DRPCServer), r.engine)
}

func (r *relay) Name() string { return CName }

func (r *relay) Run(ctx context.Context) error {
	// re-check subscriber membership whenever a hosted space's ACL changes
	r.nodeSpace.SetAclObserver(r.onAclUpdate)
	return r.engine.Run(ctx)
}

func (r *relay) Close(ctx context.Context) error {
	r.ctxCancel()
	return r.engine.Close(ctx)
}

//
// pubsub.Relay
//

func (r *relay) IsResponsible(spaceId string) bool {
	return r.nodeconf.IsResponsible(spaceId)
}

func (r *relay) IsResponsibleNode(spaceId, peerId string) bool {
	return slices.Contains(r.nodeconf.NodeIds(spaceId), peerId)
}

func (r *relay) OtherResponsiblePeers(ctx context.Context, spaceId string) ([]peer.Peer, error) {
	var peers []peer.Peer
	for _, peerId := range r.nodeconf.NodeIds(spaceId) {
		if peerId == r.selfPeerId {
			continue
		}
		p, err := r.pool.Get(ctx, peerId)
		if err != nil {
			log.InfoCtx(ctx, "can't dial responsible node", zap.String("peerId", peerId), zap.Error(err))
			continue
		}
		peers = append(peers, p)
	}
	return peers, nil
}

//
// pubsub.MembershipChecker
//

func (r *relay) CheckMember(ctx context.Context, spaceId string, identity crypto.PubKey) error {
	perms, err := r.permissions(ctx, spaceId, identity)
	if err != nil {
		return err
	}
	if perms.NoPermissions() {
		return errNotMember
	}
	return nil
}

func (r *relay) permissions(ctx context.Context, spaceId string, identity crypto.PubKey) (list.AclPermissions, error) {
	sp, err := r.nodeSpace.GetSpace(ctx, spaceId)
	if err != nil {
		return list.AclPermissionsNone, err
	}
	acl := sp.Acl()
	acl.RLock()
	defer acl.RUnlock()
	return acl.AclState().Permissions(identity), nil
}

//
// ACL-change eviction (DESIGN §6.4)
//

func (r *relay) onAclUpdate(spaceId string) {
	sp, err := r.nodeSpace.GetSpace(r.ctx, spaceId)
	if err != nil {
		log.Warn("acl update: can't get space", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	acl := sp.Acl()
	acl.RLock()
	members := make(map[string]struct{})
	for _, acc := range acl.AclState().CurrentAccounts() {
		if !acc.Permissions.NoPermissions() {
			members[acc.PubKey.Account()] = struct{}{}
		}
	}
	acl.RUnlock()
	r.engine.RevalidateMembers(spaceId, func(account string) bool {
		_, ok := members[account]
		return ok
	})
}
