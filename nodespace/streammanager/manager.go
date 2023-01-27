package streammanager

import (
	"context"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"github.com/anytypeio/any-sync/net/peer"
	"github.com/anytypeio/any-sync/net/pool"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"time"
)

const reconnectTimeout = time.Minute

type responsiblePeer struct {
	peerId   string
	lastFail atomic.Time
}

type nodeStreamManager struct {
	spaceId          string
	responsiblePeers []responsiblePeer
	p                *provider
}

func (n *nodeStreamManager) init() {
	nodeIds := n.p.nodeconf.GetLast().NodeIds(n.spaceId)
	for _, peerId := range nodeIds {
		n.responsiblePeers = append(n.responsiblePeers, responsiblePeer{peerId: peerId})
	}
}

func (n *nodeStreamManager) SendPeer(ctx context.Context, peerId string, msg *spacesyncproto.ObjectSyncMessage) (err error) {
	ctx = logger.CtxWithFields(context.Background(), logger.CtxGetFields(ctx)...)
	if n.isResponsible(peerId) {
		var p peer.Peer
		p, err = n.p.pool.Get(ctx, peerId)
		if err != nil {
			return
		}
		log.InfoCtx(ctx, "sendPeer send", zap.String("peerId", peerId))
		return n.p.streamPool.Send(ctx, msg, p)
	}
	log.InfoCtx(ctx, "sendPeer sendById", zap.String("peerId", peerId))
	return n.p.streamPool.SendById(ctx, msg, peerId)
}

func (n *nodeStreamManager) SendResponsible(ctx context.Context, msg *spacesyncproto.ObjectSyncMessage) (err error) {
	ctx = logger.CtxWithFields(context.Background(), logger.CtxGetFields(ctx)...)
	peers, err := n.getResponsiblePeers(ctx)
	if err != nil {
		return
	}
	var peerIds = make([]string, 0, len(peers))
	for _, p := range peers {
		peerIds = append(peerIds, p.Id())
	}
	log.InfoCtx(ctx, "sendResponsible", zap.Strings("peerIds", peerIds))
	return n.p.streamPool.Send(ctx, msg, peers...)
}

func (n *nodeStreamManager) Broadcast(ctx context.Context, msg *spacesyncproto.ObjectSyncMessage) (err error) {
	ctx = logger.CtxWithFields(context.Background(), logger.CtxGetFields(ctx)...)
	if e := n.SendResponsible(ctx, msg); e != nil {
		log.InfoCtx(ctx, "broadcast sendResponsible error", zap.Error(e))
	}
	log.InfoCtx(ctx, "broadcast", zap.String("spaceId", n.spaceId))
	return n.p.streamPool.Broadcast(ctx, msg, n.spaceId)
}

func (n *nodeStreamManager) getResponsiblePeers(ctx context.Context) (peers []peer.Peer, err error) {
	for _, rp := range n.responsiblePeers {
		if time.Since(rp.lastFail.Load()) > reconnectTimeout {
			p, e := n.p.pool.Get(ctx, rp.peerId)
			if e != nil {
				log.InfoCtx(ctx, "can't connect to peer", zap.Error(err), zap.String("peerId", rp.peerId))
				rp.lastFail.Store(time.Now())
				continue
			}
			peers = append(peers, p)
		}
	}
	if len(peers) == 0 {
		return nil, pool.ErrUnableToConnect
	}
	return
}

func (n *nodeStreamManager) isResponsible(peerId string) bool {
	for _, rp := range n.responsiblePeers {
		if rp.peerId == peerId {
			return true
		}
	}
	return false
}
