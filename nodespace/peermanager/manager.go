package peermanager

import (
	"context"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"github.com/anytypeio/any-sync/net/peer"
	"github.com/anytypeio/any-sync/net/pool"
	"github.com/anytypeio/any-sync/net/streampool"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"storj.io/drpc"
	"time"
)

const reconnectTimeout = time.Minute

type responsiblePeer struct {
	peerId   string
	lastFail atomic.Time
}

type nodePeerManager struct {
	spaceId          string
	responsiblePeers []responsiblePeer
	p                *provider
}

func (n *nodePeerManager) init() {
	nodeIds := n.p.nodeconf.NodeIds(n.spaceId)
	for _, peerId := range nodeIds {
		n.responsiblePeers = append(n.responsiblePeers, responsiblePeer{peerId: peerId})
	}
}

func (n *nodePeerManager) SendPeer(ctx context.Context, peerId string, msg *spacesyncproto.ObjectSyncMessage) (err error) {
	ctx = logger.CtxWithFields(context.Background(), logger.CtxGetFields(ctx)...)
	var drpcMsg drpc.Message
	drpcMsg = msg
	if msg.ReplyId != "" || msg.RequestId != "" {
		// prioritize messages with the request or reply by sending it to a separate queue
		drpcMsg = streampool.WithQueueId(msg, "replyQueue")
	}
	if n.isResponsible(peerId) {
		return n.p.streamPool.Send(ctx, drpcMsg, func(ctx context.Context) ([]peer.Peer, error) {
			log.InfoCtx(ctx, "sendPeer send", zap.String("peerId", peerId))
			p, e := n.p.pool.Get(ctx, peerId)
			if e != nil {
				return nil, e
			}
			return []peer.Peer{p}, nil
		})
	}
	log.InfoCtx(ctx, "sendPeer sendById", zap.String("peerId", peerId))
	return n.p.streamPool.SendById(ctx, drpcMsg, peerId)
}

func (n *nodePeerManager) SendResponsible(ctx context.Context, msg *spacesyncproto.ObjectSyncMessage) (err error) {
	ctx = logger.CtxWithFields(context.Background(), logger.CtxGetFields(ctx)...)
	return n.p.streamPool.Send(ctx, msg, func(ctx context.Context) (peers []peer.Peer, err error) {
		return n.getResponsiblePeers(ctx, n.p.pool)
	})
}

func (n *nodePeerManager) Broadcast(ctx context.Context, msg *spacesyncproto.ObjectSyncMessage) (err error) {
	ctx = logger.CtxWithFields(context.Background(), logger.CtxGetFields(ctx)...)
	if e := n.SendResponsible(ctx, msg); e != nil {
		log.InfoCtx(ctx, "broadcast sendResponsible error", zap.Error(e))
	}
	log.InfoCtx(ctx, "broadcast", zap.String("spaceId", n.spaceId))
	return n.p.streamPool.Broadcast(ctx, msg, n.spaceId)
}

func (n *nodePeerManager) GetResponsiblePeers(ctx context.Context) (peers []peer.Peer, err error) {
	return n.getResponsiblePeers(ctx, n.p.commonPool)
}

func (n *nodePeerManager) getResponsiblePeers(ctx context.Context, netPool pool.Pool) (peers []peer.Peer, err error) {
	for _, rp := range n.responsiblePeers {
		if time.Since(rp.lastFail.Load()) > reconnectTimeout {
			p, e := netPool.Get(ctx, rp.peerId)
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

func (n *nodePeerManager) isResponsible(peerId string) bool {
	for _, rp := range n.responsiblePeers {
		if rp.peerId == peerId {
			return true
		}
	}
	return false
}
