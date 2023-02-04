package nodesync

import (
	"context"
	"fmt"
	"github.com/anytypeio/any-sync-node/nodehead"
	"github.com/anytypeio/any-sync-node/nodespace"
	"github.com/anytypeio/any-sync-node/nodesync/coldsync"
	"github.com/anytypeio/any-sync-node/nodesync/nodesyncproto"
	commonaccount "github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/metric"
	"github.com/anytypeio/any-sync/net/pool"
	"github.com/anytypeio/any-sync/net/rpc/server"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/anytypeio/go-chash"
	"go.uber.org/zap"
	"sync"
	"time"
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
	nodeconf       nodeconf.Service
	nodehead       nodehead.NodeHead
	nodespace      nodespace.Service
	coldsync       coldsync.ColdSync
	pool           pool.Pool
	conf           Config
	peerId         string
	syncMu         sync.Mutex
	syncInProgress chan struct{}
	syncStat       *SyncStat
}

func (n *nodeSync) Init(a *app.App) (err error) {
	n.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	n.nodehead = a.MustComponent(nodehead.CName).(nodehead.NodeHead)
	n.nodespace = a.MustComponent(nodespace.CName).(nodespace.Service)
	n.coldsync = a.MustComponent(coldsync.CName).(coldsync.ColdSync)
	n.peerId = a.MustComponent(commonaccount.CName).(commonaccount.Service).Account().PeerId
	n.pool = a.MustComponent(pool.CName).(pool.Service).NewPool("nodesync")
	n.conf = a.MustComponent("config").(configGetter).GetNodeSync()
	n.syncStat = new(SyncStat)
	if m := a.Component(metric.CName); m != nil {
		registerMetric(n.syncStat, m.(metric.Metric).Registry())
	}

	return nodesyncproto.DRPCRegisterNodeSync(a.MustComponent(server.CName).(server.DRPCServer), &rpcHandler{
		nodeRemoteDiffHandler: &nodeRemoteDiffHandler{nodehead: n.nodehead},
		coldSync:              n.coldsync,
	})
}

func (n *nodeSync) Name() (name string) {
	return CName
}

func (n *nodeSync) Run(ctx context.Context) (err error) {
	if n.conf.SyncOnStart {
		go func() {
			if e := n.Sync(context.Background()); e != nil {
				log.Warn("nodesync failed", zap.Error(e))
			}
		}()
	}
	return nil
}

func (n *nodeSync) Sync(ctx context.Context) (err error) {
	n.syncMu.Lock()
	if n.syncInProgress != nil {
		n.syncMu.Unlock()
		return fmt.Errorf("sync in progress")
	} else {
		n.syncInProgress = make(chan struct{})
	}
	n.syncMu.Unlock()
	defer func() {
		n.syncMu.Lock()
		defer n.syncMu.Unlock()
		close(n.syncInProgress)
		n.syncInProgress = nil
		n.syncStat.InProgress.Store(false)
		n.syncStat.SyncsDone.Add(1)
	}()

	st := time.Now()
	n.syncStat.InProgress.Store(true)
	n.syncStat.LastStartTime.Store(uint64(st.Unix()))

	parts, err := n.getRelatePartitions()
	if err != nil {
		return err
	}
	n.syncStat.PartsTotal.Store(uint32(len(parts)))
	n.syncStat.PartsHandled.Store(0)

	log.Info("nodesync started...", zap.Int("partitions", len(parts)))
	var limiter = make(chan struct{}, 10)
	var wg sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		limiter <- struct{}{}
		go func(p part) {
			defer func() { <-limiter }()
			defer wg.Done()
			defer n.syncStat.PartsHandled.Add(1)
			if e := n.syncPart(ctx, p); e != nil {
				log.Warn("can't sync part", zap.Int("part", p.partId), zap.Error(e))
				n.syncStat.PartsErrors.Add(1)
			}
		}(p)
	}
	wg.Wait()
	dur := time.Since(st)
	n.syncStat.LastDuration.Store(uint64(dur))
	log.Info("nodesync done", zap.Duration("dur", dur))
	return nil
}

func (n *nodeSync) syncPart(ctx context.Context, p part) (err error) {
	var (
		hasSuccess bool
	)
	for _, peerId := range p.peers {
		if err = n.syncPeer(ctx, peerId, p.partId); err != nil {
			log.Info("syncPeer failed", zap.String("peerId", peerId), zap.Int("part", p.partId), zap.Error(err))
		} else {
			hasSuccess = true
		}
	}
	if hasSuccess {
		return nil
	}
	return
}

func (n *nodeSync) syncPeer(ctx context.Context, peerId string, partId int) (err error) {
	p, err := n.pool.Get(ctx, peerId)
	if err != nil {
		return
	}
	ld := n.nodehead.LDiff(partId)
	newIds, changedIds, _, err := ld.Diff(ctx, nodeRemoteDiff{
		partId: partId,
		cl:     nodesyncproto.NewDRPCNodeSyncClient(p),
	})
	if err != nil {
		return
	}
	for _, newId := range newIds {
		if e := n.coldSync(ctx, newId, peerId); e != nil {
			log.Warn("can't coldSync space with peer", zap.String("spaceId", newId), zap.String("peerId", peerId), zap.Error(e))
			n.syncStat.ColdSyncErrors.Add(1)
		}
		n.syncStat.ColdSyncHandled.Add(1)
	}
	for _, changedId := range changedIds {
		if e := n.hotSync(ctx, changedId); e != nil {
			log.Warn("can't hotSync space", zap.String("spaceId", changedId), zap.Error(e))
			n.syncStat.HotSyncErrors.Add(1)
		}
		n.syncStat.HotSyncHandled.Add(1)
	}
	return
}

func (n *nodeSync) coldSync(ctx context.Context, spaceId, peerId string) (err error) {
	if err = n.coldsync.Sync(ctx, spaceId, peerId); err != nil {
		return
	}
	return n.nodehead.ReloadHeadFromStore(spaceId)
}

func (n *nodeSync) hotSync(ctx context.Context, spaceId string) (err error) {
	// just wakeup a space
	_, err = n.nodespace.GetSpace(ctx, spaceId)
	return
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
	n.syncMu.Lock()
	syncInProgress := n.syncInProgress
	n.syncMu.Unlock()
	if syncInProgress != nil {
		select {
		case <-syncInProgress:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type part struct {
	partId int
	peers  []string
}
