package resharder

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-node/archive"
	"github.com/anyproto/any-sync-node/archive/archivestore"
	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/hotsync"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

const CName = "node.resharder"

var log = logger.NewNamed(CName)

const (
	defaultDrainIntervalMinutes = 60
	// spaceOpTimeout bounds a single space handoff (snapshot + up to RF adopt RPCs)
	spaceOpTimeout = time.Minute * 10
)

func New() Resharder {
	return new(resharder)
}

// Resharder implements the draining side of resharding: when the network
// configuration changes and this node is no longer responsible for a space,
// the resharder hands the space off to the current owners through the shared
// archive store (AdoptArchive) and deletes the local copy only after at least
// two current owners durably hold the same heads.
//
// The machinery is gated on the archive store being shared (s3Store.shared);
// networks without a shared bucket keep today's behavior.
type Resharder interface {
	app.ComponentRunnable
}

type configGetter interface {
	GetResharder() Config
}

type Config struct {
	// DrainIntervalMinutes is the period of the background drain cycle
	// (it also runs immediately on a network configuration change).
	DrainIntervalMinutes int `yaml:"drainIntervalMinutes"`
}

type resharder struct {
	nodeConf     nodeconf.Service
	storage      nodestorage.NodeStorage
	nodeHead     nodehead.NodeHead
	archive      archive.Archive
	archiveStore archivestore.ArchiveStore
	pool         pool.Pool
	hotSync      hotsync.HotSync
	conf         Config

	trigger      chan struct{}
	runCtx       context.Context
	runCtxCancel context.CancelFunc
	done         chan struct{}
	stat         *resharderStat

	// adoptFn is the AdoptArchive call to a peer; replaceable in tests
	adoptFn func(ctx context.Context, peerId string, req *nodesyncproto.AdoptArchiveRequest) (*nodesyncproto.AdoptArchiveResponse, error)
}

func (r *resharder) Init(a *app.App) (err error) {
	r.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	r.storage = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	r.nodeHead = a.MustComponent(nodehead.CName).(nodehead.NodeHead)
	r.archive = a.MustComponent(archive.CName).(archive.Archive)
	r.archiveStore = a.MustComponent(archivestore.CName).(archivestore.ArchiveStore)
	r.pool = a.MustComponent(pool.CName).(pool.Pool)
	r.hotSync = a.MustComponent(hotsync.CName).(hotsync.HotSync)
	if g, ok := a.Component("config").(configGetter); ok {
		r.conf = g.GetResharder()
	}
	if r.conf.DrainIntervalMinutes <= 0 {
		r.conf.DrainIntervalMinutes = defaultDrainIntervalMinutes
	}
	r.trigger = make(chan struct{}, 1)
	r.done = make(chan struct{})
	r.runCtx, r.runCtxCancel = context.WithCancel(context.Background())
	r.stat = new(resharderStat)
	if r.adoptFn == nil {
		r.adoptFn = r.adopt
	}
	if m := a.Component(metric.CName); m != nil {
		registerMetric(r.stat, m.(metric.Metric).Registry())
	}
	r.nodeConf.ObserveChanges(func(prev, cur nodeconf.NodeConf) {
		log.Info("network configuration changed, scheduling drain cycle",
			zap.Uint64("prevEpoch", prev.Configuration().Epoch),
			zap.Uint64("curEpoch", cur.Configuration().Epoch))
		r.Trigger()
	})
	return
}

func (r *resharder) Name() (name string) {
	return CName
}

func (r *resharder) Run(_ context.Context) (err error) {
	if !r.archiveStore.Shared() {
		log.Info("archive store is not shared: resharding drain is disabled")
		close(r.done)
		return
	}
	go r.loop()
	return
}

// Trigger schedules an immediate drain cycle.
func (r *resharder) Trigger() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

func (r *resharder) loop() {
	defer close(r.done)
	ticker := time.NewTicker(time.Duration(r.conf.DrainIntervalMinutes) * time.Minute)
	defer ticker.Stop()
	// initial cycle picks up drains interrupted by a restart
	r.drainCycle()
	for {
		select {
		case <-r.runCtx.Done():
			return
		case <-r.trigger:
		case <-ticker.C:
		}
		r.drainCycle()
	}
}

func (r *resharder) drainCycle() {
	st := time.Now()
	var candidates []string
	err := r.storage.IndexStorage().ReadHashes(r.runCtx, func(update nodestorage.SpaceUpdate) (bool, error) {
		if !r.nodeConf.IsResponsible(update.SpaceId) {
			candidates = append(candidates, update.SpaceId)
		}
		return true, nil
	})
	if err != nil {
		log.Warn("drain cycle: can't read index", zap.Error(err))
		return
	}
	r.stat.draining.Store(uint32(len(candidates)))
	if len(candidates) == 0 {
		return
	}
	log.Info("drain cycle started", zap.Int("spaces", len(candidates)))
	var moved, parked int
	for _, spaceId := range candidates {
		if r.runCtx.Err() != nil {
			return
		}
		ok, err := r.drainSpace(spaceId)
		if err != nil {
			log.Warn("drain space failed", zap.String("spaceId", spaceId), zap.Error(err))
		}
		if ok {
			moved++
			r.stat.moved.Add(1)
		} else {
			parked++
		}
	}
	r.stat.draining.Store(uint32(parked))
	log.Info("drain cycle finished", zap.Int("moved", moved), zap.Int("parked", parked), zap.Duration("dur", time.Since(st)))
}

// drainSpace hands one space off to the current owners. It returns ok=true
// when the local copy was deleted (or the space turned out to be handled
// already); ok=false parks the space for the next cycle.
func (r *resharder) drainSpace(spaceId string) (ok bool, err error) {
	ctx, cancel := context.WithTimeout(r.runCtx, spaceOpTimeout)
	defer cancel()

	index := r.storage.IndexStorage()
	entry, err := index.SpaceStatusEntry(ctx, spaceId)
	if err != nil {
		return false, err
	}
	switch entry.Status {
	case nodestorage.SpaceStatusOk, nodestorage.SpaceStatusArchived:
	default:
		// deletion flow, errors and already-moved spaces are not drained
		return true, nil
	}
	// the configuration could have changed since the candidate list was built
	if r.nodeConf.IsResponsible(spaceId) {
		return true, nil
	}

	eager := false
	switch entry.Status {
	case nodestorage.SpaceStatusArchived:
		exists, hErr := r.archiveStore.Exists(ctx, spaceId)
		if hErr != nil {
			return false, hErr
		}
		if !exists {
			if !r.storage.SpaceExists(spaceId) {
				// no data anywhere locally: nothing to hand off; leave the entry
				// for the operator, the owners sync from the other replicas
				return false, index.MarkError(ctx, spaceId, "drain: archived but object and db are missing")
			}
			// the object is gone but the db is present: snapshot it again
			if _, _, err = r.archive.ForceArchive(ctx, spaceId); err != nil {
				return false, err
			}
		}
	case nodestorage.SpaceStatusOk:
		if !r.storage.SpaceExists(spaceId) {
			// index entry without local data: nothing to hand off
			return true, index.SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusMoved, "")
		}
		if _, _, err = r.archive.ForceArchive(ctx, spaceId); err != nil {
			return false, err
		}
		// refresh heads: writes could have landed before the snapshot
		if entry, err = index.SpaceStatusEntry(ctx, spaceId); err != nil {
			return false, err
		}
		eager = true
	}

	req := &nodesyncproto.AdoptArchiveRequest{
		SpaceId:          spaceId,
		SrcKey:           r.archiveStore.Key(spaceId),
		OldHash:          entry.OldHash,
		NewHash:          entry.NewHash,
		Eager:            eager,
		CompressedSize:   entry.ArchiveSizeCompressed,
		UncompressedSize: entry.ArchiveSizeUncompressed,
	}

	owners := r.nodeConf.NodeIds(spaceId)
	minAcks := 2
	if len(owners) < minAcks {
		minAcks = len(owners)
	}
	var acks, diverged int
	for _, peerId := range owners {
		resp, aErr := r.adoptFn(ctx, peerId, req)
		if aErr != nil {
			if errors.Is(aErr, nodesyncproto.ErrSpaceDeleted) {
				// the network deleted this space: drop the local copy
				log.Info("drain: space is deleted by the network", zap.String("spaceId", spaceId))
				return true, r.deleteLocal(ctx, spaceId, entry)
			}
			log.Debug("drain: adopt failed", zap.String("spaceId", spaceId), zap.String("peerId", peerId), zap.Error(aErr))
			continue
		}
		switch resp.Result {
		case nodesyncproto.AdoptArchiveResult_AdoptArchiveOk,
			nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveSame:
			acks++
		case nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveDiverged:
			diverged++
		}
	}
	if acks < minAcks {
		if diverged > 0 {
			// an owner holds a different version: converge via tree sync
			// (loading the space syncs it with the current owners), retry next cycle
			r.hotSync.UpdateQueue([]string{spaceId})
		}
		r.stat.parked.Add(1)
		return false, nil
	}
	// deletion safety: the heads we got ACKed must still be the local heads
	cur, err := index.SpaceStatusEntry(ctx, spaceId)
	if err != nil {
		return false, err
	}
	if cur.NewHash != entry.NewHash || cur.OldHash != entry.OldHash {
		log.Info("drain: heads changed during handoff, retrying next cycle", zap.String("spaceId", spaceId))
		return false, nil
	}
	return true, r.deleteLocal(ctx, spaceId, entry)
}

func (r *resharder) deleteLocal(ctx context.Context, spaceId string, entry nodestorage.SpaceStatusEntry) (err error) {
	// delete data first, status marker last: a crash in between leaves the
	// space a drain candidate and the next cycle finishes the job
	if r.storage.SpaceExists(spaceId) {
		if err = r.storage.DeleteSpaceStorage(ctx, spaceId); err != nil {
			return
		}
	} else {
		if err = r.nodeHead.DeleteHeads(spaceId); err != nil {
			log.Warn("drain: can't delete heads", zap.String("spaceId", spaceId), zap.Error(err))
			err = nil
		}
	}
	if dErr := r.archiveStore.Delete(ctx, spaceId); dErr != nil && !errors.Is(dErr, archivestore.ErrNotFound) {
		log.Warn("drain: can't delete archive object", zap.String("spaceId", spaceId), zap.Error(dErr))
	}
	if err = r.storage.IndexStorage().SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusMoved, ""); err != nil {
		return
	}
	log.Info("space handed off and deleted locally", zap.String("spaceId", spaceId))
	return
}

func (r *resharder) adopt(ctx context.Context, peerId string, req *nodesyncproto.AdoptArchiveRequest) (resp *nodesyncproto.AdoptArchiveResponse, err error) {
	p, err := r.pool.Get(ctx, peerId)
	if err != nil {
		return
	}
	err = p.DoDrpc(ctx, func(conn drpc.Conn) error {
		var dErr error
		resp, dErr = nodesyncproto.NewDRPCNodeSyncClient(conn).AdoptArchive(ctx, req)
		return dErr
	})
	if err != nil {
		return nil, rpcerr.Unwrap(err)
	}
	return
}

func (r *resharder) Close(_ context.Context) (err error) {
	r.runCtxCancel()
	select {
	case <-r.done:
	case <-time.After(time.Second * 10):
	}
	return
}
