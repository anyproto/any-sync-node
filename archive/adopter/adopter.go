//go:generate mockgen -destination mock_adopter/mock_adopter.go github.com/anyproto/any-sync-node/archive/adopter Adopter
package adopter

import (
	"context"
	"errors"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/archive"
	"github.com/anyproto/any-sync-node/archive/archivestore"
	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

const CName = "node.archive.adopter"

var log = logger.NewNamed(CName)

func New() Adopter {
	return new(adopter)
}

// Adopter handles AdoptArchive requests: another node hands over an archived
// space snapshot parked in the shared bucket; we server-side copy the object
// into our own prefix and register the space as archived without ever
// materializing the SQLite DB.
type Adopter interface {
	app.Component
	AdoptArchive(ctx context.Context, req *nodesyncproto.AdoptArchiveRequest) (resp *nodesyncproto.AdoptArchiveResponse, err error)
}

// historyPeerEpochs is how many retained configuration epochs back a sender
// may be recognized as a network node: a node removed from the configuration
// must still be able to hand its spaces off while it drains.
const historyPeerEpochs = 5

type adopter struct {
	storage      nodestorage.NodeStorage
	archiveStore archivestore.ArchiveStore
	archive      archive.Archive
	nodeHead     nodehead.NodeHead
	nodeConf     nodeconf.Service
	confHistory  nodeconf.HistoryStore
	stat         *adopterStat
}

func (ad *adopter) Init(a *app.App) (err error) {
	ad.storage = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	ad.archiveStore = a.MustComponent(archivestore.CName).(archivestore.ArchiveStore)
	ad.archive = a.MustComponent(archive.CName).(archive.Archive)
	ad.nodeHead = a.MustComponent(nodehead.CName).(nodehead.NodeHead)
	ad.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	ad.confHistory, _ = a.Component(nodeconf.CNameStore).(nodeconf.HistoryStore)
	ad.stat = new(adopterStat)
	if m := a.Component(metric.CName); m != nil {
		registerMetric(ad.stat, m.(metric.Metric).Registry())
	}
	return
}

func (ad *adopter) Name() (name string) {
	return CName
}

func (ad *adopter) AdoptArchive(ctx context.Context, req *nodesyncproto.AdoptArchiveRequest) (resp *nodesyncproto.AdoptArchiveResponse, err error) {
	defer func() {
		switch {
		case err != nil:
			ad.stat.rejected.Add(1)
		case resp.Result == nodesyncproto.AdoptArchiveResult_AdoptArchiveOk:
			ad.stat.adopted.Add(1)
		case resp.Result == nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveSame:
			ad.stat.alreadyHaveSame.Add(1)
		case resp.Result == nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveDiverged:
			ad.stat.alreadyHaveDiverged.Add(1)
		}
	}()
	if !ad.archiveStore.Shared() {
		return nil, nodesyncproto.ErrArchiveUnavailable
	}
	if err = ad.checkPeerIsNode(ctx); err != nil {
		return nil, err
	}
	if !ad.nodeConf.IsResponsible(req.SpaceId) {
		// the sender acts on a different (stale or newer) configuration:
		// never adopt spaces we don't own — an ACK from a non-owner must not
		// justify the sender's deletion
		return nil, nodesyncproto.ErrNotResponsible
	}
	entry, entryErr := ad.storage.IndexStorage().SpaceStatusEntry(ctx, req.SpaceId)
	switch {
	case entryErr == nil:
		switch entry.Status {
		case nodestorage.SpaceStatusRemove:
			return nil, nodesyncproto.ErrSpaceDeleted
		case nodestorage.SpaceStatusRemovePrepare:
			// pending deletion is cancellable: don't let the sender drop its
			// copy, and don't adopt data into a deletion-flow status either;
			// the sender parks the space until the deletion resolves
			return nil, nodesyncproto.ErrSpacePendingDeletion
		case nodestorage.SpaceStatusOk:
			if ad.storage.SpaceExists(req.SpaceId) {
				return alreadyHaveResponse(entry, req), nil
			}
			// index entry says ok, but there is no local db: adopt to repair
		case nodestorage.SpaceStatusArchived:
			ok, hErr := ad.archiveStore.Exists(ctx, req.SpaceId)
			if hErr != nil {
				return nil, hErr
			}
			if ok {
				return alreadyHaveResponse(entry, req), nil
			}
			// index says archived, but our object is gone: adopt to repair
		case nodestorage.SpaceStatusError:
			// an Error entry needs an operator: the local db may hold data
			// that failed to archive — never overwrite it with a snapshot and
			// never ACK the sender's deletion with a broken copy
			return &nodesyncproto.AdoptArchiveResponse{
				Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveDiverged,
			}, nil
		}
	case errors.Is(entryErr, anystore.ErrDocNotFound):
		if ad.storage.SpaceExists(req.SpaceId) {
			// a local db without an index entry: heads unknown, force convergence
			return &nodesyncproto.AdoptArchiveResponse{
				Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveDiverged,
			}, nil
		}
	default:
		return nil, entryErr
	}

	if err = ad.archiveStore.CopyFrom(ctx, req.SrcKey, req.SpaceId); err != nil {
		if errors.Is(err, archivestore.ErrNotFound) {
			return nil, nodesyncproto.ErrArchiveObjectMissing
		}
		return nil, err
	}
	ok, err := ad.archiveStore.Exists(ctx, req.SpaceId)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nodesyncproto.ErrArchiveObjectMissing
	}
	if err = ad.storage.IndexStorage().MarkArchivedRemote(ctx, req.SpaceId, req.OldHash, req.NewHash, req.CompressedSize, req.UncompressedSize); err != nil {
		return nil, err
	}
	if _, err = ad.nodeHead.SetHead(req.SpaceId, req.OldHash, req.NewHash); err != nil {
		log.Warn("can't set nodehead after adoption", zap.String("spaceId", req.SpaceId), zap.Error(err))
		err = nil
	}
	log.Info("adopted archived space", zap.String("spaceId", req.SpaceId), zap.Bool("eager", req.Eager))
	if req.Eager {
		ad.archive.QueueRestore(req.SpaceId)
	}
	return &nodesyncproto.AdoptArchiveResponse{
		Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveOk,
	}, nil
}

// alreadyHaveResponse reports whether our existing copy matches the offered
// heads: only an exact match counts as a durable ACK for the sender — a
// diverged copy could be older than the sender's and must not justify its
// deletion.
func alreadyHaveResponse(entry nodestorage.SpaceStatusEntry, req *nodesyncproto.AdoptArchiveRequest) *nodesyncproto.AdoptArchiveResponse {
	if entry.NewHash == req.NewHash && entry.OldHash == req.OldHash {
		return &nodesyncproto.AdoptArchiveResponse{
			Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveSame,
		}
	}
	return &nodesyncproto.AdoptArchiveResponse{
		Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveDiverged,
	}
}

func (ad *adopter) checkPeerIsNode(ctx context.Context) (err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return err
	}
	if len(ad.nodeConf.NodeTypes(peerId)) > 0 {
		return nil
	}
	// a node removed from the configuration keeps draining after the removal:
	// recognize senders that were tree nodes in recently retained epochs
	if ad.wasRecentTreeNode(ctx, peerId) {
		return nil
	}
	return nodesyncproto.ErrPeerIsNotNode
}

func (ad *adopter) wasRecentTreeNode(ctx context.Context, peerId string) bool {
	if ad.confHistory == nil {
		return false
	}
	netId := ad.nodeConf.Configuration().NetworkId
	epochs, err := ad.confHistory.Epochs(ctx, netId)
	if err != nil || len(epochs) == 0 {
		return false
	}
	if len(epochs) > historyPeerEpochs {
		epochs = epochs[len(epochs)-historyPeerEpochs:]
	}
	for i := len(epochs) - 1; i >= 0; i-- {
		conf, gErr := ad.confHistory.GetByEpoch(ctx, netId, epochs[i])
		if gErr != nil {
			continue
		}
		for _, n := range conf.Nodes {
			if n.PeerId == peerId && n.HasType(nodeconf.NodeTypeTree) {
				return true
			}
		}
	}
	return false
}
