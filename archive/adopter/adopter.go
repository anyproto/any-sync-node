//go:generate mockgen -destination mock_adopter/mock_adopter.go github.com/anyproto/any-sync-node/archive/adopter Adopter
package adopter

import (
	"context"
	"errors"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
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

type adopter struct {
	storage      nodestorage.NodeStorage
	archiveStore archivestore.ArchiveStore
	archive      archive.Archive
	nodeHead     nodehead.NodeHead
	nodeConf     nodeconf.Service
}

func (ad *adopter) Init(a *app.App) (err error) {
	ad.storage = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	ad.archiveStore = a.MustComponent(archivestore.CName).(archivestore.ArchiveStore)
	ad.archive = a.MustComponent(archive.CName).(archive.Archive)
	ad.nodeHead = a.MustComponent(nodehead.CName).(nodehead.NodeHead)
	ad.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	return
}

func (ad *adopter) Name() (name string) {
	return CName
}

func (ad *adopter) AdoptArchive(ctx context.Context, req *nodesyncproto.AdoptArchiveRequest) (resp *nodesyncproto.AdoptArchiveResponse, err error) {
	if !ad.archiveStore.Shared() {
		return nil, nodesyncproto.ErrArchiveUnavailable
	}
	if err = ad.checkPeerIsNode(ctx); err != nil {
		return nil, err
	}
	entry, entryErr := ad.storage.IndexStorage().SpaceStatusEntry(ctx, req.SpaceId)
	switch {
	case entryErr == nil:
		switch entry.Status {
		case nodestorage.SpaceStatusRemove, nodestorage.SpaceStatusRemovePrepare:
			return nil, nodesyncproto.ErrSpaceDeleted
		case nodestorage.SpaceStatusOk:
			if ad.storage.SpaceExists(req.SpaceId) {
				return &nodesyncproto.AdoptArchiveResponse{
					Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHave,
				}, nil
			}
			// index entry says ok, but there is no local db: adopt to repair
		case nodestorage.SpaceStatusArchived:
			ok, hErr := ad.archiveStore.Exists(ctx, req.SpaceId)
			if hErr != nil {
				return nil, hErr
			}
			if ok {
				return &nodesyncproto.AdoptArchiveResponse{
					Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHave,
				}, nil
			}
			// index says archived, but our object is gone: adopt to repair
		}
	case errors.Is(entryErr, anystore.ErrDocNotFound):
		if ad.storage.SpaceExists(req.SpaceId) {
			return &nodesyncproto.AdoptArchiveResponse{
				Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHave,
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

func (ad *adopter) checkPeerIsNode(ctx context.Context) (err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return err
	}
	if len(ad.nodeConf.NodeTypes(peerId)) == 0 {
		return nodesyncproto.ErrPeerIsNotNode
	}
	return nil
}
