package treesyncer

import (
	"context"

	"github.com/anyproto/any-sync/net/peer"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
)

var log = logger.NewNamed(treesyncer.CName)

func New(spaceId string) treesyncer.TreeSyncer {
	return &treeSyncer{spaceId: spaceId}
}

type treeSyncer struct {
	spaceId     string
	treeManager treemanager.TreeManager
}

func (t *treeSyncer) Init(a *app.App) (err error) {
	t.treeManager = a.MustComponent(treemanager.CName).(treemanager.TreeManager)
	return
}

func (t *treeSyncer) Name() (name string) {
	return treesyncer.CName
}

func (t *treeSyncer) Run(ctx context.Context) (err error) {
	return nil
}

func (t *treeSyncer) Close(ctx context.Context) (err error) {
	return nil
}

func (t *treeSyncer) StartSync() {
}

func (t *treeSyncer) StopSync() {
}

func (t *treeSyncer) ShouldSync(peerId string) bool {
	return true
}

func (t *treeSyncer) SyncAll(ctx context.Context, p peer.Peer, existing, missing []string) (err error) {
	// TODO: copied from any-sync's previous version, should change later if needed to use queues
	//  problem here is that all sync process is basically synchronous and has same timeout
	ctx = peer.CtxWithPeerId(ctx, p.Id())
	syncTrees := func(ids []string) {
		for _, id := range ids {
			log := log.With(zap.String("treeId", id))
			tr, err := t.treeManager.GetTree(ctx, t.spaceId, id)
			if err != nil {
				log.WarnCtx(ctx, "can't load existing tree", zap.Error(err))
				return
			}
			syncTree, ok := tr.(synctree.SyncTree)
			if !ok {
				log.WarnCtx(ctx, "not a sync tree")
			}
			if err = syncTree.SyncWithPeer(ctx, p); err != nil {
				log.WarnCtx(ctx, "synctree.SyncWithPeer error", zap.Error(err))
			} else {
				log.DebugCtx(ctx, "success synctree.SyncWithPeer")
			}
		}
	}
	syncTrees(missing)
	syncTrees(existing)
	return
}
