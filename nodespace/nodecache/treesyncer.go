package nodecache

import (
	"context"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
	"go.uber.org/zap"
)

type treeSyncer struct {
	spaceId     string
	treeManager treemanager.TreeManager
}

func NewTreeSyncer(spaceId string, treeManager treemanager.TreeManager) treemanager.TreeSyncer {
	return &treeSyncer{
		spaceId:     spaceId,
		treeManager: treeManager,
	}
}

func (t *treeSyncer) Init() {
	return
}

func (t *treeSyncer) SyncAll(ctx context.Context, peerId string, existing, missing []string) (err error) {
	// TODO: copied from any-sync's previous version, should change later if needed to use queues
	//  problem here is that all sync process is basically synchronous and has same timeout
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
			if err = syncTree.SyncWithPeer(ctx, peerId); err != nil {
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

func (t *treeSyncer) Close() error {
	return nil
}
