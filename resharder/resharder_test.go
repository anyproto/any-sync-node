package resharder

import (
	"context"
	"fmt"
	"testing"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/archive"
	"github.com/anyproto/any-sync-node/archive/archivestore"
	"github.com/anyproto/any-sync-node/archive/archivestore/mock_archivestore"
	"github.com/anyproto/any-sync-node/archive/mock_archive"
	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodehead/mock_nodehead"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodestorage/mock_nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/hotsync"
	"github.com/anyproto/any-sync-node/nodesync/hotsync/mock_hotsync"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

var ctx = context.Background()

const spaceId = "space.id"

func okEntry() nodestorage.SpaceStatusEntry {
	return nodestorage.SpaceStatusEntry{
		SpaceId: spaceId,
		Status:  nodestorage.SpaceStatusOk,
		OldHash: "oldHash",
		NewHash: "newHash",
	}
}

func archivedEntry() nodestorage.SpaceStatusEntry {
	e := okEntry()
	e.Status = nodestorage.SpaceStatusArchived
	e.ArchiveSizeCompressed = 10
	e.ArchiveSizeUncompressed = 20
	return e
}

func respOk() *nodesyncproto.AdoptArchiveResponse {
	return &nodesyncproto.AdoptArchiveResponse{Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveOk}
}

func respSame() *nodesyncproto.AdoptArchiveResponse {
	return &nodesyncproto.AdoptArchiveResponse{Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveSame}
}

func respDiverged() *nodesyncproto.AdoptArchiveResponse {
	return &nodesyncproto.AdoptArchiveResponse{Result: nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveDiverged}
}

func TestResharder_DrainSpace(t *testing.T) {
	t.Run("archived space handed off with 2 acks", func(t *testing.T) {
		fx := newFixture(t)
		fx.nodeConf.EXPECT().IsResponsible(spaceId).Return(false)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(archivedEntry(), nil)
		fx.archiveStore.EXPECT().Exists(gomock.Any(), spaceId).Return(true, nil)
		fx.archiveStore.EXPECT().Key(spaceId).Return("me/" + spaceId)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2", "p3"})
		fx.adoptResponses["p1"] = respOk()
		fx.adoptResponses["p2"] = respSame()
		fx.adoptResponses["p3"] = respDiverged()
		// heads recheck before deletion
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(archivedEntry(), nil)
		// deletion: no local db -> heads deleted directly, object removed, moved
		fx.storage.EXPECT().SpaceExists(spaceId).Return(false)
		fx.nodeHead.EXPECT().DeleteHeads(spaceId).Return(nil)
		fx.archiveStore.EXPECT().Delete(gomock.Any(), spaceId).Return(nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusMoved, "").Return(nil)

		ok, err := fx.drainSpace(spaceId)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.ElementsMatch(t, []string{"p1", "p2", "p3"}, fx.adopted)
	})
	t.Run("live space force-archived and handed off", func(t *testing.T) {
		fx := newFixture(t)
		fx.nodeConf.EXPECT().IsResponsible(spaceId).Return(false)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(okEntry(), nil)
		fx.storage.EXPECT().SpaceExists(spaceId).Return(true)
		fx.archive.EXPECT().ForceArchive(gomock.Any(), spaceId).Return("oldHash", "newHash", int64(11), int64(22), nil)
		// post-snapshot check: index still matches the snapshot heads
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(okEntry(), nil)
		fx.archiveStore.EXPECT().Key(spaceId).Return("me/" + spaceId)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2"})
		fx.adoptResponses["p1"] = respOk()
		fx.adoptResponses["p2"] = respOk()
		// heads recheck
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(okEntry(), nil)
		// deletion: local db exists
		fx.storage.EXPECT().SpaceExists(spaceId).Return(true)
		fx.storage.EXPECT().DeleteSpaceStorage(gomock.Any(), spaceId).Return(nil)
		fx.archiveStore.EXPECT().Delete(gomock.Any(), spaceId).Return(nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusMoved, "").Return(nil)

		ok, err := fx.drainSpace(spaceId)
		require.NoError(t, err)
		assert.True(t, ok)
		// eager restore requested for live spaces
		assert.True(t, fx.lastReq.Eager)
	})
	t.Run("not enough acks parks and converges", func(t *testing.T) {
		fx := newFixture(t)
		fx.nodeConf.EXPECT().IsResponsible(spaceId).Return(false)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(archivedEntry(), nil)
		fx.archiveStore.EXPECT().Exists(gomock.Any(), spaceId).Return(true, nil)
		fx.archiveStore.EXPECT().Key(spaceId).Return("me/" + spaceId)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2", "p3"})
		fx.adoptResponses["p1"] = respOk()
		fx.adoptResponses["p2"] = respDiverged()
		fx.adoptResponses["p3"] = respDiverged()
		fx.hotSync.EXPECT().UpdateQueue([]string{spaceId})

		ok, err := fx.drainSpace(spaceId)
		require.NoError(t, err)
		assert.False(t, ok)
	})
	t.Run("space deleted by network drops local copy", func(t *testing.T) {
		fx := newFixture(t)
		fx.nodeConf.EXPECT().IsResponsible(spaceId).Return(false)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(archivedEntry(), nil)
		fx.archiveStore.EXPECT().Exists(gomock.Any(), spaceId).Return(true, nil)
		fx.archiveStore.EXPECT().Key(spaceId).Return("me/" + spaceId)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2", "p3"})
		fx.adoptErrors["p1"] = nodesyncproto.ErrSpaceDeleted
		fx.storage.EXPECT().SpaceExists(spaceId).Return(false)
		fx.nodeHead.EXPECT().DeleteHeads(spaceId).Return(nil)
		fx.archiveStore.EXPECT().Delete(gomock.Any(), spaceId).Return(nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusMoved, "").Return(nil)

		ok, err := fx.drainSpace(spaceId)
		require.NoError(t, err)
		assert.True(t, ok)
	})
	t.Run("heads drift during handoff postpones deletion", func(t *testing.T) {
		fx := newFixture(t)
		fx.nodeConf.EXPECT().IsResponsible(spaceId).Return(false)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(archivedEntry(), nil)
		fx.archiveStore.EXPECT().Exists(gomock.Any(), spaceId).Return(true, nil)
		fx.archiveStore.EXPECT().Key(spaceId).Return("me/" + spaceId)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2"})
		fx.adoptResponses["p1"] = respOk()
		fx.adoptResponses["p2"] = respOk()
		drifted := archivedEntry()
		drifted.NewHash = "differentHash"
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(drifted, nil)

		ok, err := fx.drainSpace(spaceId)
		require.NoError(t, err)
		assert.False(t, ok)
	})
	t.Run("responsible again: skip", func(t *testing.T) {
		fx := newFixture(t)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(archivedEntry(), nil)
		fx.nodeConf.EXPECT().IsResponsible(spaceId).Return(true)
		ok, err := fx.drainSpace(spaceId)
		require.NoError(t, err)
		assert.True(t, ok)
	})
	t.Run("statuses out of scope are skipped", func(t *testing.T) {
		fx := newFixture(t)
		e := okEntry()
		e.Status = nodestorage.SpaceStatusRemovePrepare
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(e, nil)
		ok, err := fx.drainSpace(spaceId)
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestResharder_DrainCycle(t *testing.T) {
	fx := newFixture(t)
	// no unindexed dirs
	fx.storage.EXPECT().AllSpaceIds().Return(nil, nil)
	// two spaces in the index: one responsible, one not
	fx.indexStorage.EXPECT().ReadHashes(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, iter func(nodestorage.SpaceUpdate) (bool, error)) error {
			_, _ = iter(nodestorage.SpaceUpdate{SpaceId: "keep.id"})
			_, _ = iter(nodestorage.SpaceUpdate{SpaceId: spaceId})
			return nil
		})
	fx.nodeConf.EXPECT().IsResponsible("keep.id").Return(true)
	fx.nodeConf.EXPECT().IsResponsible(spaceId).Return(false)
	// drainSpace path: entry lookup fails with not found -> parked with error
	fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{}, anystore.ErrDocNotFound)

	fx.drainCycle()
	assert.Equal(t, uint32(1), fx.stat.draining.Load())
}

type fixture struct {
	*resharder
	a              *app.App
	storage        *mock_nodestorage.MockNodeStorage
	indexStorage   *mock_nodestorage.MockIndexStorage
	archiveStore   *mock_archivestore.MockArchiveStore
	archive        *mock_archive.MockArchive
	nodeHead       *mock_nodehead.MockNodeHead
	nodeConf       *mock_nodeconf.MockService
	hotSync        *mock_hotsync.MockHotSync
	observer       nodeconf.ChangeObserver
	adopted        []string
	lastReq        *nodesyncproto.AdoptArchiveRequest
	adoptResponses map[string]*nodesyncproto.AdoptArchiveResponse
	adoptErrors    map[string]error
}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		resharder:      New().(*resharder),
		a:              new(app.App),
		storage:        mock_nodestorage.NewMockNodeStorage(ctrl),
		indexStorage:   mock_nodestorage.NewMockIndexStorage(ctrl),
		archiveStore:   mock_archivestore.NewMockArchiveStore(ctrl),
		archive:        mock_archive.NewMockArchive(ctrl),
		nodeHead:       mock_nodehead.NewMockNodeHead(ctrl),
		nodeConf:       mock_nodeconf.NewMockService(ctrl),
		hotSync:        mock_hotsync.NewMockHotSync(ctrl),
		adoptResponses: map[string]*nodesyncproto.AdoptArchiveResponse{},
		adoptErrors:    map[string]error{},
	}
	fx.resharder.adoptFn = func(_ context.Context, peerId string, req *nodesyncproto.AdoptArchiveRequest) (*nodesyncproto.AdoptArchiveResponse, error) {
		fx.adopted = append(fx.adopted, peerId)
		fx.lastReq = req
		if err, ok := fx.adoptErrors[peerId]; ok {
			return nil, err
		}
		if resp, ok := fx.adoptResponses[peerId]; ok {
			return resp, nil
		}
		return nil, context.DeadlineExceeded
	}

	anymock.ExpectComp(fx.storage.EXPECT(), nodestorage.CName)
	anymock.ExpectComp(fx.archiveStore.EXPECT(), archivestore.CName)
	anymock.ExpectComp(fx.archive.EXPECT(), archive.CName)
	anymock.ExpectComp(fx.nodeHead.EXPECT(), nodehead.CName)
	anymock.ExpectComp(fx.nodeConf.EXPECT(), nodeconf.CName)
	anymock.ExpectComp(fx.hotSync.EXPECT(), hotsync.CName)
	fx.hotSync.EXPECT().SetMetric(gomock.Any(), gomock.Any()).AnyTimes()
	fx.storage.EXPECT().IndexStorage().AnyTimes().Return(fx.indexStorage)
	fx.nodeConf.EXPECT().ObserveChanges(gomock.Any()).Do(func(observer nodeconf.ChangeObserver) {
		fx.observer = observer
	})
	fx.nodeConf.EXPECT().Configuration().AnyTimes().Return(nodeconf.Configuration{Epoch: 1})
	// Run gates on Shared; keep the background loop off in unit tests
	fx.archiveStore.EXPECT().Shared().Return(false)

	tp := rpctest.NewTestPool()
	var _ pool.Pool = tp

	fx.a.Register(fx.storage).
		Register(fx.archiveStore).
		Register(fx.archive).
		Register(fx.nodeHead).
		Register(fx.nodeConf).
		Register(fx.hotSync).
		Register(tp).
		Register(fx.resharder)

	require.NoError(t, fx.a.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, fx.a.Close(ctx))
		ctrl.Finish()
	})
	return fx
}

func confWithTreeNodes(epoch uint64, addrSuffix string, peerIds ...string) nodeconf.Configuration {
	c := nodeconf.Configuration{Id: fmt.Sprintf("cfg-%d", epoch), NetworkId: "net", Epoch: epoch}
	for _, id := range peerIds {
		c.Nodes = append(c.Nodes, nodeconf.Node{
			PeerId:    id,
			Addresses: []string{id + addrSuffix},
			Types:     []nodeconf.NodeType{nodeconf.NodeTypeTree},
		})
	}
	return c
}

func TestResharder_ObserverSkipsUnchangedTreeSet(t *testing.T) {
	fx := newFixture(t)
	require.NotNil(t, fx.observer)

	prev := newRingConf("p1")
	prev.set(confWithTreeNodes(1, ":1001", "p1", "p2", "p3"), nil)

	t.Run("addresses changed, same tree members: no drain scheduled", func(t *testing.T) {
		cur := newRingConf("p1")
		cur.set(confWithTreeNodes(2, ":2002", "p1", "p2", "p3"), nil)
		fx.observer(prev, cur)
		select {
		case <-fx.resharder.trigger:
			t.Fatal("drain cycle must not be scheduled for an address-only change")
		default:
		}
	})
	t.Run("tree member added: drain scheduled", func(t *testing.T) {
		cur := newRingConf("p1")
		cur.set(confWithTreeNodes(3, ":1001", "p1", "p2", "p3", "p4"), nil)
		fx.observer(prev, cur)
		select {
		case <-fx.resharder.trigger:
		default:
			t.Fatal("drain cycle must be scheduled when the tree set changes")
		}
	})
	t.Run("tree member replaced: drain scheduled", func(t *testing.T) {
		cur := newRingConf("p1")
		cur.set(confWithTreeNodes(4, ":1001", "p1", "p2", "p9"), nil)
		fx.observer(prev, cur)
		select {
		case <-fx.resharder.trigger:
		default:
			t.Fatal("drain cycle must be scheduled when a tree member is replaced")
		}
	})
}
