package adopter

import (
	"context"
	"testing"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/peer"
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
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

var ctx = peer.CtxWithPeerId(context.Background(), "peer1")

func adoptReq() *nodesyncproto.AdoptArchiveRequest {
	return &nodesyncproto.AdoptArchiveRequest{
		SpaceId:          "space.id",
		SrcKey:           "othernode/space.id",
		OldHash:          "oldHash",
		NewHash:          "newHash",
		CompressedSize:   10,
		UncompressedSize: 20,
	}
}

func TestAdopter_AdoptArchive(t *testing.T) {
	t.Run("not shared", func(t *testing.T) {
		fx := newFixture(t)
		fx.archiveStore.EXPECT().Shared().Return(false)
		_, err := fx.AdoptArchive(ctx, adoptReq())
		assert.ErrorIs(t, err, nodesyncproto.ErrArchiveUnavailable)
	})
	t.Run("client peer rejected", func(t *testing.T) {
		fx := newFixture(t)
		fx.archiveStore.EXPECT().Shared().Return(true)
		fx.nodeConf.EXPECT().NodeTypes("peer1").Return(nil)
		_, err := fx.AdoptArchive(ctx, adoptReq())
		assert.ErrorIs(t, err, nodesyncproto.ErrPeerIsNotNode)
	})
	t.Run("adopt ok", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{}, anystore.ErrDocNotFound)
		fx.storage.EXPECT().SpaceExists(req.SpaceId).Return(false)
		fx.archiveStore.EXPECT().CopyFrom(ctx, req.SrcKey, req.SpaceId).Return(nil)
		fx.archiveStore.EXPECT().Exists(ctx, req.SpaceId).Return(true, nil)
		fx.indexStorage.EXPECT().MarkArchivedRemote(ctx, req.SpaceId, "newHash", int64(10), int64(20)).Return(nil)
		fx.nodeHead.EXPECT().SetHead(req.SpaceId, "newHash").Return(0, nil)

		resp, err := fx.AdoptArchive(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, nodesyncproto.AdoptArchiveResult_AdoptArchiveOk, resp.Result)
	})
	t.Run("adopt eager queues restore", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		req.Eager = true
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{}, anystore.ErrDocNotFound)
		fx.storage.EXPECT().SpaceExists(req.SpaceId).Return(false)
		fx.archiveStore.EXPECT().CopyFrom(ctx, req.SrcKey, req.SpaceId).Return(nil)
		fx.archiveStore.EXPECT().Exists(ctx, req.SpaceId).Return(true, nil)
		fx.indexStorage.EXPECT().MarkArchivedRemote(ctx, req.SpaceId, "newHash", int64(10), int64(20)).Return(nil)
		fx.nodeHead.EXPECT().SetHead(req.SpaceId, "newHash").Return(0, nil)
		fx.archive.EXPECT().QueueRestore(req.SpaceId)

		resp, err := fx.AdoptArchive(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, nodesyncproto.AdoptArchiveResult_AdoptArchiveOk, resp.Result)
	})
	t.Run("already have live", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusOk, NewHash: "newHash"}, nil)
		fx.storage.EXPECT().SpaceExists(req.SpaceId).Return(true)

		resp, err := fx.AdoptArchive(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveSame, resp.Result)
	})
	t.Run("already have archived with object", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusArchived, NewHash: "newHash"}, nil)
		fx.archiveStore.EXPECT().Exists(ctx, req.SpaceId).Return(true, nil)

		resp, err := fx.AdoptArchive(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveSame, resp.Result)
	})
	t.Run("archived but object missing: re-adopt", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusArchived}, nil)
		fx.archiveStore.EXPECT().Exists(ctx, req.SpaceId).Return(false, nil)
		fx.archiveStore.EXPECT().CopyFrom(ctx, req.SrcKey, req.SpaceId).Return(nil)
		fx.archiveStore.EXPECT().Exists(ctx, req.SpaceId).Return(true, nil)
		fx.indexStorage.EXPECT().MarkArchivedRemote(ctx, req.SpaceId, "newHash", int64(10), int64(20)).Return(nil)
		fx.nodeHead.EXPECT().SetHead(req.SpaceId, "newHash").Return(0, nil)

		resp, err := fx.AdoptArchive(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, nodesyncproto.AdoptArchiveResult_AdoptArchiveOk, resp.Result)
	})
	t.Run("deleted space rejected", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusRemove}, nil)
		_, err := fx.AdoptArchive(ctx, req)
		assert.ErrorIs(t, err, nodesyncproto.ErrSpaceDeleted)
	})
	t.Run("pending deletion parks the sender", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusRemovePrepare}, nil)
		_, err := fx.AdoptArchive(ctx, req)
		assert.ErrorIs(t, err, nodesyncproto.ErrSpacePendingDeletion)
	})
	t.Run("error status: never adopted, never acked", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError}, nil)
		resp, err := fx.AdoptArchive(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, nodesyncproto.AdoptArchiveResult_AdoptArchiveAlreadyHaveDiverged, resp.Result)
	})
	t.Run("not responsible rejected", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.archiveStore.EXPECT().Shared().Return(true)
		fx.nodeConf.EXPECT().NodeTypes("peer1").Return([]nodeconf.NodeType{nodeconf.NodeTypeTree})
		fx.nodeConf.EXPECT().IsResponsible(req.SpaceId).Return(false)
		_, err := fx.AdoptArchive(ctx, req)
		assert.ErrorIs(t, err, nodesyncproto.ErrNotResponsible)
	})
	t.Run("source object missing", func(t *testing.T) {
		fx := newFixture(t)
		req := adoptReq()
		fx.expectNodePeer()
		fx.indexStorage.EXPECT().SpaceStatusEntry(ctx, req.SpaceId).Return(nodestorage.SpaceStatusEntry{}, anystore.ErrDocNotFound)
		fx.storage.EXPECT().SpaceExists(req.SpaceId).Return(false)
		fx.archiveStore.EXPECT().CopyFrom(ctx, req.SrcKey, req.SpaceId).Return(archivestore.ErrNotFound)
		_, err := fx.AdoptArchive(ctx, req)
		assert.ErrorIs(t, err, nodesyncproto.ErrArchiveObjectMissing)
	})
}

type fixture struct {
	Adopter
	a            *app.App
	storage      *mock_nodestorage.MockNodeStorage
	indexStorage *mock_nodestorage.MockIndexStorage
	archiveStore *mock_archivestore.MockArchiveStore
	archive      *mock_archive.MockArchive
	nodeHead     *mock_nodehead.MockNodeHead
	nodeConf     *mock_nodeconf.MockService
}

func (fx *fixture) expectNodePeer() {
	fx.archiveStore.EXPECT().Shared().Return(true)
	fx.nodeConf.EXPECT().NodeTypes("peer1").Return([]nodeconf.NodeType{nodeconf.NodeTypeTree})
	fx.nodeConf.EXPECT().IsResponsible("space.id").Return(true)
}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		a:            new(app.App),
		storage:      mock_nodestorage.NewMockNodeStorage(ctrl),
		indexStorage: mock_nodestorage.NewMockIndexStorage(ctrl),
		archiveStore: mock_archivestore.NewMockArchiveStore(ctrl),
		archive:      mock_archive.NewMockArchive(ctrl),
		nodeHead:     mock_nodehead.NewMockNodeHead(ctrl),
		nodeConf:     mock_nodeconf.NewMockService(ctrl),
		Adopter:      New(),
	}
	anymock.ExpectComp(fx.storage.EXPECT(), nodestorage.CName)
	anymock.ExpectComp(fx.archiveStore.EXPECT(), archivestore.CName)
	anymock.ExpectComp(fx.archive.EXPECT(), archive.CName)
	anymock.ExpectComp(fx.nodeHead.EXPECT(), nodehead.CName)
	anymock.ExpectComp(fx.nodeConf.EXPECT(), nodeconf.CName)
	fx.storage.EXPECT().IndexStorage().AnyTimes().Return(fx.indexStorage)

	fx.a.Register(fx.storage).
		Register(fx.archiveStore).
		Register(fx.archive).
		Register(fx.nodeHead).
		Register(fx.nodeConf).
		Register(fx.Adopter)

	require.NoError(t, fx.a.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, fx.a.Close(context.Background()))
		ctrl.Finish()
	})
	return fx
}
