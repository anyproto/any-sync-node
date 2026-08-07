package repairer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodespace/mock_nodespace"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/coldsync"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

// TestIntegration_RepairFlows drives the repairer end to end with two real
// node stacks and the real coldsync protocol over an in-process drpc pair:
// node B holds a real (GenStorage-created) space; node A repairs its broken
// local state by pulling from B.
func TestIntegration_RepairFlows(t *testing.T) {
	t.Run("corrupted db: quarantined and replaced from neighbor", func(t *testing.T) {
		fx := newIntFixture(t)
		spaceId := fx.genSpaceOnB(t)
		// node A: an index entry with heads and a corrupted db on disk
		fx.corruptSpaceOnA(t, spaceId)

		require.NoError(t, fx.repairerA.repairSpace(spaceId))

		// the corrupted db is preserved in quarantine
		quarantined, err := os.ReadDir(filepath.Join(fx.dirA, ".quarantine"))
		require.NoError(t, err)
		require.Len(t, quarantined, 1)
		assert.Contains(t, quarantined[0].Name(), spaceId)
		fx.assertRepairedOnA(t, spaceId)
	})
	t.Run("missing db file: pulled from neighbor", func(t *testing.T) {
		fx := newIntFixture(t)
		spaceId := fx.genSpaceOnB(t)
		// node A: an index entry with heads, but no data on disk at all
		require.NoError(t, fx.storageA.IndexStorage().UpdateHash(ctx, nodestorage.SpaceUpdate{
			SpaceId: spaceId, NewHash: "lost",
		}))
		require.NoError(t, fx.storageA.IndexStorage().MarkError(ctx, spaceId, "db file is missing"))

		require.NoError(t, fx.repairerA.repairSpace(spaceId))
		fx.assertRepairedOnA(t, spaceId)
	})
	t.Run("space missing everywhere: entry garbage-collected over the wire", func(t *testing.T) {
		fx := newIntFixture(t)
		const spaceId = "never.pushed"
		// an entry without heads: leftover of an uncommitted push
		require.NoError(t, fx.storageA.IndexStorage().SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusError, ""))

		require.NoError(t, fx.repairerA.repairSpace(spaceId))

		// the entry is gone entirely: the id is usable again
		_, err := fx.storageA.IndexStorage().SpaceStatusEntry(ctx, spaceId)
		assert.ErrorIs(t, err, anystore.ErrDocNotFound)
		assert.Equal(t, uint32(1), fx.repairerA.stat.droppedEntries.Load())
	})
}

type intFixture struct {
	appA, appB *app.App
	dirA       string
	storageA   nodestorage.NodeStorage
	storageB   nodestorage.NodeStorage
	repairerA  *repairer
}

func (fx *intFixture) genSpaceOnB(t *testing.T) (spaceId string) {
	store := nodestorage.GenStorage(t, fx.storageB, 3, 64)
	return store.Id()
}

func (fx *intFixture) corruptSpaceOnA(t *testing.T, spaceId string) {
	spaceDir := fx.storageA.StoreDir(spaceId)
	require.NoError(t, os.MkdirAll(spaceDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(spaceDir, "store.db"), []byte("this is not a sqlite database"), 0o644))
	require.NoError(t, fx.storageA.IndexStorage().UpdateHash(ctx, nodestorage.SpaceUpdate{
		SpaceId: spaceId, NewHash: "corrupt",
	}))
	require.NoError(t, fx.storageA.IndexStorage().MarkError(ctx, spaceId, "malformed database"))
}

func (fx *intFixture) assertRepairedOnA(t *testing.T, spaceId string) {
	// status cleared, heads registered from the pulled copy
	entry, err := fx.storageA.IndexStorage().SpaceStatusEntry(ctx, spaceId)
	require.NoError(t, err)
	assert.Equal(t, nodestorage.SpaceStatusOk, entry.Status)
	assert.NotEqual(t, "corrupt", entry.NewHash)
	assert.NotEqual(t, "lost", entry.NewHash)
	// the pulled db is a valid space storage
	ss, err := fx.storageA.SpaceStorage(ctx, spaceId)
	require.NoError(t, err)
	state, err := ss.StateStorage().GetState(ctx)
	require.NoError(t, err)
	assert.Equal(t, spaceId, state.SpaceId)
	require.NoError(t, ss.Close(ctx))
}

// coldSyncServer exposes only the ColdSync RPC of the NodeSync service.
type coldSyncServer struct {
	nodesyncproto.DRPCNodeSyncUnimplementedServer
	coldSync coldsync.ColdSync
}

func (s *coldSyncServer) ColdSync(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	return s.coldSync.ColdSyncHandle(req, stream)
}

func newIntFixture(t *testing.T) *intFixture {
	ctrl := gomock.NewController(t)
	fx := &intFixture{
		appA: new(app.App),
		appB: new(app.App),
		dirA: t.TempDir(),
	}

	newNode := func(a *app.App, dir string) (nodestorage.NodeStorage, coldsync.ColdSync, *rpctest.TestPool, server.DRPCServer) {
		storage := nodestorage.New()
		cs := coldsync.New()
		tp := rpctest.NewTestPool()
		ts := server.New()
		nodeSpace := mock_nodespace.NewMockService(ctrl)
		anymock.ExpectComp(nodeSpace.EXPECT(), nodespace.CName)
		a.Register(intConfig{dir: dir}).
			Register(&accounttest.AccountTestService{}).
			Register(&archiveStub{}).
			Register(storage).
			Register(nodeSpace).
			Register(tp).
			Register(ts).
			Register(cs)
		return storage, cs, tp, ts
	}

	var (
		csA, csB coldsync.ColdSync
		tpA      *rpctest.TestPool
		tsA, tsB server.DRPCServer
	)
	fx.storageA, csA, tpA, tsA = newNode(fx.appA, fx.dirA)
	fx.storageB, csB, _, tsB = newNode(fx.appB, t.TempDir())

	// node A's repairer with a nodeconf pointing at node B as the only owner
	nodeConf := mock_nodeconf.NewMockService(ctrl)
	anymock.ExpectComp(nodeConf.EXPECT(), nodeconf.CName)
	nodeConf.EXPECT().IsResponsible(gomock.Any()).AnyTimes().Return(true)
	nodeConf.EXPECT().NodeIds(gomock.Any()).AnyTimes().Return([]string{"nodeB"})
	fx.repairerA = &repairer{disableLoop: true}
	fx.appA.Register(nodeConf).Register(fx.repairerA)

	require.NoError(t, fx.appA.Start(ctx))
	require.NoError(t, fx.appB.Start(ctx))

	// serve only ColdSync on node B and wire a live in-process conn pair
	require.NoError(t, nodesyncproto.DRPCRegisterNodeSync(tsB, &coldSyncServer{coldSync: csB}))
	mcA, mcB := rpctest.MultiConnPair("nodeB", "nodeA")
	pB, err := peer.NewPeer(mcA, tsA)
	require.NoError(t, err)
	require.NoError(t, tpA.AddPeer(ctx, pB))
	_, err = peer.NewPeer(mcB, tsB)
	require.NoError(t, err)

	_ = csA
	t.Cleanup(func() {
		require.NoError(t, fx.appA.Close(ctx))
		require.NoError(t, fx.appB.Close(ctx))
		ctrl.Finish()
	})
	return fx
}

type intConfig struct {
	dir string
}

func (c intConfig) Init(_ *app.App) error { return nil }
func (c intConfig) Name() string          { return "config" }

func (c intConfig) GetStorage() nodestorage.Config {
	return nodestorage.Config{Path: c.dir, AnyStorePath: c.dir}
}

func (c intConfig) GetDrpc() rpc.Config {
	return rpc.Config{Stream: rpc.StreamConfig{MaxMsgSizeMb: 10}}
}

// archiveStub satisfies nodestorage's "node.archive" dependency.
type archiveStub struct{}

func (a *archiveStub) Init(_ *app.App) error { return nil }
func (a *archiveStub) Name() string          { return "node.archive" }

func (a *archiveStub) Restore(_ context.Context, _ string) error {
	return anystore.ErrDocNotFound
}
