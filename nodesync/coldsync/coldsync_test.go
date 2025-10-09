package coldsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/archive/mock_archive"
	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodespace/mock_nodespace"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

var ctx = context.Background()

func TestColdSync_Sync(t *testing.T) {
	var makeClientServer = func(t *testing.T) (fxC, fxS *fixture, peerId string) {
		fxC = newFixture(t)
		fxS = newFixture(t)
		peerId = "peer"
		mcS, mcC := rpctest.MultiConnPair(peerId, peerId+"client")
		pS, err := peer.NewPeer(mcS, fxC.ts)
		require.NoError(t, err)
		fxC.tp.AddPeer(ctx, pS)
		_, err = peer.NewPeer(mcC, fxS.ts)
		require.NoError(t, err)
		return
	}
	t.Run("sync", func(t *testing.T) {
		// whether the space in cache or not doesn't matter
		// because we do backup
		fxC, fxS, peerId := makeClientServer(t)
		defer fxC.Finish(t)
		defer fxS.Finish(t)
		store := nodestorage.GenStorage(t, fxS.store, 100, 100)
		require.NoError(t, fxC.Sync(ctx, store.Id(), peerId))
		store, err := fxC.store.SpaceStorage(ctx, store.Id())
		require.NoError(t, err)
		cnt := 0
		err = store.HeadStorage().IterateEntries(ctx, headstorage.IterOpts{}, func(entry headstorage.HeadsEntry) (bool, error) {
			cnt++
			return true, nil
		})
		require.NoError(t, err)
		// 100 trees + acl + settings
		require.Equal(t, 102, cnt)
	})
	t.Run("space missing", func(t *testing.T) {
		fxC, fxS, peerId := makeClientServer(t)
		defer fxC.Finish(t)
		defer fxS.Finish(t)
		err := fxC.Sync(ctx, "id", peerId)
		require.ErrorIs(t, rpcerr.Unwrap(err), spacesyncproto.ErrSpaceMissing)
	})
	t.Run("unsupported storage request", func(t *testing.T) {
		fxC, fxS, peerId := makeClientServer(t)
		defer fxC.Finish(t)
		defer fxS.Finish(t)
		store := nodestorage.GenStorage(t, fxS.store, 100, 100)
		currentReqProtocol = nodesyncproto.ColdSyncProtocolType_Pogreb
		err := fxC.Sync(ctx, store.Id(), peerId)
		require.ErrorIs(t, rpcerr.Unwrap(err), nodesyncproto.ErrUnsupportedStorageType)
	})
	t.Run("unsupported storage response", func(t *testing.T) {
		fxC, fxS, peerId := makeClientServer(t)
		defer fxC.Finish(t)
		defer fxS.Finish(t)
		store := nodestorage.GenStorage(t, fxS.store, 100, 100)
		currentRespProtocol = nodesyncproto.ColdSyncProtocolType_Pogreb
		err := fxC.Sync(ctx, store.Id(), peerId)
		require.ErrorIs(t, nodesyncproto.ErrUnsupportedStorageType, rpcerr.Unwrap(err))
	})
}

func newFixture(t *testing.T) (fx *fixture) {
	ts := rpctest.NewTestServer()
	fx = &fixture{
		ColdSync: New(),
		ctrl:     gomock.NewController(t),
		a:        new(app.App),
		ts:       ts,
		tp:       rpctest.NewTestPool(),
		tmpDir:   t.TempDir(),
	}
	tempDir := fx.tmpDir
	fx.store = nodestorage.New()
	fx.space = mock_nodespace.NewMockService(fx.ctrl)
	configGetter := mockConfigGetter{tempStoreNew: filepath.Join(tempDir, "new"), tempStoreOld: filepath.Join(tempDir, "old")}
	archive := mock_archive.NewMockArchive(fx.ctrl)
	anymock.ExpectComp(archive.EXPECT(), "node.archive")
	anymock.ExpectComp(fx.space.EXPECT(), nodespace.CName)
	fx.a.Register(configGetter).
		Register(fx.store).
		Register(fx.ColdSync).
		Register(fx.tp).
		Register(fx.ts).
		Register(archive).
		Register(fx.space)
	require.NoError(t, nodesyncproto.DRPCRegisterNodeSync(ts, &testServer{cs: fx.ColdSync}))
	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	ColdSync
	a      *app.App
	store  nodestorage.NodeStorage
	ctrl   *gomock.Controller
	tmpDir string
	space  *mock_nodespace.MockService
	ts     *rpctest.TestServer
	tp     *rpctest.TestPool
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
	if fx.tmpDir != "" {
		_ = os.RemoveAll(fx.tmpDir)
	}
}

type testServer struct {
	nodesyncproto.DRPCNodeSyncUnimplementedServer
	cs ColdSync
}

func (t *testServer) ColdSync(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	return t.cs.ColdSyncHandle(req, stream)
}

type mockConfigGetter struct {
	tempStoreNew string
	tempStoreOld string
}

func (m mockConfigGetter) Init(a *app.App) (err error) {
	return nil
}

func (m mockConfigGetter) Name() (name string) {
	return "config"
}

func (m mockConfigGetter) GetStorage() nodestorage.Config {
	return nodestorage.Config{
		Path:         m.tempStoreOld,
		AnyStorePath: m.tempStoreNew,
	}
}
