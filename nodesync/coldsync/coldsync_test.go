package coldsync

import (
	"bytes"
	"context"
	"errors"
	"github.com/anytypeio/any-sync-node/nodespace"
	"github.com/anytypeio/any-sync-node/nodespace/mock_nodespace"
	"github.com/anytypeio/any-sync-node/nodestorage"
	"github.com/anytypeio/any-sync-node/nodestorage/mock_nodestorage"
	"github.com/anytypeio/any-sync-node/nodesync/nodesyncproto"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/net/rpc/rpctest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var ctx = context.Background()

func TestColdSync_Sync(t *testing.T) {
	var makeClientServer = func(t *testing.T) (fxC, fxS *fixture, peerId string) {
		fxC = newFixture(t)
		fxS = newFixture(t)
		peerId = "peer"
		p, err := fxS.tp.Get(ctx, "peer")
		require.NoError(t, err)
		fxC.tp.AddPeer(p)
		return
	}

	t.Run("sync", func(t *testing.T) {
		var spaceId = "spaceId"
		fxC, fxS, peerId := makeClientServer(t)
		defer fxC.Finish(t)
		defer fxS.Finish(t)
		writeFiles(t, fxS.store.StoreDir(spaceId), testFiles...)
		fxC.store.EXPECT().
			TryLockAndDo(gomock.Any(), gomock.Any()).AnyTimes().
			DoAndReturn(func(spaceId string, do func() error) (err error) {
				return do()
			})
		fxS.store.EXPECT().
			TryLockAndDo(gomock.Any(), gomock.Any()).AnyTimes().
			DoAndReturn(func(spaceId string, do func() error) (err error) {
				return do()
			})
		fxC.store.EXPECT().SpaceExists(spaceId).Return(false)

		require.NoError(t, fxC.Sync(ctx, "spaceId", peerId))

		for _, tf := range testFiles {
			cBytes, err := os.ReadFile(filepath.Join(fxC.store.StoreDir(spaceId), tf.name))
			require.NoError(t, err)
			sBytes, err := os.ReadFile(filepath.Join(fxS.store.StoreDir(spaceId), tf.name))
			require.NoError(t, err)
			assert.True(t, bytes.Equal(cBytes, sBytes))
		}
	})
	t.Run("remote space in cache", func(t *testing.T) {
		var spaceId = "spaceId"
		fxC, fxS, peerId := makeClientServer(t)
		defer fxC.Finish(t)
		defer fxS.Finish(t)
		fxC.store.EXPECT().
			TryLockAndDo(gomock.Any(), gomock.Any()).AnyTimes().
			DoAndReturn(func(spaceId string, do func() error) (err error) {
				return do()
			})
		fxC.store.EXPECT().SpaceExists(spaceId).Return(false)
		fxS.store.EXPECT().TryLockAndDo(gomock.Any(), gomock.Any()).Return(nodestorage.ErrLocked)
		fxS.space.EXPECT().GetSpace(gomock.Any(), spaceId).Return(nil, nil)
		err := fxC.Sync(ctx, "spaceId", peerId)
		assert.Equal(t, ErrRemoteSpaceLocked, err)
	})
	t.Run("remote error", func(t *testing.T) {
		var spaceId = "spaceId"
		fxC, fxS, peerId := makeClientServer(t)
		defer fxC.Finish(t)
		defer fxS.Finish(t)
		fxC.store.EXPECT().
			TryLockAndDo(gomock.Any(), gomock.Any()).AnyTimes().
			DoAndReturn(func(spaceId string, do func() error) (err error) {
				return do()
			})
		fxC.store.EXPECT().SpaceExists(spaceId).Return(false)
		testErr := errors.New("test remote error")
		fxS.store.EXPECT().TryLockAndDo(gomock.Any(), gomock.Any()).Return(testErr)
		err := fxC.Sync(ctx, "spaceId", peerId)
		assert.EqualError(t, err, testErr.Error())
	})
}

type fInfo struct {
	name string
	size int
}

func writeFiles(t *testing.T, dir string, files ...fInfo) {
	for _, fi := range files {
		fpath := filepath.Join(dir, fi.name)
		_ = os.MkdirAll(filepath.Dir(fpath), 0755)
		f, err := os.Create(fpath)
		require.NoError(t, err)
		defer f.Close()
		written, err := io.CopyN(f, rand.New(rand.NewSource(time.Now().UnixNano())), int64(fi.size))
		require.NoError(t, err)
		assert.Equal(t, int64(fi.size), written)
	}
}

func newFixture(t *testing.T) (fx *fixture) {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	ts := rpctest.NewTestServer()
	fx = &fixture{
		ColdSync: New(),
		ctrl:     gomock.NewController(t),
		a:        new(app.App),
		tp:       rpctest.NewTestPool().WithServer(ts),
		tmpDir:   tmpDir,
	}
	fx.store = mock_nodestorage.NewMockNodeStorage(fx.ctrl)
	fx.store.EXPECT().Name().Return(nodestorage.CName).AnyTimes()
	fx.store.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.store.EXPECT().StoreDir(gomock.Any()).AnyTimes().
		DoAndReturn(func(spaceId string) string {
			return filepath.Join(fx.tmpDir, spaceId)
		})
	fx.space = mock_nodespace.NewMockService(fx.ctrl)
	fx.space.EXPECT().Name().Return(nodespace.CName).AnyTimes()
	fx.space.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.space.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.space.EXPECT().Close(gomock.Any()).AnyTimes()

	fx.a.Register(fx.ColdSync).Register(fx.tp).Register(fx.store).Register(fx.space)
	require.NoError(t, nodesyncproto.DRPCRegisterNodeSync(ts, &testServer{cs: fx.ColdSync}))
	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	ColdSync
	tp     *rpctest.TestPool
	a      *app.App
	store  *mock_nodestorage.MockNodeStorage
	ctrl   *gomock.Controller
	tmpDir string
	space  *mock_nodespace.MockService
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

var testFiles = []fInfo{
	{
		name: "small.t",
		size: 3,
	},
	{
		name: "dir1/chunkSize.t",
		size: chunkSize,
	},
	{
		name: "dir2/2chunkSize.t",
		size: chunkSize * 2,
	},
	{
		name: "dir1/big.t",
		size: (chunkSize * 10) - 5,
	},
}
