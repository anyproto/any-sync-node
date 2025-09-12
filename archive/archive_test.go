package archive

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/archive/archivestore"
	"github.com/anyproto/any-sync-node/archive/archivestore/mock_archivestore"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodestorage/mock_nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
	"github.com/anyproto/any-sync-node/nodesync/mock_nodesync"
)

var ctx = context.Background()

func TestArchive_Archive(t *testing.T) {
	fx := newFixture(t)

	var spaceId = "space.id"

	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	spaceDir := filepath.Join(tmpDir, "spaceid")
	require.NoError(t, os.Mkdir(spaceDir, 0755))

	fx.storage.EXPECT().StoreDir(spaceId).Return(spaceDir).AnyTimes()

	db, err := anystore.Open(ctx, filepath.Join(spaceDir, "test.db"), nil)
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	fx.storage.EXPECT().
		TryLockAndOpenDb(ctx, spaceId, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, openFunc nodestorage.DoAfterOpenFunc) error {
			return openFunc(db)
		})

	fx.archiveStore.EXPECT().Put(ctx, spaceId, gomock.Any()).DoAndReturn(func(ctx context.Context, spaceId string, rd io.ReadSeeker) error {
		bytes, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "archive.gz"), bytes, 0644))
		return nil
	})

	fx.indexStorage.EXPECT().MarkArchived(ctx, spaceId, gomock.Not(0), gomock.Not(0))

	require.NoError(t, fx.Archive.(*archive).Archive(ctx, spaceId))

	_, err = os.Stat(spaceDir)
	require.True(t, os.IsNotExist(err))

	fx.archiveStore.EXPECT().Get(ctx, spaceId).DoAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
		return os.Open(filepath.Join(tmpDir, "archive.gz"))
	})

	fx.indexStorage.EXPECT().SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusOk, "")
	fx.archiveStore.EXPECT().Delete(ctx, spaceId)

	require.NoError(t, fx.Restore(ctx, spaceId))

	db, err = anystore.Open(ctx, filepath.Join(spaceDir, "store.db"), nil)
	require.NoError(t, err)
	coll, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"test"}, coll)
}

type fixture struct {
	Archive
	a            *app.App
	archiveStore *mock_archivestore.MockArchiveStore
	nodeSync     *mock_nodesync.MockNodeSync
	storage      *mock_nodestorage.MockNodeStorage
	indexStorage *mock_nodestorage.MockIndexStorage
}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		a:            new(app.App),
		archiveStore: mock_archivestore.NewMockArchiveStore(ctrl),
		nodeSync:     mock_nodesync.NewMockNodeSync(ctrl),
		storage:      mock_nodestorage.NewMockNodeStorage(ctrl),
		indexStorage: mock_nodestorage.NewMockIndexStorage(ctrl),
		Archive:      New(),
	}

	anymock.ExpectComp(fx.archiveStore.EXPECT(), archivestore.CName)
	anymock.ExpectComp(fx.nodeSync.EXPECT(), nodesync.CName)
	anymock.ExpectComp(fx.storage.EXPECT(), nodestorage.CName)
	var ch = make(chan struct{})
	close(ch)
	fx.nodeSync.EXPECT().WaitSyncOnStart().AnyTimes().Return(ch)
	fx.storage.EXPECT().IndexStorage().AnyTimes().Return(fx.indexStorage)
	fx.a.Register(fx.archiveStore).
		Register(fx.nodeSync).
		Register(fx.storage).
		Register(&testConfig{}).
		Register(fx.Archive)

	require.NoError(t, fx.a.Start(ctx))

	t.Cleanup(func() {
		require.NoError(t, fx.a.Close(ctx))
		ctrl.Finish()
	})

	return fx
}

type testConfig struct {
}

func (t testConfig) Init(_ *app.App) error {
	return nil
}

func (t testConfig) Name() string {
	return "config"
}

func (t testConfig) GetArchive() Config {
	return Config{}
}
