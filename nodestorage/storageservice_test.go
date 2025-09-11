package nodestorage

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"

	"github.com/anyproto/any-sync-node/archive/mock_archive"
)

func TestStorageService_SpaceStorage(t *testing.T) {
	t.Run("create and get", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		payload := NewStorageCreatePayload(t)
		store, err := ss.CreateSpaceStorage(ctx, payload)
		require.NoError(t, err)
		nodeStore := store.(*nodeStorage)
		require.Equal(t, 1, nodeStore.cont.handlers)
		otherStore, err := ss.WaitSpaceStorage(ctx, payload.SpaceHeaderWithId.Id)
		require.NoError(t, err)
		require.Equal(t, 2, nodeStore.cont.handlers)
		require.NoError(t, otherStore.Close(ctx))
		require.Equal(t, 1, nodeStore.cont.handlers)
		require.NoError(t, otherStore.Close(ctx))
		require.Equal(t, 0, nodeStore.cont.handlers)
	})
	t.Run("fill index storage via set hash", func(t *testing.T) {
		dir := t.TempDir()
		ss := newStorageServiceWithDir(t, dir)
		defer ss.Close(ctx)
		total := 100
		for i := 0; i < total; i++ {
			payload := NewStorageCreatePayload(t)
			storage, err := ss.CreateSpaceStorage(ctx, payload)
			require.NoError(t, err)
			err = storage.StateStorage().SetHash(ctx, fmt.Sprint(i), fmt.Sprint(i))
			require.NoError(t, err)
		}
		ss.updater.Close()
		var allIds []string
		err := ss.IndexStorage().ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
			allIds = append(allIds, update.SpaceId)
			return true, nil
		})
		require.NoError(t, err)
		require.Len(t, allIds, total)
	})
	t.Run("fill index storage from start", func(t *testing.T) {
		dir := t.TempDir()
		ss := newStorageServiceWithDir(t, dir)
		total := 100
		for i := 0; i < total; i++ {
			payload := NewStorageCreatePayload(t)
			storage, err := ss.CreateSpaceStorage(ctx, payload)
			require.NoError(t, err)
			err = storage.StateStorage().SetHash(ctx, fmt.Sprint(i), fmt.Sprint(i))
			require.NoError(t, err)
		}
		err := ss.indexStorage.(*indexStorage).spaceColl.Drop(ctx)
		require.NoError(t, err)
		err = ss.Close(ctx)
		require.NoError(t, err)
		ss = newStorageServiceWithDir(t, dir)
		defer ss.Close(ctx)
		var allIds []string
		err = ss.IndexStorage().ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
			allIds = append(allIds, update.SpaceId)
			return true, nil
		})
		require.NoError(t, err)
		require.Len(t, allIds, total)
	})
	t.Run("add storages, drop hashes collection, remove storages, check hashes", func(t *testing.T) {
		dir := t.TempDir()
		ss := newStorageServiceWithDir(t, dir)
		total := 100
		for i := 0; i < total; i++ {
			payload := NewStorageCreatePayload(t)
			storage, err := ss.CreateSpaceStorage(ctx, payload)
			require.NoError(t, err)
			err = storage.StateStorage().SetHash(ctx, fmt.Sprint(i), fmt.Sprint(i))
			require.NoError(t, err)
		}
		err := ss.indexStorage.(*indexStorage).spaceColl.Drop(ctx)
		require.NoError(t, err)
		err = ss.Close(ctx)
		require.NoError(t, err)
		newDir := filepath.Join(dir, "new")
		entries, err := os.ReadDir(newDir)
		require.NoError(t, err)
		removed := 0
		i := 0
		for removed < total/2 {
			if strings.HasPrefix(entries[i].Name(), ".") {
				i++
				continue
			}
			removed++
			err = os.RemoveAll(filepath.Join(newDir, entries[i].Name()))
			require.NoError(t, err)
			i++
		}
		ss = newStorageServiceWithDir(t, dir)
		defer ss.Close(ctx)
		var allIds []string
		err = ss.IndexStorage().ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
			allIds = append(allIds, update.SpaceId)
			return true, nil
		})
		require.NoError(t, err)
		l, err := ss.AllSpaceIds()
		require.NoError(t, err)
		require.Len(t, allIds, total/2)
		require.Equal(t, len(allIds), len(l))
	})
	t.Run("add storages, add hashes, remove random hashes, check hashes", func(t *testing.T) {
		dir := t.TempDir()
		ss := newStorageServiceWithDir(t, dir)
		total := 100
		for i := 0; i < total; i++ {
			payload := NewStorageCreatePayload(t)
			storage, err := ss.CreateSpaceStorage(ctx, payload)
			require.NoError(t, err)
			err = storage.StateStorage().SetHash(ctx, fmt.Sprint(i), fmt.Sprint(i))
			require.NoError(t, err)
		}
		newDir := filepath.Join(dir, "new")
		entries, err := os.ReadDir(newDir)
		require.NoError(t, err)
		rand.Shuffle(len(entries), func(i, j int) {
			entries[i], entries[j] = entries[j], entries[i]
		})
		removed := 0
		i := 0
		for removed < total/2 {
			if strings.HasPrefix(entries[i].Name(), ".") {
				i++
				continue
			}
			removed++
			err = ss.indexStorage.(*indexStorage).spaceColl.DeleteId(ctx, entries[i].Name())
			require.NoError(t, err)
			i++
		}
		err = ss.Close(ctx)
		require.NoError(t, err)
		ss = newStorageServiceWithDir(t, dir)
		defer ss.Close(ctx)
		var allIds []string
		err = ss.IndexStorage().ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
			allIds = append(allIds, update.SpaceId)
			return true, nil
		})
		require.NoError(t, err)
		l, err := ss.AllSpaceIds()
		require.NoError(t, err)
		require.Len(t, allIds, total)
		require.Equal(t, len(allIds), len(l))
	})
	t.Run("create and all spaces", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		var allIds []string
		for i := 0; i < 10; i++ {
			payload := NewStorageCreatePayload(t)
			store, err := ss.CreateSpaceStorage(ctx, payload)
			require.NoError(t, err)
			allIds = append(allIds, payload.SpaceHeaderWithId.Id)
			require.NoError(t, store.Close(ctx))
		}
		allSpaces, err := ss.AllSpaceIds()
		require.NoError(t, err)
		slices.Sort(allSpaces)
		slices.Sort(allIds)
		require.Equal(t, allIds, allSpaces)
	})
	t.Run("create and exists", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		payload := NewStorageCreatePayload(t)
		store, err := ss.CreateSpaceStorage(ctx, payload)
		require.NoError(t, err)
		nodeStore := store.(*nodeStorage)
		require.Equal(t, 1, nodeStore.cont.handlers)
		require.True(t, ss.SpaceExists(payload.SpaceHeaderWithId.Id))
	})
	t.Run("create and dump", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		payload := NewStorageCreatePayload(t)
		store, err := ss.CreateSpaceStorage(ctx, payload)
		require.NoError(t, err)
		var tempPath string
		err = ss.DumpStorage(ctx, store.Id(), func(path string) error {
			tempPath = path
			anyStore, err := anystore.Open(ctx, filepath.Join(path, "store.db"), nil)
			require.NoError(t, err)
			_, err = spacestorage.New(ctx, store.Id(), anyStore)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
		require.NoDirExists(t, tempPath)
	})
	t.Run("try lock and do", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		payload := NewStorageCreatePayload(t)
		err := ss.TryLockAndDo(ctx, payload.SpaceHeaderWithId.Id, func() error {
			waitCh := make(chan struct{})
			go func() {
				ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
				defer cancel()
				_, err := ss.CreateSpaceStorage(ctx, payload)
				require.Equal(t, ctx.Err(), err)
				close(waitCh)
			}()
			<-waitCh
			path := ss.StoreDir(payload.SpaceHeaderWithId.Id)
			os.MkdirAll(path, 0755)
			anyStore, err := anystore.Open(ctx, filepath.Join(ss.StoreDir(payload.SpaceHeaderWithId.Id), "store.db"), nil)
			require.NoError(t, err)
			_, err = spacestorage.Create(ctx, anyStore, payload)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
		store, err := ss.WaitSpaceStorage(ctx, payload.SpaceHeaderWithId.Id)
		require.NoError(t, err)
		require.NoError(t, store.Close(ctx))
	})
	t.Run("delete", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		payload := NewStorageCreatePayload(t)
		_, err := ss.CreateSpaceStorage(ctx, payload)
		require.NoError(t, err)
		err = ss.DeleteSpaceStorage(ctx, payload.SpaceHeaderWithId.Id)
		require.NoError(t, err)
		require.NoDirExists(t, ss.StoreDir(payload.SpaceHeaderWithId.Id))
	})
	t.Run("delete missing", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		err := ss.DeleteSpaceStorage(ctx, "1")
		require.NoError(t, err)
	})
	t.Run("get stats", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		st := GenStorage(t, ss, 1000, 1000)
		stats, err := ss.GetStats(ctx, st.Id(), 0)
		require.NoError(t, err)
		require.Equal(t, 1001, stats.Storage.ObjectsCount)
		require.Equal(t, 0, stats.Storage.DeletedObjectsCount)
		require.Equal(t, 1000, stats.Storage.ChangeSize.MaxLen)
		require.Equal(t, 1000, int(stats.Storage.ChangeSize.Median))
		// settings tree change has size like 285, that's why we round down to 999 :-)
		require.Equal(t, 999, int(stats.Storage.ChangeSize.Avg))
		require.Equal(t, 1000285, stats.Storage.ChangeSize.Total)
	})
	t.Run("restore", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)

		payload := NewStorageCreatePayload(t)
		_, err := ss.CreateSpaceStorage(ctx, payload)
		require.NoError(t, err)

		spaceId := payload.SpaceHeaderWithId.Id
		require.NoError(t, ss.ForceRemove(spaceId))

		tmpDir, err := os.MkdirTemp("", "")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		spacePath := ss.StoreDir(spaceId)

		require.NoError(t, os.Rename(filepath.Join(spacePath, "store.db"), filepath.Join(tmpDir, "store.db")))
		require.NoError(t, os.RemoveAll(spacePath))

		require.NoError(t, ss.IndexStorage().SetSpaceStatus(ctx, spaceId, SpaceStatusOk, ""))
		require.NoError(t, ss.IndexStorage().MarkArchived(ctx, spaceId, 1, 2))

		ss.archive.(*mock_archive.MockArchive).EXPECT().Restore(gomock.Any(), spaceId).Do(func(_ context.Context, _ string) error {
			require.NoError(t, os.MkdirAll(spacePath, 0755))
			require.NoError(t, os.Rename(filepath.Join(tmpDir, "store.db"), filepath.Join(spacePath, "store.db")))
			return nil
		})

		store, err := ss.WaitSpaceStorage(ctx, spaceId)
		require.NoError(t, err)
		require.NoError(t, store.Close(ctx))
	})
}

func TestStorageService_TryLockAndOpenDb(t *testing.T) {
	ss := newStorageService(t)
	defer ss.Close(ctx)
	payload := NewStorageCreatePayload(t)
	store, err := ss.CreateSpaceStorage(ctx, payload)
	require.NoError(t, err)
	spaceId := store.Id()

	err = ss.TryLockAndOpenDb(ctx, spaceId, func(db anystore.DB) error { return nil })
	require.ErrorIs(t, err, ErrLocked)

	_ = ss.ForceRemove(spaceId)

	var called bool
	err = ss.TryLockAndOpenDb(ctx, spaceId, func(db anystore.DB) error {
		called = true
		names, _ := db.GetCollectionNames(ctx)
		assert.NotEmpty(t, names)
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)
}

func TestSpaceStorage_GetSpaceStats_CalcMedian(t *testing.T) {
	l1 := []int{1, 3, 2}
	l2 := []int{1}
	l3 := []int{3, 3, 3, 3, 2, 22, 2, 2, 2, 2, 1, 1, 1, 1, 1, 111}
	sort.Ints(l1)
	sort.Ints(l2)
	sort.Ints(l3)
	assertFloat64(t, 2.0, calcMedian(l1), "should have a correct median on odd-sized slice")
	assertFloat64(t, 1.0, calcMedian(l2), "should have a correct median on even-sized slice")
	assertFloat64(t, 2.0, calcMedian(l3), "should have a correct median on mixed-value slice")
}

func TestSpaceStorage_GetSpaceStats_CalcAvg(t *testing.T) {
	l1 := []int{1, 3, 2}
	l2 := []int{1}
	l3 := []int{3, 3, 3, 3, 2, 22, 2, 2, 2, 2, 1, 1, 1, 1, 1, 111}

	assertFloat64(t, 2.0, calcAvg(l1), "should have a correct avg 1")
	assertFloat64(t, 1.0, calcAvg(l2), "should have a correct avg 2")
	assertFloat64(t, 10.0, calcAvg(l3), "should have a correct avg 3")

}

func TestSpaceStorage_GetSpaceStats_CalcP95(t *testing.T) {
	l1 := []int{1, 3, 2, 4}
	l2 := []int{1, 2}
	l3 := []int{3, 3, 3, 3, 2, 22, 2, 2, 2, 2, 1, 1, 1, 1, 1, 111}
	l4 := []int{1}
	sort.Ints(l1)
	sort.Ints(l2)
	sort.Ints(l3)

	assertFloat64(t, 3.85, calcP95(l1), "should have a correct p95 1")
	assertFloat64(t, 1.95, calcP95(l2), "should have a correct p95 2")
	assertFloat64(t, 44.25, calcP95(l3), "should have a correct p95 3")
	assertFloat64(t, 1.0, calcP95(l4), "should have a correct p95 4")
}

func assertFloat64(t *testing.T, a, b float64, msg string) {
	tolerance := 10e-4
	ok := math.Abs(a-b) <= tolerance
	require.True(t, ok, msg, fmt.Sprintf("(%.4f !~ %.4f)", a, b))
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

func (m mockConfigGetter) GetStorage() Config {
	return Config{
		Path:         m.tempStoreOld,
		AnyStorePath: m.tempStoreNew,
	}
}

var ctx = context.Background()

func newStorageService(t *testing.T) *storageService {
	return newStorageServiceWithDir(t, t.TempDir())
}

func newStorageServiceWithDir(t *testing.T, tempDir string) *storageService {
	ss := New()
	a := new(app.App)

	ctrl := gomock.NewController(t)
	archive := mock_archive.NewMockArchive(ctrl)
	anymock.ExpectComp(archive.EXPECT(), archiveCName)

	t.Cleanup(ctrl.Finish)

	a.Register(mockConfigGetter{tempStoreNew: filepath.Join(tempDir, "new"), tempStoreOld: filepath.Join(tempDir, "old")}).Register(ss).Register(archive)
	a.Start(ctx)
	return ss.(*storageService)
}
