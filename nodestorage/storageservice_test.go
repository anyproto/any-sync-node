package nodestorage

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestStorageService_SpaceStorage(t *testing.T) {
	t.Run("create and get", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		payload := newStorageCreatePayload(t)
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
	t.Run("create and all spaces", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		var allIds []string
		for i := 0; i < 10; i++ {
			payload := newStorageCreatePayload(t)
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
		payload := newStorageCreatePayload(t)
		store, err := ss.CreateSpaceStorage(ctx, payload)
		require.NoError(t, err)
		nodeStore := store.(*nodeStorage)
		require.Equal(t, 1, nodeStore.cont.handlers)
		require.True(t, ss.SpaceExists(payload.SpaceHeaderWithId.Id))
	})
	t.Run("create and dump", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		payload := newStorageCreatePayload(t)
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
		payload := newStorageCreatePayload(t)
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
		payload := newStorageCreatePayload(t)
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
		require.Equal(t, spacestorage.ErrSpaceStorageMissing, err)
	})
	t.Run("get stats", func(t *testing.T) {
		ss := newStorageService(t)
		defer ss.Close(ctx)
		st := genStorage(t, ss, 1000, 1000)
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

func newPayloadWithAcc(t *testing.T, keys *accountdata.AccountKeys) spacestorage.SpaceStorageCreatePayload {
	masterKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	readKey := crypto.NewAES()
	meta := []byte("account")
	payload := commonspace.SpaceCreatePayload{
		SigningKey:     keys.SignKey,
		SpaceType:      "space",
		ReplicationKey: 10,
		SpacePayload:   nil,
		MasterKey:      masterKey,
		ReadKey:        readKey,
		MetadataKey:    metaKey,
		Metadata:       meta,
	}
	createSpace, err := commonspace.StoragePayloadForSpaceCreate(payload)
	require.NoError(t, err)
	return createSpace
}

func newStorageCreatePayload(t *testing.T) spacestorage.SpaceStorageCreatePayload {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	masterKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	readKey := crypto.NewAES()
	meta := []byte("account")
	payload := commonspace.SpaceCreatePayload{
		SigningKey:     keys.SignKey,
		SpaceType:      "space",
		ReplicationKey: 10,
		SpacePayload:   nil,
		MasterKey:      masterKey,
		ReadKey:        readKey,
		MetadataKey:    metaKey,
		Metadata:       meta,
	}
	createSpace, err := commonspace.StoragePayloadForSpaceCreate(payload)
	require.NoError(t, err)
	return createSpace
}

type testChangeBuilder struct {
}

func (t testChangeBuilder) Unmarshall(rawIdChange *treechangeproto.RawTreeChangeWithId, verify bool) (ch *objecttree.Change, err error) {
	return &objecttree.Change{IsDerived: false}, nil
}

func (t testChangeBuilder) UnmarshallReduced(rawIdChange *treechangeproto.RawTreeChangeWithId) (ch *objecttree.Change, err error) {
	panic("should not call")
}

func (t testChangeBuilder) Build(payload objecttree.BuilderContent) (ch *objecttree.Change, raw *treechangeproto.RawTreeChangeWithId, err error) {
	panic("should not call")
}

func (t testChangeBuilder) BuildRoot(payload objecttree.InitialContent) (ch *objecttree.Change, raw *treechangeproto.RawTreeChangeWithId, err error) {
	panic("should not call")
}

func (t testChangeBuilder) BuildDerivedRoot(payload objecttree.InitialDerivedContent) (ch *objecttree.Change, raw *treechangeproto.RawTreeChangeWithId, err error) {
	panic("should not call")
}

func (t testChangeBuilder) Marshall(ch *objecttree.Change) (*treechangeproto.RawTreeChangeWithId, error) {
	panic("should not call")
}

var ctx = context.Background()

func newStorageService(t *testing.T) *storageService {
	ss := New()
	a := new(app.App)
	tempDir := t.TempDir()

	a.Register(mockConfigGetter{tempStoreNew: filepath.Join(tempDir, "new"), tempStoreOld: filepath.Join(tempDir, "old")}).Register(ss)
	a.Start(ctx)
	return ss.(*storageService)
}

func createTreeStorage(t *testing.T, storage spacestorage.SpaceStorage, treeLen, changeLen int) {
	for i := 0; i < treeLen; i++ {
		raw := make([]byte, changeLen)
		payload := treestorage.TreeStorageCreatePayload{
			RootRawChange: &treechangeproto.RawTreeChangeWithId{
				Id:        fmt.Sprintf("root-%d", i),
				RawChange: raw,
			},
		}
		_, err := storage.CreateTreeStorage(ctx, payload)
		require.NoError(t, err)
	}
}

func genStorage(t *testing.T, ss *storageService, treeLen, changeLen int) *nodeStorage {
	payload := newStorageCreatePayload(t)
	store, err := ss.CreateSpaceStorage(ctx, payload)
	objecttree.StorageChangeBuilder = func(keys crypto.KeyStorage, rootChange *treechangeproto.RawTreeChangeWithId) objecttree.ChangeBuilder {
		return testChangeBuilder{}
	}
	require.NoError(t, err)
	createTreeStorage(t, store, treeLen, changeLen)
	return store.(*nodeStorage)
}
