package nodestorage

import (
	"context"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"fmt"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	spacestorage "github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
)

var ctx = context.Background()

func spaceTestPayload() spacestorage.SpaceStorageCreatePayload {
	header := &spacesyncproto.RawSpaceHeaderWithId{
		RawHeader: []byte("header"),
		Id:        "headerId",
	}
	aclRoot := &consensusproto.RawRecordWithId{
		Payload: []byte("aclRoot"),
		Id:      "aclRootId",
	}
	settings := &treechangeproto.RawTreeChangeWithId{
		RawChange: []byte("settings"),
		Id:        "settingsId",
	}
	return spacestorage.SpaceStorageCreatePayload{
		AclWithId:           aclRoot,
		SpaceHeaderWithId:   header,
		SpaceSettingsWithId: settings,
	}
}

type storeConfig string

func (sc storeConfig) Name() string          { return "config" }
func (sc storeConfig) Init(_ *app.App) error { return nil }

func (sc storeConfig) GetStorage() Config {
	return Config{Path: string(sc)}
}
func newTestService(dir string) *storageService {
	ss := New()
	a := new(app.App)
	a.Register(storeConfig(dir)).Register(ss)
	a.Start(context.Background())
	return ss.(*storageService)
}

func testSpace(t *testing.T, store spacestorage.SpaceStorage, payload spacestorage.SpaceStorageCreatePayload) {
	header, err := store.SpaceHeader()
	require.NoError(t, err)
	require.Equal(t, payload.SpaceHeaderWithId, header)

	aclStorage, err := store.AclStorage()
	require.NoError(t, err)
	testList(t, aclStorage, payload.AclWithId, payload.AclWithId.Id)
}

func TestSpaceStorage_Create(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	payload := spaceTestPayload()
	store, err := createSpaceStorage(newTestService(dir), payload)
	require.NoError(t, err)

	testSpace(t, store, payload)
	require.NoError(t, store.Close(ctx))

	t.Run("create same storage returns error", func(t *testing.T) {
		_, err := createSpaceStorage(newTestService(dir), payload)
		require.Error(t, err)
	})
}

func TestSpaceStorage_NewAndCreateTree(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	payload := spaceTestPayload()
	store, err := createSpaceStorage(newTestService(dir), payload)
	require.NoError(t, err)
	require.NoError(t, store.Close(ctx))

	store, err = newSpaceStorage(&storageService{rootPath: dir}, payload.SpaceHeaderWithId.Id)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(ctx))
	}()
	testSpace(t, store, payload)

	t.Run("create tree, get tree and mark deleted", func(t *testing.T) {
		payload := treeTestPayload()
		treeStore, err := store.CreateTreeStorage(payload)
		require.NoError(t, err)
		testTreePayload(t, treeStore, payload)

		otherStore, err := store.TreeStorage(payload.RootRawChange.Id)
		require.NoError(t, err)
		testTreePayload(t, otherStore, payload)

		initialStatus := "deleted"
		err = store.SetTreeDeletedStatus(otherStore.Id(), initialStatus)
		require.NoError(t, err)

		status, err := store.TreeDeletedStatus(otherStore.Id())
		require.NoError(t, err)
		require.Equal(t, initialStatus, status)
	})
}

func TestSpaceStorage_StoredIds(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	payload := spaceTestPayload()
	store, err := createSpaceStorage(newTestService(dir), payload)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(ctx))
	}()

	n := 5
	var ids []string
	for i := 0; i < n; i++ {
		treePayload := treeTestPayload()
		treePayload.RootRawChange.Id += strconv.Itoa(i)
		ids = append(ids, treePayload.RootRawChange.Id)
		_, err := store.CreateTreeStorage(treePayload)
		require.NoError(t, err)
	}
	ids = append(ids, payload.SpaceSettingsWithId.Id)
	sort.Strings(ids)

	storedIds, err := store.StoredIds()
	sort.Strings(storedIds)
	require.NoError(t, err)
	require.Equal(t, ids, storedIds)
}

func TestSpaceStorage_WriteSpaceHash(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	payload := spaceTestPayload()
	store, err := createSpaceStorage(newTestService(dir), payload)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close(ctx))
	}()

	hash := "123"
	require.NoError(t, store.WriteSpaceHash(hash))
	hash2, err := store.ReadSpaceHash()
	require.NoError(t, err)
	assert.Equal(t, hash, hash2)
}

func TestLock(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	ss := newTestService(dir)

	payload := spaceTestPayload()
	store, err := ss.CreateSpaceStorage(payload)
	require.NoError(t, err)

	_, err = ss.SpaceStorage(payload.SpaceHeaderWithId.Id)
	require.Equal(t, ErrLocked, err)

	require.NoError(t, store.Close(ctx))

	store, err = ss.SpaceStorage(payload.SpaceHeaderWithId.Id)
	require.NoError(t, err)
	require.NoError(t, store.Close(ctx))
}

func TestWaitStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	ss := newTestService(dir)

	payload := spaceTestPayload()
	store, err := ss.CreateSpaceStorage(payload)
	require.NoError(t, err)

	var storeCh = make(chan spacestorage.SpaceStorage)

	go func() {
		st, e := ss.WaitSpaceStorage(context.Background(), payload.SpaceHeaderWithId.Id)
		require.NoError(t, e)
		storeCh <- st
	}()

	select {
	case <-time.After(time.Second / 10):
	case <-storeCh:
		assert.True(t, false, "should be locked")
	}

	require.NoError(t, store.Close(ctx))

	select {
	case <-time.After(time.Second / 10):
		assert.True(t, false, "timeout")
	case store = <-storeCh:
	}
	require.NoError(t, store.Close(ctx))
}

func assertFloat64(t *testing.T, a, b float64, msg string) {
	tolerance := 10e-4
	ok := math.Abs(a-b) <= tolerance
	assert.True(t, ok, msg, fmt.Sprintf("(%.4f !~ %.4f)", a, b))
}

func TestSpaceStorage_GetSpaceStats_CalcMedian(t *testing.T) {
	assertFloat64(t, 2.0, calcMedian([]int{1, 3, 2}), "should have a correct median on odd-sized slice")
	assertFloat64(t, 1.0, calcMedian([]int{1}), "should have a correct median on even-sized slice")
	assertFloat64(t, 2.0, calcMedian([]int{3, 3, 3, 3, 2, 22, 2, 2, 2, 2, 1, 1, 1, 1, 1, 111}), "should have a correct median on mixed-value slice")
}

func TestSpaceStorage_GetSpaceStats(t *testing.T) {
	dir, err := os.MkdirTemp("", "")

	require.NoError(t, err)
	defer os.RemoveAll(dir)

	payload := spaceTestPayload()
	store, _ := createSpaceStorage(newTestService(dir), payload)

	for i := range 1000 {
		n := i + 10
		treePayload := dummyTreeTestPayload(n)
		_, err = store.CreateTreeStorage(treePayload)
		require.NoError(t, err)
	}

	treePayload := dummyTreeTestPayload(1984)
	_, err = store.CreateTreeStorage(treePayload)
	require.NoError(t, err)

	storeStats, ok := store.(NodeStorageStats)
	assert.True(t, ok, "should be casted to NodeStorageStats")

	stats, err := storeStats.GetSpaceStats()
	assert.Equal(t, 2010, stats.DocsCount, "should have a correct DocsCount")
	assert.Equal(t, 1984, stats.ChangeSize.MaxLen, "should have a correct MaxLen")
	assertFloat64(t, 0.9870, stats.ChangeSize.Avg, "should have a correct Avg")
	assertFloat64(t, 0.98701563456, stats.ChangeSize.Median, "should have a correct Median")
	assertFloat64(t, 0.9870, stats.ChangeSize.P95, "should have a correct P95")

}
