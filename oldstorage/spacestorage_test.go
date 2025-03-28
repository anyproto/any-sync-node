package oldstorage

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	spacestorage "github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage/oldstorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-node/nodestorage"
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

func (sc storeConfig) GetStorage() nodestorage.Config {
	return nodestorage.Config{Path: string(sc)}
}
func newTestService(dir string) *storageService {
	ss := New()
	a := new(app.App)
	a.Register(storeConfig(dir)).Register(ss)
	a.Start(context.Background())
	return ss.(*storageService)
}

func testSpace(t *testing.T, store oldstorage.SpaceStorage, payload spacestorage.SpaceStorageCreatePayload) {
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

	var storeCh = make(chan oldstorage.SpaceStorage)

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

func TestSpaceStorage_GetSpaceStats(t *testing.T) {
	dir, err := os.MkdirTemp("", "")

	require.NoError(t, err)
	defer os.RemoveAll(dir)

	payload := spaceTestPayload()
	store, _ := createSpaceStorage(newTestService(dir), payload)

	for i := range 1000 {
		n := i + 10
		treePayload := dummyTreeTestPayload(n, 1000+n)
		_, err = store.CreateTreeStorage(treePayload)
		require.NoError(t, err)
	}

	maxLen := 8400
	treePayload := dummyTreeTestPayload(3000, maxLen)
	_, err = store.CreateTreeStorage(treePayload)
	require.NoError(t, err)

	storeStats, ok := store.(NodeStorageStats)
	assert.True(t, ok, "should be casted to NodeStorageStats")

	stats, err := storeStats.GetSpaceStats(0)
	assert.Equal(t, 1002, stats.ObjectsCount, "should have a correct ObjectsCount")
	assert.Equal(t, maxLen, stats.ChangeSize.MaxLen, "should have a correct MaxLen")
	assertFloat64(t, 1013.6835, stats.ChangeSize.Avg, "should have a correct Avg")
	assertFloat64(t, 1010, stats.ChangeSize.Median, "should have a correct Median")
	assertFloat64(t, 1910.9, stats.ChangeSize.P95, "should have a correct P95")

}
