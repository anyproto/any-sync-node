package nodestorage

import (
	"context"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	spacestorage "github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/cidutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
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

func TestSpaceStorage_GetHashes(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	payload := spaceTestPayload()
	store, err := createSpaceStorage(newTestService(dir), payload)
	require.NoError(t, err)
	rand.Seed(uint64(time.Now().Unix()))
	var maxLen int
	for i := 0; i < 1000; i++ {
		n := rand.Intn(10000) + 10
		if n > maxLen {
			maxLen = n
		}
		buf := make([]byte, n)
		_, _ = rand.Read(buf)
		root := buf
		rootId, _ := cidutil.NewCidFromBytes(root)
		buf = make([]byte, n)
		_, _ = rand.Read(buf)
		other := buf
		otherId, _ := cidutil.NewCidFromBytes(root)
		payload := treeTestPayloadWithData(root, other, rootId, otherId)
		_, err := store.CreateTreeStorage(payload)
		require.NoError(t, err)
	}
	stats, err := store.(NodeStorageStats).GetSpaceStats()
	require.NoError(t, err)
	require.Equal(t, maxLen, stats.ChangeSize.MaxLen)
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
