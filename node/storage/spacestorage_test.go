package storage

import (
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/commonspace/spacesyncproto"
	spacestorage "github.com/anytypeio/go-anytype-infrastructure-experiments/common/commonspace/storage"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/pkg/acl/aclrecordproto"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/common/pkg/acl/treechangeproto"
	"github.com/stretchr/testify/require"
	"os"
	"sort"
	"strconv"
	"testing"
)

func spaceTestPayload() spacestorage.SpaceStorageCreatePayload {
	header := &spacesyncproto.RawSpaceHeaderWithId{
		RawHeader: []byte("header"),
		Id:        "headerId",
	}
	aclRoot := &aclrecordproto.RawACLRecordWithId{
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

func testSpace(t *testing.T, store spacestorage.SpaceStorage, payload spacestorage.SpaceStorageCreatePayload) {
	header, err := store.SpaceHeader()
	require.NoError(t, err)
	require.Equal(t, payload.SpaceHeaderWithId, header)

	aclStorage, err := store.ACLStorage()
	require.NoError(t, err)
	testList(t, aclStorage, payload.AclWithId, payload.AclWithId.Id)
}

func TestSpaceStorage_Create(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	payload := spaceTestPayload()
	store, err := createSpaceStorage(dir, payload)
	require.NoError(t, err)

	testSpace(t, store, payload)
	require.NoError(t, store.Close())

	t.Run("create same storage returns error", func(t *testing.T) {
		_, err := createSpaceStorage(dir, payload)
		require.Error(t, err)
	})
}

func TestSpaceStorage_NewAndCreateTree(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	payload := spaceTestPayload()
	store, err := createSpaceStorage(dir, payload)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	store, err = newSpaceStorage(dir, payload.SpaceHeaderWithId.Id)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
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

	payload := spaceTestPayload()
	store, err := createSpaceStorage(dir, payload)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
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