package oldstorage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/akrylysov/pogreb"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage/oldstorage"
	"github.com/anyproto/any-sync/util/cidutil"

	"github.com/stretchr/testify/require"
)

func treeTestPayload() treestorage.TreeStorageCreatePayload {
	rootRawChange := &treechangeproto.RawTreeChangeWithId{RawChange: []byte("some"), Id: "rootId"}
	otherChange := &treechangeproto.RawTreeChangeWithId{RawChange: []byte("some other"), Id: "otherId"}
	changes := []*treechangeproto.RawTreeChangeWithId{rootRawChange, otherChange}
	return treestorage.TreeStorageCreatePayload{
		RootRawChange: rootRawChange,
		Changes:       changes,
		Heads:         []string{rootRawChange.Id},
	}
}

func treeTestPayloadWithData(root, other []byte, rootId, otherId string) treestorage.TreeStorageCreatePayload {
	rootRawChange := &treechangeproto.RawTreeChangeWithId{RawChange: root, Id: rootId}
	otherChange := &treechangeproto.RawTreeChangeWithId{RawChange: other, Id: otherId}
	changes := []*treechangeproto.RawTreeChangeWithId{rootRawChange, otherChange}
	return treestorage.TreeStorageCreatePayload{
		RootRawChange: rootRawChange,
		Changes:       changes,
		Heads:         []string{rootRawChange.Id},
	}
}

func dummyTreeTestPayload(len1, len2 int) treestorage.TreeStorageCreatePayload {
	root := make([]byte, len1)
	other := make([]byte, len2)
	rootId, _ := cidutil.NewCidFromBytes(root)
	otherId, _ := cidutil.NewCidFromBytes(other)
	return treeTestPayloadWithData(root, other, rootId, otherId)
}

type fixture struct {
	dir string
	db  *pogreb.DB
}

func newFixture(t *testing.T) *fixture {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	return &fixture{dir: dir}
}

func (fx *fixture) open(t *testing.T) {
	var err error
	fx.db, err = pogreb.Open(fx.dir, nil)
	require.NoError(t, err)
}

func (fx *fixture) stop(t *testing.T) {
	require.NoError(t, fx.db.Close())
}

func (fx *fixture) testNoKeysExist(t *testing.T, treeId string) {
	index := fx.db.Items()
	treeKeys := newTreeKeys(treeId)
	var keys [][]byte
	key, _, err := index.Next()
	for err == nil {
		strKey := string(key)
		if treeKeys.isTreeRelatedKey(strKey) {
			keys = append(keys, key)
		}
		key, _, err = index.Next()
	}
	require.Equal(t, pogreb.ErrIterationDone, err)
	require.Equal(t, 0, len(keys))

	res, err := fx.db.Has(treeKeys.HeadsKey())
	require.NoError(t, err)
	require.False(t, res)
}

func testTreePayload(t *testing.T, store oldstorage.TreeStorage, payload treestorage.TreeStorageCreatePayload) {
	require.Equal(t, payload.RootRawChange.Id, store.Id())

	root, err := store.Root()
	require.NoError(t, err)
	require.Equal(t, root, payload.RootRawChange)

	heads, err := store.Heads()
	require.NoError(t, err)
	require.Equal(t, payload.Heads, heads)

	for _, ch := range payload.Changes {
		dbCh, err := store.GetRawChange(context.Background(), ch.Id)
		require.NoError(t, err)
		require.Equal(t, ch, dbCh)
	}
	return
}

func TestTreeStorage_Create(t *testing.T) {
	fx := newFixture(t)
	fx.open(t)
	defer fx.stop(t)

	payload := treeTestPayload()
	store, err := createTreeStorage(fx.db, payload)
	require.NoError(t, err)
	testTreePayload(t, store, payload)

	t.Run("create same storage returns error", func(t *testing.T) {
		_, err := createTreeStorage(fx.db, payload)
		require.Error(t, err)
	})
}

func TestTreeStorage_Methods(t *testing.T) {
	fx := newFixture(t)
	fx.open(t)
	payload := treeTestPayload()
	_, err := createTreeStorage(fx.db, payload)
	require.NoError(t, err)
	fx.stop(t)

	fx.open(t)
	defer fx.stop(t)
	store, err := newTreeStorage(fx.db, payload.RootRawChange.Id)
	require.NoError(t, err)
	testTreePayload(t, store, payload)

	t.Run("update heads", func(t *testing.T) {
		newHeads := []string{"a", "b"}
		require.NoError(t, store.SetHeads(newHeads))
		heads, err := store.Heads()
		require.NoError(t, err)
		require.Equal(t, newHeads, heads)
	})

	t.Run("add raw change, get change and has change", func(t *testing.T) {
		newChange := &treechangeproto.RawTreeChangeWithId{RawChange: []byte("ab"), Id: "newId"}
		require.NoError(t, store.AddRawChange(newChange))
		rawCh, err := store.GetRawChange(context.Background(), newChange.Id)
		require.NoError(t, err)
		require.Equal(t, newChange, rawCh)
		has, err := store.HasChange(context.Background(), newChange.Id)
		require.NoError(t, err)
		require.True(t, has)
	})

	t.Run("get and has for unknown change", func(t *testing.T) {
		incorrectId := "incorrectId"
		_, err := store.GetRawChange(context.Background(), incorrectId)
		require.Error(t, err)
		has, err := store.HasChange(context.Background(), incorrectId)
		require.NoError(t, err)
		require.False(t, has)
	})
}

func TestTreeStorage_Delete(t *testing.T) {
	fx := newFixture(t)
	fx.open(t)
	payload := treeTestPayload()
	_, err := createTreeStorage(fx.db, payload)
	require.NoError(t, err)
	fx.stop(t)

	fx.open(t)
	defer fx.stop(t)
	store, err := newTreeStorage(fx.db, payload.RootRawChange.Id)
	require.NoError(t, err)
	testTreePayload(t, store, payload)

	t.Run("add raw change, get change and has change", func(t *testing.T) {
		newChange := &treechangeproto.RawTreeChangeWithId{RawChange: []byte("ab"), Id: "newId"}
		require.NoError(t, store.AddRawChange(newChange))

		err = store.Delete()
		require.NoError(t, err)

		_, err = newTreeStorage(fx.db, payload.RootRawChange.Id)
		require.Equal(t, err, treestorage.ErrUnknownTreeId)

		fx.testNoKeysExist(t, payload.RootRawChange.Id)
	})
}

func TestTreeStorage_ChangedIds(t *testing.T) {
	fx := newFixture(t)
	fx.open(t)
	payload := treeTestPayload()
	_, err := createTreeStorage(fx.db, payload)
	require.NoError(t, err)
	fx.stop(t)

	fx.open(t)
	defer fx.stop(t)
	store, err := newTreeStorage(fx.db, payload.RootRawChange.Id)
	require.NoError(t, err)
	testTreePayload(t, store, payload)

	t.Run("add raw change, get change and has change", func(t *testing.T) {
		newChange := &treechangeproto.RawTreeChangeWithId{RawChange: []byte("ab"), Id: "newId"}
		require.NoError(t, store.AddRawChange(newChange))

		ids, err := store.GetAllChangeIds()
		require.NoError(t, err)
		require.Equal(t, []string{"rootId", "otherId", "newId"}, ids)
		fmt.Println(ids)
	})
}
