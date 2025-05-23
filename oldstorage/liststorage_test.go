package oldstorage

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/commonspace/spacestorage/oldstorage"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/stretchr/testify/require"
)

func testList(t *testing.T, store oldstorage.ListStorage, root *consensusproto.RawRecordWithId, head string) {
	require.Equal(t, store.Id(), root.Id)

	aclRoot, err := store.Root()
	require.NoError(t, err)
	require.Equal(t, root, aclRoot)

	aclHead, err := store.Head()
	require.NoError(t, err)
	require.Equal(t, head, aclHead)
}

func TestListStorage_Create(t *testing.T) {
	fx := newFixture(t)
	fx.open(t)
	defer fx.stop(t)

	aclRoot := &consensusproto.RawRecordWithId{Payload: []byte("root"), Id: "someRootId"}
	listStore, err := createListStorage(fx.db, aclRoot)
	require.NoError(t, err)
	testList(t, listStore, aclRoot, aclRoot.Id)

	t.Run("create same list storage returns nil", func(t *testing.T) {
		// this is ok, because we only create new list storage when we create space storage
		listStore, err := createListStorage(fx.db, aclRoot)
		require.NoError(t, err)
		testList(t, listStore, aclRoot, aclRoot.Id)
	})
}

func TestListStorage_Methods(t *testing.T) {
	fx := newFixture(t)
	fx.open(t)
	aclRoot := &consensusproto.RawRecordWithId{Payload: []byte("root"), Id: "someRootId"}
	_, err := createListStorage(fx.db, aclRoot)
	require.NoError(t, err)
	fx.stop(t)

	fx.open(t)
	defer fx.stop(t)
	listStore, err := newListStorage(fx.db)
	require.NoError(t, err)
	testList(t, listStore, aclRoot, aclRoot.Id)

	t.Run("set head", func(t *testing.T) {
		head := "newHead"
		require.NoError(t, listStore.SetHead(head))
		aclHead, err := listStore.Head()
		require.NoError(t, err)
		require.Equal(t, head, aclHead)
	})

	t.Run("add raw record and get raw record", func(t *testing.T) {
		newRec := &consensusproto.RawRecordWithId{Payload: []byte("rec"), Id: "someRecId"}
		require.NoError(t, listStore.AddRawRecord(context.Background(), newRec))
		aclRec, err := listStore.GetRawRecord(context.Background(), newRec.Id)
		require.NoError(t, err)
		require.Equal(t, newRec, aclRec)
	})
}
