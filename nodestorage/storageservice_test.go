package nodestorage

//
//import (
//	"context"
//	"github.com/stretchr/testify/require"
//	"os"
//	"sync/atomic"
//	"testing"
//	"time"
//)
//
//func TestStorageService_DeleteSpaceStorage(t *testing.T) {
//	dir, err := os.MkdirTemp("", "")
//	require.NoError(t, err)
//	defer os.RemoveAll(dir)
//	isClosed := atomic.Bool{}
//	deleteWait := make(chan struct{})
//
//	payload := spaceTestPayload()
//	testServ := newTestService(dir)
//	store, err := testServ.CreateSpaceStorage(payload)
//	require.NoError(t, err)
//	go func() {
//		storePath := testServ.StoreDir(payload.SpaceHeaderWithId.Id)
//		_, err = os.Stat(storePath)
//		require.NoError(t, err)
//		err = testServ.DeleteSpaceStorage(ctx, payload.SpaceHeaderWithId.Id)
//		require.True(t, isClosed.Load())
//		require.NoError(t, err)
//		_, err = os.Stat(storePath)
//		require.Error(t, os.ErrNotExist, err)
//		_, err = testServ.WaitSpaceStorage(ctx, payload.SpaceHeaderWithId.Id)
//		require.Error(t, err)
//		close(deleteWait)
//	}()
//	// waiting to be sure that goroutine has started
//	time.Sleep(100 * time.Millisecond)
//	isClosed.Store(true)
//	store.Close(context.Background())
//	<-deleteWait
//}
//
//func TestStorageService_DeletionStorage(t *testing.T) {
//	dir, err := os.MkdirTemp("", "")
//	require.NoError(t, err)
//	defer os.RemoveAll(dir)
//
//	testServ := newTestService(dir)
//	store := testServ.DeletionStorage()
//
//	// testing status
//	_, err = store.SpaceStatus("id")
//	require.Error(t, ErrUnknownSpaceId, err)
//	err = store.SetSpaceStatus("id", SpaceStatusRemove)
//	require.NoError(t, err)
//	status, err := store.SpaceStatus("id")
//	require.NoError(t, err)
//	require.Equal(t, SpaceStatusRemove, status)
//
//	// testing last stored id
//	_, err = store.LastRecordId()
//	require.Error(t, ErrNoLastRecordId, err)
//	err = store.SetLastRecordId("id")
//	require.NoError(t, err)
//	id, err := store.LastRecordId()
//	require.NoError(t, err)
//	require.Equal(t, "id", id)
//
//	err = testServ.Close(context.Background())
//	require.NoError(t, err)
//}
