package spacedeleter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/anyproto/any-sync/consensus/consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusclient/mock_consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/testutil/anymock"

	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodespace/mock_nodespace"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
	"github.com/anyproto/any-sync-node/nodesync/mock_nodesync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient/mock_coordinatorclient"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var ctx = context.Background()

func TestSpaceDeleter_Run_Ok(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	defer fx.stop(t)
	payload := nodestorage.NewStorageCreatePayload(t)
	store, err := fx.storage.CreateSpaceStorage(ctx, payload)
	require.NoError(t, err)
	lg := mockDeletionLog(store.Id())

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()
	fx.consClient.EXPECT().DeleteLog(gomock.Any(), payload.AclWithId.Id).Return(nil)
	store.Close(context.Background())

	close(fx.waiterChan)
	<-fx.deleter.testChan

	id, err := fx.storage.IndexStorage().LastRecordId(ctx)
	require.NoError(t, err)
	require.Equal(t, lg[2].Id, id)
	store, err = fx.storage.WaitSpaceStorage(ctx, payload.SpaceHeaderWithId.Id)
	require.Error(t, err)
	status, err := fx.storage.IndexStorage().SpaceStatus(ctx, payload.SpaceHeaderWithId.Id)
	require.NoError(t, err)
	require.Equal(t, nodestorage.SpaceStatusRemove, status)
}

func TestSpaceDeleter_Run_Ok_NewPush(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	defer fx.stop(t)
	payload := nodestorage.NewStorageCreatePayload(t)
	store, err := fx.storage.CreateSpaceStorage(ctx, payload)
	require.NoError(t, err)
	lg := mockDeletionLogNewPush(store.Id())

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()
	fx.consClient.EXPECT().DeleteLog(gomock.Any(), payload.AclWithId.Id).Return(nil)
	store.Close(context.Background())

	close(fx.waiterChan)
	<-fx.deleter.testChan

	id, err := fx.storage.IndexStorage().LastRecordId(ctx)
	require.NoError(t, err)
	require.Equal(t, lg[3].Id, id)
	status, err := fx.storage.IndexStorage().SpaceStatus(ctx, payload.SpaceHeaderWithId.Id)
	require.NoError(t, err)
	require.Equal(t, nodestorage.SpaceStatusOk, status)
}

func TestSpaceDeleter_Run_Ok_LogNotFound(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	defer fx.stop(t)
	payload := nodestorage.NewStorageCreatePayload(t)
	store, err := fx.storage.CreateSpaceStorage(ctx, payload)
	require.NoError(t, err)
	lg := mockDeletionLog(store.Id())

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()
	fx.consClient.EXPECT().DeleteLog(gomock.Any(), payload.AclWithId.Id).Return(consensuserr.ErrLogNotFound)
	store.Close(context.Background())

	close(fx.waiterChan)
	<-fx.deleter.testChan

	id, err := fx.storage.IndexStorage().LastRecordId(ctx)
	require.NoError(t, err)
	require.Equal(t, lg[2].Id, id)
	store, err = fx.storage.WaitSpaceStorage(context.Background(), payload.SpaceHeaderWithId.Id)
	require.Error(t, err)
	status, err := fx.storage.IndexStorage().SpaceStatus(ctx, payload.SpaceHeaderWithId.Id)
	require.NoError(t, err)
	require.Equal(t, nodestorage.SpaceStatusRemove, status)
}

func TestSpaceDeleter_Run_Ok_NoStorage(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	defer fx.stop(t)
	lg := mockDeletionLog("space3")

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()

	close(fx.waiterChan)
	<-fx.deleter.testChan

	id, err := fx.storage.IndexStorage().LastRecordId(ctx)
	require.NoError(t, err)
	require.Equal(t, lg[2].Id, id)
	status, err := fx.storage.IndexStorage().SpaceStatus(ctx, "space3")
	require.NoError(t, err)
	require.Equal(t, nodestorage.SpaceStatusRemove, status)
}

type forceRemover interface {
	nodestorage.NodeStorage
	ForceRemove(id string) (err error)
}

func TestSpaceDeleter_Run_Ok_EmptyStorage(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	defer fx.stop(t)
	payload := nodestorage.NewStorageCreatePayload(t)
	store, err := fx.storage.CreateSpaceStorage(ctx, payload)
	require.NoError(t, err)
	lg := mockDeletionLog(store.Id())
	collNames, err := store.AnyStore().GetCollectionNames(ctx)
	require.NoError(t, err)
	for _, name := range collNames {
		coll, err := store.AnyStore().Collection(ctx, name)
		require.NoError(t, err)
		err = coll.Drop(ctx)
		require.NoError(t, err)
	}
	collNames, err = store.AnyStore().GetCollectionNames(ctx)
	require.NoError(t, err)
	require.Empty(t, collNames)
	err = fx.storage.(forceRemover).ForceRemove(store.Id())
	require.NoError(t, err)

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()
	store.Close(context.Background())

	close(fx.waiterChan)
	<-fx.deleter.testChan

	id, err := fx.storage.IndexStorage().LastRecordId(ctx)
	require.NoError(t, err)
	require.Equal(t, lg[2].Id, id)
	store, err = fx.storage.WaitSpaceStorage(context.Background(), payload.SpaceHeaderWithId.Id)
	require.Error(t, err)
	status, err := fx.storage.IndexStorage().SpaceStatus(ctx, payload.SpaceHeaderWithId.Id)
	require.NoError(t, err)
	require.Equal(t, nodestorage.SpaceStatusRemove, status)
}

func TestSpaceDeleter_Run_Failure_LogError(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	payload := nodestorage.NewStorageCreatePayload(t)
	store, err := fx.storage.CreateSpaceStorage(ctx, payload)
	require.NoError(t, err)
	lg := mockDeletionLog(store.Id())

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()
	fx.consClient.EXPECT().DeleteLog(gomock.Any(), payload.AclWithId.Id).Return(consensuserr.ErrConflict)
	store.Close(context.Background())

	close(fx.waiterChan)
	<-fx.deleter.testChan

	id, err := fx.storage.IndexStorage().LastRecordId(ctx)
	require.NoError(t, err)
	require.Equal(t, lg[0].Id, id)
	// checking that storage is still there
	store, err = fx.storage.WaitSpaceStorage(ctx, payload.SpaceHeaderWithId.Id)
	require.NoError(t, err)
	store.Close(context.Background())
	_, err = fx.storage.IndexStorage().SpaceStatus(ctx, payload.SpaceHeaderWithId.Id)
	require.Error(t, nodestorage.ErrUnknownSpaceId, err)
	fx.stop(t)
}

func mockDeletionLog(realId string) []*coordinatorproto.DeletionLogRecord {
	return []*coordinatorproto.DeletionLogRecord{
		{Id: "1", SpaceId: "space1", Status: coordinatorproto.DeletionLogRecordStatus_Ok},
		{Id: "2", SpaceId: "space2", Status: coordinatorproto.DeletionLogRecordStatus_RemovePrepare},
		{Id: "3", SpaceId: realId, Status: coordinatorproto.DeletionLogRecordStatus_Remove},
	}
}

func mockDeletionLogNewPush(realId string) []*coordinatorproto.DeletionLogRecord {
	return []*coordinatorproto.DeletionLogRecord{
		{Id: "1", SpaceId: "space1", Status: coordinatorproto.DeletionLogRecordStatus_Ok},
		{Id: "2", SpaceId: "space2", Status: coordinatorproto.DeletionLogRecordStatus_RemovePrepare},
		{Id: "3", SpaceId: realId, Status: coordinatorproto.DeletionLogRecordStatus_Remove},
		{Id: "4", SpaceId: realId, Status: coordinatorproto.DeletionLogRecordStatus_Ok},
	}
}

type spaceDeleterFixture struct {
	coordClient  *mock_coordinatorclient.MockCoordinatorClient
	consClient   *mock_consensusclient.MockService
	spaceService *mock_nodespace.MockService
	storage      nodestorage.NodeStorage
	nodesync     *mock_nodesync.MockNodeSync
	deleter      *spaceDeleter
	waiterChan   chan struct{}
	ctrl         *gomock.Controller
	dir          string
	app          *app.App
}

type storeConfig string

func (sc storeConfig) Name() string          { return "config" }
func (sc storeConfig) Init(_ *app.App) error { return nil }

func (sc storeConfig) GetStorage() nodestorage.Config {
	return nodestorage.Config{Path: string(sc), AnyStorePath: string(sc)}
}

func newSpaceDeleterFixture(t *testing.T) *spaceDeleterFixture {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	a := new(app.App)
	ctrl := gomock.NewController(t)
	waiterChan := make(chan struct{})
	coordClient := mock_coordinatorclient.NewMockCoordinatorClient(ctrl)
	spaceService := mock_nodespace.NewMockService(ctrl)
	nodeSync := mock_nodesync.NewMockNodeSync(ctrl)
	consClient := mock_consensusclient.NewMockService(ctrl)
	storage := nodestorage.New()
	anymock.ExpectComp(coordClient.EXPECT(), coordinatorclient.CName)
	anymock.ExpectComp(spaceService.EXPECT(), nodespace.CName)
	anymock.ExpectComp(consClient.EXPECT(), consensusclient.CName)
	anymock.ExpectComp(nodeSync.EXPECT(), nodesync.CName)
	nodeSync.EXPECT().WaitSyncOnStart().Return(waiterChan).AnyTimes()
	deleter := New().(*spaceDeleter)
	a.Register(storeConfig(dir)).
		Register(coordClient).
		Register(consClient).
		Register(storage).
		Register(spaceService).
		Register(nodeSync).
		Register(deleter)
	err = a.Start(context.Background())
	require.NoError(t, err)
	return &spaceDeleterFixture{
		coordClient:  coordClient,
		consClient:   consClient,
		spaceService: spaceService,
		storage:      storage,
		nodesync:     nodeSync,
		deleter:      deleter,
		waiterChan:   waiterChan,
		ctrl:         ctrl,
		app:          a,
	}
}

const testSaveDelay = 500 * time.Millisecond

func (fx *spaceDeleterFixture) stop(t *testing.T) {
	fx.nodesync.EXPECT().Close(gomock.Any()).Return(nil).AnyTimes()
	fx.spaceService.EXPECT().Close(gomock.Any()).Return(nil).AnyTimes()
	fx.consClient.EXPECT().Close(gomock.Any()).Return(nil).AnyTimes()
	err := fx.app.Close(context.Background())
	require.NoError(t, err)
	fx.ctrl.Finish()
	err = os.RemoveAll(fx.dir)
	require.NoError(t, err)
}
