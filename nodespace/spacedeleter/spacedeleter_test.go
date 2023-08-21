package spacedeleter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodespace/mock_nodespace"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
	"github.com/anyproto/any-sync-node/nodesync/mock_nodesync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient/mock_coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type spaceDeleterFixture struct {
	coordClient  *mock_coordinatorclient.MockCoordinatorClient
	spaceService *mock_nodespace.MockService
	storage      nodestorage.NodeStorage
	nodesync     *mock_nodesync.MockNodeSync
	deleter      *spaceDeleter
	waiterChan   chan struct{}
	ctrl         *gomock.Controller
	dir          string
	app          *app.App
}

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

func newSpaceDeleterFixture(t *testing.T) *spaceDeleterFixture {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	a := new(app.App)
	ctrl := gomock.NewController(t)
	waiterChan := make(chan struct{})
	coordClient := mock_coordinatorclient.NewMockCoordinatorClient(ctrl)
	spaceService := mock_nodespace.NewMockService(ctrl)
	nodeSync := mock_nodesync.NewMockNodeSync(ctrl)
	storage := nodestorage.New()
	coordClient.EXPECT().Name().Return(coordinatorclient.CName).AnyTimes()
	coordClient.EXPECT().Init(a).Return(nil).AnyTimes()
	spaceService.EXPECT().Name().Return(nodespace.CName).AnyTimes()
	spaceService.EXPECT().Init(a).Return(nil).AnyTimes()
	spaceService.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
	nodeSync.EXPECT().Name().Return(nodesync.CName).AnyTimes()
	nodeSync.EXPECT().Init(a).Return(nil).AnyTimes()
	nodeSync.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
	nodeSync.EXPECT().WaitSyncOnStart().Return(waiterChan).AnyTimes()
	deleter := New().(*spaceDeleter)

	a.Register(storeConfig(dir)).
		Register(coordClient).
		Register(storage).
		Register(spaceService).
		Register(nodeSync).
		Register(deleter)
	err = a.Start(context.Background())
	require.NoError(t, err)
	return &spaceDeleterFixture{
		coordClient:  coordClient,
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
	err := fx.app.Close(context.Background())
	require.NoError(t, err)
	fx.ctrl.Finish()
	err = os.RemoveAll(fx.dir)
	require.NoError(t, err)
}

func TestSpaceDeleter_Run_Ok(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	store, err := fx.storage.CreateSpaceStorage(spaceTestPayload())
	require.NoError(t, err)
	lg := mockDeletionLog(store.Id())
	close(fx.waiterChan)

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()
	space1 := mock_nodespace.NewMockNodeSpace(fx.ctrl)
	fx.spaceService.EXPECT().PickSpace(gomock.Any(), "space1").Return(space1, nil).AnyTimes()
	space1.EXPECT().SetIsDeleted(false)
	fx.spaceService.EXPECT().PickSpace(gomock.Any(), "space2").Return(nil, ocache.ErrNotExists).AnyTimes()
	store.Close(context.Background())

	time.Sleep(testSaveDelay)

	id, err := fx.storage.DeletionStorage().LastRecordId()
	require.NoError(t, err)
	require.Equal(t, lg[2].Id, id)
	fx.stop(t)
}

func TestSpaceDeleter_Run_OkNoStorage(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	lg := mockDeletionLog("space3")
	close(fx.waiterChan)

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()
	space1 := mock_nodespace.NewMockNodeSpace(fx.ctrl)
	fx.spaceService.EXPECT().PickSpace(gomock.Any(), "space1").Return(space1, nil).AnyTimes()
	space1.EXPECT().SetIsDeleted(false)
	fx.spaceService.EXPECT().PickSpace(gomock.Any(), "space2").Return(nil, ocache.ErrNotExists).AnyTimes()

	time.Sleep(testSaveDelay)

	id, err := fx.storage.DeletionStorage().LastRecordId()
	require.NoError(t, err)
	require.Equal(t, lg[2].Id, id)
	fx.stop(t)
}

func TestSpaceDeleter_Run_Failure(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	lg := mockDeletionLog("space3")
	close(fx.waiterChan)

	fx.coordClient.EXPECT().DeletionLog(gomock.Any(), "", logLimit).Return(lg, nil).AnyTimes()
	space1 := mock_nodespace.NewMockNodeSpace(fx.ctrl)
	fx.spaceService.EXPECT().PickSpace(gomock.Any(), "space1").Return(space1, nil).AnyTimes()
	space1.EXPECT().SetIsDeleted(false)
	fx.spaceService.EXPECT().PickSpace(gomock.Any(), "space2").Return(nil, fmt.Errorf("some system error")).AnyTimes()

	time.Sleep(testSaveDelay)

	id, err := fx.storage.DeletionStorage().LastRecordId()
	require.NoError(t, err)
	require.Equal(t, lg[0].Id, id)
	fx.stop(t)
}

func mockDeletionLog(realId string) []*coordinatorproto.DeletionLogRecord {
	return []*coordinatorproto.DeletionLogRecord{
		{Id: "1", SpaceId: "space1", Status: coordinatorproto.DeletionLogRecordStatus_Ok},
		{Id: "2", SpaceId: "space2", Status: coordinatorproto.DeletionLogRecordStatus_RemovePrepare},
		{Id: "3", SpaceId: realId, Status: coordinatorproto.DeletionLogRecordStatus_Remove},
	}
}
