package spacedeleter

import (
	"context"
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
	"os"
	"testing"
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
	nodeSync.EXPECT().WaitSyncOnStart().Return(make(chan struct{})).AnyTimes()
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

func (fx *spaceDeleterFixture) stop(t *testing.T) {
	fx.nodesync.EXPECT().Close(gomock.Any()).Return(nil).AnyTimes()
	fx.spaceService.EXPECT().Close(gomock.Any()).Return(nil).AnyTimes()
	err := fx.app.Close(context.Background())
	require.NoError(t, err)
	fx.ctrl.Finish()
	err = os.RemoveAll(fx.dir)
	require.NoError(t, err)
}

func TestSpaceDeleter_Run(t *testing.T) {
	fx := newSpaceDeleterFixture(t)
	defer fx.stop(t)
}
