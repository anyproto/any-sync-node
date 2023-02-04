package nodesync

import (
	"context"
	"github.com/anytypeio/any-sync-node/nodehead"
	"github.com/anytypeio/any-sync-node/nodehead/mock_nodehead"
	"github.com/anytypeio/any-sync-node/nodespace"
	"github.com/anytypeio/any-sync-node/nodespace/mock_nodespace"
	"github.com/anytypeio/any-sync-node/nodesync/coldsync"
	"github.com/anytypeio/any-sync-node/nodesync/coldsync/mock_coldsync"
	"github.com/anytypeio/any-sync-node/testutil/testnodeconf"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/net/rpc/rpctest"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var ctx = context.Background()

func TestNodeSync_getRelatePartitions(t *testing.T) {
	fx := newFixture(t)
	defer fx.Finish(t)
	st := time.Now()
	parts, err := fx.NodeSync.(*nodeSync).getRelatePartitions()
	require.NoError(t, err)
	t.Log("parts", len(parts), time.Since(st))
	for _, p := range parts {
		assert.Len(t, p.peers, 2)
	}
}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		ctrl:      ctrl,
		NodeSync:  New(),
		nodeHead:  mock_nodehead.NewMockNodeHead(ctrl),
		nodeSpace: mock_nodespace.NewMockService(ctrl),
		coldSync:  mock_coldsync.NewMockColdSync(ctrl),
		a:         new(app.App),
	}
	accServ, confServ := testnodeconf.GenNodeConfig(9)
	fx.nodeHead.EXPECT().Name().Return(nodehead.CName).AnyTimes()
	fx.nodeHead.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.nodeHead.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.nodeHead.EXPECT().Close(gomock.Any()).AnyTimes()
	fx.nodeSpace.EXPECT().Name().Return(nodespace.CName).AnyTimes()
	fx.nodeSpace.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.nodeSpace.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.nodeSpace.EXPECT().Close(gomock.Any()).AnyTimes()
	fx.coldSync.EXPECT().Name().Return(coldsync.CName).AnyTimes()
	fx.coldSync.EXPECT().Init(gomock.Any()).AnyTimes()
	tp := rpctest.NewTestPool()
	ts := rpctest.NewTestServer()
	fx.a.Register(nodeconf.New()).
		Register(accServ).
		Register(&config{Config: confServ}).
		Register(fx.NodeSync).
		Register(fx.nodeHead).
		Register(fx.nodeSpace).
		Register(fx.coldSync).
		Register(tp).
		Register(ts)
	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	NodeSync
	a         *app.App
	ctrl      *gomock.Controller
	nodeHead  *mock_nodehead.MockNodeHead
	nodeSpace *mock_nodespace.MockService
	coldSync  *mock_coldsync.MockColdSync
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
}

type config struct {
	*testnodeconf.Config
}

func (c config) GetNodeSync() Config {
	return Config{
		SyncOnStart: true,
	}
}
