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
	"github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/ldiff"
	"github.com/anytypeio/any-sync/net/rpc/rpctest"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var ctx = context.Background()

func TestNodeSync_Sync(t *testing.T) {
	t.Run("offline sync", func(t *testing.T) {
		fx := newFixture(t, 3)
		defer fx.Finish(t)
		assert.NoError(t, fx.Sync())
		stat := fx.NodeSync.(*nodeSync).syncStat
		partsErr := stat.PartsErrors.Load()
		assert.NotEmpty(t, partsErr)
		assert.Equal(t, partsErr, stat.PartsTotal.Load())
		assert.Equal(t, partsErr, stat.PartsHandled.Load())
	})
	t.Run("partial sync", func(t *testing.T) {
		nodeServ := testnodeconf.GenNodeConfig(2)
		acc1 := nodeServ.GetAccountService(0)
		fx1 := newFixtureWithNodeConf(t, acc1, nodeServ)
		defer fx1.Finish(t)
		acc2 := nodeServ.GetAccountService(1)
		fx2 := newFixtureWithNodeConf(t, acc2, nodeServ)
		defer fx2.Finish(t)
		fx1.tp = fx1.tp.WithServer(fx2.ts)
		emptyLdiff := ldiff.New(8, 8)

		ld1 := ldiff.New(8, 8)
		ld2 := ldiff.New(8, 8)

		ld1.Set(
			ldiff.Element{
				Id: "sameId",
			}, ldiff.Element{
				Id:   "spaceA",
				Head: "versionA",
			}, ldiff.Element{
				Id: "ld1Only",
			},
		)

		ld2.Set(
			ldiff.Element{
				Id: "sameId",
			}, ldiff.Element{
				Id:   "spaceA",
				Head: "versionB",
			}, ldiff.Element{
				Id: "ld2Only",
			},
		)

		for i := 0; i < nodeconf.PartitionCount; i++ {
			if i == 0 {
				fx1.nodeHead.EXPECT().LDiff(i).Return(ld1)
				fx2.nodeHead.EXPECT().LDiff(i).Return(ld2)
			} else {
				fx1.nodeHead.EXPECT().LDiff(i).Return(emptyLdiff)
				fx2.nodeHead.EXPECT().LDiff(i).Return(emptyLdiff)
			}
		}

		// cold update for ld2Only
		fx1.coldSync.EXPECT().Sync(gomock.Any(), "ld2Only", acc2.Account().PeerId)
		fx1.nodeHead.EXPECT().ReloadHeadFromStore("ld2Only").Return(nil)

		// hot update for spaceA
		fx1.nodeSpace.EXPECT().GetSpace(gomock.Any(), "spaceA").Return(nil, nil)

		assert.NoError(t, fx1.Sync())
	})
}

func TestNodeSync_getRelatePartitions(t *testing.T) {
	fx := newFixture(t, 8)
	defer fx.Finish(t)
	st := time.Now()
	parts, err := fx.NodeSync.(*nodeSync).getRelatePartitions()
	require.NoError(t, err)
	t.Log("parts", len(parts), time.Since(st))
	for _, p := range parts {
		assert.Len(t, p.peers, 2)
	}
}

func newFixtureWithNodeConf(t *testing.T, accServ accountservice.Service, confServ *testnodeconf.Config) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		ctrl:      ctrl,
		NodeSync:  New(),
		nodeHead:  mock_nodehead.NewMockNodeHead(ctrl),
		nodeSpace: mock_nodespace.NewMockService(ctrl),
		coldSync:  mock_coldsync.NewMockColdSync(ctrl),
		a:         new(app.App),
	}

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
	fx.tp = rpctest.NewTestPool()
	fx.ts = rpctest.NewTestServer()
	fx.a.Register(nodeconf.New()).
		Register(accServ).
		Register(&config{Config: confServ}).
		Register(fx.NodeSync).
		Register(fx.nodeHead).
		Register(fx.nodeSpace).
		Register(fx.coldSync).
		Register(fx.tp).
		Register(fx.ts)
	require.NoError(t, fx.a.Start(ctx))
	return fx
}
func newFixture(t *testing.T, nodeCount int) *fixture {
	confServ := testnodeconf.GenNodeConfig(nodeCount)
	return newFixtureWithNodeConf(t, confServ.GetAccountService(0), confServ)
}

type fixture struct {
	NodeSync
	a         *app.App
	ctrl      *gomock.Controller
	nodeHead  *mock_nodehead.MockNodeHead
	nodeSpace *mock_nodespace.MockService
	coldSync  *mock_coldsync.MockColdSync
	tp        *rpctest.TestPool
	ts        *rpctest.TesServer
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
}

type config struct {
	*testnodeconf.Config
}

func (c config) GetNodeSync() Config {
	return Config{
		SyncOnStart: false,
	}
}
