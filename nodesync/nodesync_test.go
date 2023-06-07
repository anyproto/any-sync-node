package nodesync

import (
	"context"
	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodehead/mock_nodehead"
	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodespace/mock_nodespace"
	"github.com/anyproto/any-sync-node/nodesync/coldsync"
	"github.com/anyproto/any-sync-node/nodesync/coldsync/mock_coldsync"
	"github.com/anyproto/any-sync-node/nodesync/hotsync"
	"github.com/anyproto/any-sync-node/nodesync/hotsync/mock_hotsync"
	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/ldiff"
	"github.com/anyproto/any-sync/net"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/testnodeconf"
	"github.com/anyproto/go-chash"
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

		mcS, mcC := rpctest.MultiConnPair(acc2.Account().PeerId, acc1.Account().PeerId)
		pS, err := peer.NewPeer(mcS, fx1.ts)
		require.NoError(t, err)
		_, err = peer.NewPeer(mcC, fx2.ts)
		require.NoError(t, err)
		fx1.tp.AddPeer(ctx, pS)
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
		fx1.hotSync.EXPECT().UpdateQueue([]string{"spaceA"})
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
		hotSync:   mock_hotsync.NewMockHotSync(ctrl),
		nodeConf:  mock_nodeconf.NewMockService(ctrl),
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

	fx.hotSync.EXPECT().Name().Return(hotsync.CName).AnyTimes()
	fx.hotSync.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.hotSync.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.hotSync.EXPECT().Close(gomock.Any()).AnyTimes()
	fx.hotSync.EXPECT().SetMetric(gomock.Any(), gomock.Any()).AnyTimes()

	fx.nodeConf.EXPECT().Name().Return(nodeconf.CName).AnyTimes()
	fx.nodeConf.EXPECT().Init(fx.a).AnyTimes()
	fx.nodeConf.EXPECT().Run(ctx).AnyTimes()
	fx.nodeConf.EXPECT().Close(ctx).AnyTimes()
	ch, _ := chash.New(chash.Config{
		PartitionCount:    3000,
		ReplicationFactor: 3,
	})
	for _, n := range confServ.GetNodeConf().Nodes {
		require.NoError(t, ch.AddMembers(member{n.PeerId}))
	}
	fx.nodeConf.EXPECT().CHash().AnyTimes().Return(ch)

	fx.tp = rpctest.NewTestPool()
	fx.ts = server.New()
	fx.a.Register(fx.nodeConf).
		Register(fx.ts).
		Register(accServ).
		Register(&config{Config: confServ}).
		Register(fx.NodeSync).
		Register(fx.nodeHead).
		Register(fx.nodeSpace).
		Register(fx.coldSync).
		Register(fx.hotSync).
		Register(fx.tp)
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
	hotSync   *mock_hotsync.MockHotSync
	nodeConf  *mock_nodeconf.MockService
	tp        *rpctest.TestPool
	ts        server.DRPCServer
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

func (c config) GetNet() net.Config {
	return net.Config{
		Stream: net.StreamConfig{MaxMsgSizeMb: 10},
	}
}

type member struct {
	id string
}

func (m member) Id() string {
	return m.id
}

func (m member) Capacity() float64 {
	return 1
}
