package nodehead

import (
	"context"
	"encoding/hex"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/ldiff"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/testnodeconf"
	"github.com/anyproto/go-chash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"math"
	"os"
	"testing"
)

var ctx = context.Background()

func TestNodeHead_Run(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fx := newFixture(t, tmpDir)
	store := fx.a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	ss, err := store.CreateSpaceStorage(spaceTestPayload())
	require.NoError(t, err)
	require.NoError(t, ss.WriteSpaceHash("123"))
	require.NoError(t, ss.Close(ctx))
	fx.Finish(t)

	fx = newFixture(t, tmpDir)
	defer fx.Finish(t)
	assert.Len(t, fx.NodeHead.(*nodeHead).partitions, 1)
}

func TestNodeHead_SetHead(t *testing.T) {
	fx := newFixture(t, "")
	defer fx.Finish(t)

	getHash := func(part int) string {
		res, err := fx.Ranges(ctx, part, []ldiff.Range{{
			From:  0,
			To:    math.MaxUint64,
			Limit: 1,
		}}, nil)
		require.NoError(t, err)
		require.Len(t, res, 1)
		return hex.EncodeToString(res[0].Hash)
	}

	t.Run("set head", func(t *testing.T) {
		part, err := fx.SetHead("2.2", "head")
		require.NoError(t, err)
		h1 := getHash(part)
		part2, err := fx.SetHead("3.2", "head")
		assert.Equal(t, part, part2)
		h2 := getHash(part)
		assert.NotEqual(t, h1, h2)
	})

}

func TestNodeHead_Ranges(t *testing.T) {
	fx := newFixture(t, "")
	defer fx.Finish(t)

	t.Run("partition not exists", func(t *testing.T) {
		res, err := fx.Ranges(ctx, 1, []ldiff.Range{{
			From:  0,
			To:    math.MaxUint64,
			Limit: 1,
		}}, nil)
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, 0, res[0].Count)
	})
}

func TestNodeHead_GetSpaceHash(t *testing.T) {
	fx := newFixture(t, "")
	defer fx.Finish(t)
	hash := "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
	_, err := fx.SetHead("space1", hash)
	require.NoError(t, err)

	head, err := fx.GetHead("space1")
	require.NoError(t, err)
	assert.Equal(t, hash, head)

	_, err = fx.GetHead("not found")
	assert.Equal(t, ErrSpaceNotFound, err)
}

func newFixture(t *testing.T, dataPath string) *fixture {
	var tmpDir string
	if dataPath != "" {
		tmpDir = dataPath
	} else {
		var err error
		tmpDir, err = os.MkdirTemp("", "")
		require.NoError(t, err)
	}
	ctrl := gomock.NewController(t)
	fx := &fixture{
		NodeHead:      New(),
		a:             new(app.App),
		dataPath:      tmpDir,
		forceDataPath: dataPath != "",
		nodeConf:      mock_nodeconf.NewMockService(ctrl),
		ctrl:          ctrl,
	}
	confServ := testnodeconf.GenNodeConfig(3)
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
	fx.nodeConf.EXPECT().Partition(gomock.Any()).DoAndReturn(func(spaceId string) int {
		return ch.GetPartition(nodeconf.ReplKey(spaceId))
	}).AnyTimes()

	fx.a.Register(&config{Config: confServ, dataPath: tmpDir}).
		Register(fx.nodeConf).
		Register(confServ.GetAccountService(0)).
		Register(nodestorage.New()).
		Register(fx.NodeHead)

	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	NodeHead
	a             *app.App
	dataPath      string
	forceDataPath bool
	ctrl          *gomock.Controller
	nodeConf      *mock_nodeconf.MockService
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	if !fx.forceDataPath && fx.dataPath != "" {
		_ = os.RemoveAll(fx.dataPath)
	}
}

type config struct {
	*testnodeconf.Config
	dataPath string
}

func (c *config) GetStorage() nodestorage.Config {
	return nodestorage.Config{
		Path: c.dataPath,
	}
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

type member struct {
	id string
}

func (m member) Id() string {
	return m.id
}

func (m member) Capacity() float64 {
	return 1
}
