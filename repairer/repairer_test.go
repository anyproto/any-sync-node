package repairer

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodestorage/mock_nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/coldsync"
	"github.com/anyproto/any-sync-node/nodesync/coldsync/mock_coldsync"
)

var ctx = context.Background()

const spaceId = "err.space"

func TestRepairer_RepairSpace(t *testing.T) {
	t.Run("transient error repaired in place", func(t *testing.T) {
		fx := newFixture(t)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError, NewHash: "someHash"}, nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusOk, "").Return(nil)
		fx.storage.EXPECT().SpaceExists(spaceId).Return(true)
		fx.storage.EXPECT().IndexSpace(gomock.Any(), spaceId, true).Return(nil, nil)

		require.NoError(t, fx.repairSpace(spaceId))
		assert.Equal(t, uint32(1), fx.stat.repairedInPlace.Load())
	})
	t.Run("corrupted db quarantined and pulled from a neighbor", func(t *testing.T) {
		fx := newFixture(t)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError, NewHash: "someHash"}, nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusOk, "").Return(nil)
		fx.storage.EXPECT().SpaceExists(spaceId).Return(true)
		// in-place validation fails: db is corrupted
		fx.storage.EXPECT().IndexSpace(gomock.Any(), spaceId, true).Return(nil, errors.New("malformed database"))
		fx.storage.EXPECT().QuarantineSpace(gomock.Any(), spaceId).Return("/quarantine/err.space-1", nil)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2"})
		// first peer fails, second delivers
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p1").Return(errors.New("unreachable"))
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p2").Return(nil)
		fx.storage.EXPECT().IndexSpace(gomock.Any(), spaceId, true).Return(nil, nil)

		require.NoError(t, fx.repairSpace(spaceId))
		assert.Equal(t, uint32(1), fx.stat.quarantined.Load())
		assert.Equal(t, uint32(1), fx.stat.repaired.Load())
	})
	t.Run("missing data pulled without quarantine", func(t *testing.T) {
		fx := newFixture(t)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError, NewHash: "someHash"}, nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusOk, "").Return(nil)
		fx.storage.EXPECT().SpaceExists(spaceId).Return(false)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1"})
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p1").Return(nil)
		fx.storage.EXPECT().IndexSpace(gomock.Any(), spaceId, true).Return(nil, nil)

		require.NoError(t, fx.repairSpace(spaceId))
		assert.Equal(t, uint32(1), fx.stat.repaired.Load())
	})
	t.Run("no valid copy anywhere: status flips back to Error", func(t *testing.T) {
		fx := newFixture(t)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError, NewHash: "someHash"}, nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusOk, "").Return(nil)
		fx.storage.EXPECT().SpaceExists(spaceId).Return(false)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2"})
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p1").Return(errors.New("unreachable"))
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p2").Return(errors.New("unreachable"))
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusError, "").Return(nil)

		err := fx.repairSpace(spaceId)
		assert.ErrorIs(t, err, errNoValidCopy)
	})
	t.Run("never existed anywhere: entry garbage-collected", func(t *testing.T) {
		fx := newFixture(t)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError}, nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusOk, "").Return(nil)
		fx.storage.EXPECT().SpaceExists(spaceId).Return(false)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2"})
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p1").Return(spacesyncproto.ErrSpaceMissing)
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p2").Return(spacesyncproto.ErrSpaceMissing)
		fx.indexStorage.EXPECT().DeleteSpaceEntry(gomock.Any(), spaceId).Return(nil)

		require.NoError(t, fx.repairSpace(spaceId))
		assert.Equal(t, uint32(1), fx.stat.droppedEntries.Load())
	})
	t.Run("missing everywhere but heads were recorded: keep Error", func(t *testing.T) {
		fx := newFixture(t)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError, NewHash: "realHeads"}, nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusOk, "").Return(nil)
		fx.storage.EXPECT().SpaceExists(spaceId).Return(false)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1", "p2"})
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p1").Return(spacesyncproto.ErrSpaceMissing)
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p2").Return(spacesyncproto.ErrSpaceMissing)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusError, "").Return(nil)

		assert.ErrorIs(t, fx.repairSpace(spaceId), errNoValidCopy)
	})
	t.Run("pulled copy fails validation: status flips back to Error", func(t *testing.T) {
		fx := newFixture(t)
		fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError, NewHash: "someHash"}, nil)
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusOk, "").Return(nil)
		fx.storage.EXPECT().SpaceExists(spaceId).Return(false)
		fx.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"p1"})
		fx.coldSync.EXPECT().Sync(gomock.Any(), spaceId, "p1").Return(nil)
		fx.storage.EXPECT().IndexSpace(gomock.Any(), spaceId, true).Return(nil, errors.New("malformed database"))
		fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusError, "").Return(nil)

		assert.Error(t, fx.repairSpace(spaceId))
	})
}

func TestRepairer_RepairCycle(t *testing.T) {
	fx := newFixture(t)
	// two errored spaces: one responsible (repairable), one not (operator territory)
	fx.indexStorage.EXPECT().ReadSpacesByStatus(gomock.Any(), nodestorage.SpaceStatusError, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ nodestorage.SpaceStatus, iter func(string) (bool, error)) error {
			_, _ = iter(spaceId)
			_, _ = iter("foreign.space")
			return nil
		})
	fx.nodeConf.EXPECT().IsResponsible(spaceId).Return(true)
	fx.nodeConf.EXPECT().IsResponsible("foreign.space").Return(false)
	// repairSpace path for the responsible one
	fx.indexStorage.EXPECT().SpaceStatusEntry(gomock.Any(), spaceId).Return(nodestorage.SpaceStatusEntry{Status: nodestorage.SpaceStatusError, NewHash: "someHash"}, nil)
	fx.indexStorage.EXPECT().SetSpaceStatus(gomock.Any(), spaceId, nodestorage.SpaceStatusOk, "").Return(nil)
	fx.storage.EXPECT().SpaceExists(spaceId).Return(true)
	fx.storage.EXPECT().IndexSpace(gomock.Any(), spaceId, true).Return(nil, nil)

	fx.repairCycle()
	assert.Equal(t, uint32(0), fx.stat.errored.Load())
	assert.Equal(t, uint32(1), fx.stat.repairedInPlace.Load())
}

type fixture struct {
	*repairer
	a            *app.App
	storage      *mock_nodestorage.MockNodeStorage
	indexStorage *mock_nodestorage.MockIndexStorage
	nodeConf     *mock_nodeconf.MockService
	coldSync     *mock_coldsync.MockColdSync
}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		repairer:     &repairer{disableLoop: true},
		a:            new(app.App),
		storage:      mock_nodestorage.NewMockNodeStorage(ctrl),
		indexStorage: mock_nodestorage.NewMockIndexStorage(ctrl),
		nodeConf:     mock_nodeconf.NewMockService(ctrl),
		coldSync:     mock_coldsync.NewMockColdSync(ctrl),
	}
	anymock.ExpectComp(fx.storage.EXPECT(), nodestorage.CName)
	anymock.ExpectComp(fx.nodeConf.EXPECT(), nodeconf.CName)
	anymock.ExpectComp(fx.coldSync.EXPECT(), coldsync.CName)
	fx.storage.EXPECT().IndexStorage().AnyTimes().Return(fx.indexStorage)

	fx.a.Register(fx.storage).
		Register(fx.nodeConf).
		Register(fx.coldSync).
		Register(fx.repairer)

	require.NoError(t, fx.a.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, fx.a.Close(ctx))
		ctrl.Finish()
	})
	return fx
}
