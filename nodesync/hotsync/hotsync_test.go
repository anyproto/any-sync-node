package hotsync

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app/ocache"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/nodespace/mock_nodespace"
)

type space struct {
	id string
}

func newSpace(id string) *space {
	return &space{id}
}

func (s *space) Id() string {
	return s.id
}

func (s *space) Close() (err error) {
	return nil
}

func (s *space) TryClose(objectTTL time.Duration) (res bool, err error) {
	return false, nil
}

type fixture struct {
	ctrl             *gomock.Controller
	hotSync          *hotSync
	mockSpaceService *mock_nodespace.MockService
	cache            ocache.OCache
}

func (fx *fixture) stop() {
	fx.ctrl.Finish()
}

func newFixture(t *testing.T, simReq int) *fixture {
	ctrl := gomock.NewController(t)
	mockSpaceService := mock_nodespace.NewMockService(ctrl)

	sync := &hotSync{}
	sync.SetMetric(&atomic.Uint32{}, &atomic.Uint32{})
	sync.simultaneousSync = simReq
	sync.spaceService = mockSpaceService
	sync.syncQueue = map[string]struct{}{}
	cache := ocache.New(func(ctx context.Context, id string) (value ocache.Object, err error) {
		return newSpace(id), nil
	})
	return &fixture{
		cache:            cache,
		hotSync:          sync,
		mockSpaceService: mockSpaceService,
		ctrl:             ctrl,
	}
}

func TestHotSync_UpdateQueue(t *testing.T) {
	fx := newFixture(t, 10)
	defer fx.stop()
	fx.hotSync.UpdateQueue([]string{"b"})
	require.Equal(t, []string{"b"}, fx.hotSync.spaceQueue)
	fx.hotSync.UpdateQueue([]string{"a", "b", "c"})
	require.Equal(t, []string{"b", "a", "c"}, fx.hotSync.spaceQueue)
	fx.hotSync.UpdateQueue([]string{"d", "e"})
	require.Equal(t, []string{"b", "a", "c", "d", "e"}, fx.hotSync.spaceQueue)
}

func TestHotSync_checkCache(t *testing.T) {
	t.Run("exceed capacity", func(t *testing.T) {
		fx := newFixture(t, 3)
		defer fx.stop()
		fx.mockSpaceService.EXPECT().Cache().Return(fx.cache).AnyTimes()
		fx.mockSpaceService.EXPECT().GetSpace(gomock.Any(), gomock.Any()).Return(nil, nil)
		fx.cache.Add("a", newSpace("a"))
		fx.cache.Add("b", newSpace("b"))
		fx.hotSync.syncQueue["a"] = struct{}{}
		fx.hotSync.syncQueue["b"] = struct{}{}
		fx.hotSync.syncQueue["c"] = struct{}{}
		fx.hotSync.spaceQueue = []string{"d", "e"}

		err := fx.hotSync.checkCache(context.Background())
		require.NoError(t, err)
		require.Equal(t, []string{"e"}, fx.hotSync.spaceQueue)
		require.Contains(t, fx.hotSync.syncQueue, "d")
		require.NotContains(t, fx.hotSync.syncQueue, "e")
	})
	t.Run("exceed capacity space not found", func(t *testing.T) {
		fx := newFixture(t, 3)
		defer fx.stop()
		fx.mockSpaceService.EXPECT().Cache().Return(fx.cache).AnyTimes()
		fx.mockSpaceService.EXPECT().GetSpace(gomock.Any(), "d").Return(nil, fmt.Errorf("some error"))
		fx.cache.Add("a", newSpace("a"))
		fx.cache.Add("b", newSpace("b"))
		fx.hotSync.syncQueue["a"] = struct{}{}
		fx.hotSync.syncQueue["b"] = struct{}{}
		fx.hotSync.syncQueue["c"] = struct{}{}
		fx.hotSync.spaceQueue = []string{"d", "e"}

		err := fx.hotSync.checkCache(context.Background())
		require.NoError(t, err)
		require.Equal(t, []string{"e"}, fx.hotSync.spaceQueue)
		require.NotContains(t, fx.hotSync.syncQueue, "d")
		require.NotContains(t, fx.hotSync.syncQueue, "e")
	})
	t.Run("empty space queue", func(t *testing.T) {
		fx := newFixture(t, 3)
		defer fx.stop()
		fx.mockSpaceService.EXPECT().Cache().Return(fx.cache).AnyTimes()
		fx.cache.Add("a", newSpace("a"))
		fx.cache.Add("b", newSpace("b"))
		fx.hotSync.syncQueue["a"] = struct{}{}
		fx.hotSync.syncQueue["b"] = struct{}{}
		fx.hotSync.syncQueue["c"] = struct{}{}

		err := fx.hotSync.checkCache(context.Background())
		require.NoError(t, err)
		require.NotContains(t, fx.hotSync.syncQueue, "c")
	})
	t.Run("empty space queue then update", func(t *testing.T) {
		fx := newFixture(t, 3)
		defer fx.stop()
		fx.mockSpaceService.EXPECT().Cache().Return(fx.cache).AnyTimes()
		fx.mockSpaceService.EXPECT().GetSpace(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
		fx.cache.Add("a", newSpace("a"))
		fx.cache.Add("b", newSpace("b"))
		fx.hotSync.syncQueue["a"] = struct{}{}
		fx.hotSync.syncQueue["b"] = struct{}{}
		fx.hotSync.syncQueue["c"] = struct{}{}

		err := fx.hotSync.checkCache(context.Background())
		require.NoError(t, err)
		require.NotContains(t, fx.hotSync.syncQueue, "c")
		fx.hotSync.UpdateQueue([]string{"d", "e"})
		fx.cache.Remove(context.Background(), "b")
		err = fx.hotSync.checkCache(context.Background())
		require.NoError(t, err)
		require.Empty(t, fx.hotSync.spaceQueue)
		require.Contains(t, fx.hotSync.syncQueue, "d")
		require.Contains(t, fx.hotSync.syncQueue, "e")
		require.Len(t, fx.hotSync.syncQueue, 3)
	})
}
