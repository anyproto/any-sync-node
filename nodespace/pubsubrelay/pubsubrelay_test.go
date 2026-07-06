package pubsubrelay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/pool/mock_pool"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodespace/mock_nodespace"
)

func newRelay(t *testing.T) (*relay, *mock_nodeconf.MockService, *mock_pool.MockService) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	nc := mock_nodeconf.NewMockService(ctrl)
	p := mock_pool.NewMockService(ctrl)
	r := &relay{
		nodeconf:   nc,
		pool:       p,
		selfPeerId: "self",
	}
	return r, nc, p
}

func TestRelayIsResponsible(t *testing.T) {
	r, nc, _ := newRelay(t)
	nc.EXPECT().IsResponsible("space1").Return(true)
	nc.EXPECT().IsResponsible("space2").Return(false)
	assert.True(t, r.IsResponsible("space1"))
	assert.False(t, r.IsResponsible("space2"))
}

func TestRelayIsResponsibleNode(t *testing.T) {
	r, nc, _ := newRelay(t)
	nc.EXPECT().NodeIds("space1").Return([]string{"nodeA", "nodeB"}).AnyTimes()
	assert.True(t, r.IsResponsibleNode("space1", "nodeA"))
	assert.True(t, r.IsResponsibleNode("space1", "nodeB"))
	assert.False(t, r.IsResponsibleNode("space1", "clientX"))
}

func TestRelayOtherResponsiblePeersExcludesSelf(t *testing.T) {
	r, nc, p := newRelay(t)
	// nodeconf.NodeIds already excludes self, but the relay also self-skips
	// defensively; this stub includes "self" to exercise that guard.
	nc.EXPECT().NodeIds("space1").Return([]string{"self", "nodeA", "nodeB"})
	pa := rpctest.MockPeer{}
	pb := rpctest.MockPeer{}
	p.EXPECT().Get(gomock.Any(), "nodeA").Return(pa, nil)
	p.EXPECT().Get(gomock.Any(), "nodeB").Return(pb, nil)
	// self is never dialed

	peers, err := r.OtherResponsiblePeers(context.Background(), "space1")
	require.NoError(t, err)
	require.Len(t, peers, 2)
}

func TestRelayOtherResponsiblePeersSkipsUndialable(t *testing.T) {
	r, nc, p := newRelay(t)
	nc.EXPECT().NodeIds("space1").Return([]string{"nodeA", "nodeB"})
	pb := rpctest.MockPeer{}
	p.EXPECT().Get(gomock.Any(), "nodeA").Return(nil, errors.New("unreachable"))
	p.EXPECT().Get(gomock.Any(), "nodeB").Return(pb, nil)

	peers, err := r.OtherResponsiblePeers(context.Background(), "space1")
	require.NoError(t, err)
	require.Len(t, peers, 1, "unreachable node is skipped, not fatal")
}

func TestRelayOtherResponsiblePeersSingleNode(t *testing.T) {
	r, nc, _ := newRelay(t)
	// only self is responsible (small network): empty result, no error
	nc.EXPECT().NodeIds("space1").Return([]string{"self"})
	peers, err := r.OtherResponsiblePeers(context.Background(), "space1")
	require.NoError(t, err)
	require.Empty(t, peers)
}

// TestOnAclUpdateNonBlocking is the regression test for the consensus-mutex
// deadlock: onAclUpdate runs on the consensus stream reader while it holds the
// consensusclient mutex, so it must only enqueue — never touch the space cache
// synchronously (which would deadlock against Watch/UnWatch). With no worker
// running, the space service must not be called at all.
func TestOnAclUpdateNonBlocking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ns := mock_nodespace.NewMockService(ctrl)
	// no PickSpace/GetSpace expectations: any synchronous cache access fails the test
	r := &relay{
		nodeSpace:    ns,
		revalPending: make(map[string]struct{}),
		revalWake:    make(chan struct{}, 1),
	}

	done := make(chan struct{})
	go func() {
		r.onAclUpdate("space1")
		r.onAclUpdate("space1") // dedup
		r.onAclUpdate("space2")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("onAclUpdate blocked — must be non-blocking on the consensus goroutine")
	}

	r.revalMu.Lock()
	defer r.revalMu.Unlock()
	require.Len(t, r.revalPending, 2)
	require.Contains(t, r.revalPending, "space1")
	require.Contains(t, r.revalPending, "space2")
}

// TestRevalidateSpaceMissIsNoop verifies a PickSpace miss (space not hosted /
// closing) is a no-op that never resurrects the space or touches the engine.
func TestRevalidateSpaceMissIsNoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ns := mock_nodespace.NewMockService(ctrl)
	ns.EXPECT().PickSpace(gomock.Any(), "space1").Return(nil, errors.New("not found"))
	r := &relay{
		nodeSpace: ns,
		// engine intentionally nil: a miss must return before touching it
	}
	r.ctx, r.ctxCancel = context.WithCancel(context.Background())
	defer r.ctxCancel()
	require.NotPanics(t, func() { r.revalidateSpace("space1") })
}

// TestRevalidateWorkerDrainsQueue verifies the worker drains enqueued spaceIds
// and calls PickSpace off the enqueuing goroutine.
func TestRevalidateWorkerDrainsQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ns := mock_nodespace.NewMockService(ctrl)
	picked := make(chan string, 1)
	ns.EXPECT().PickSpace(gomock.Any(), "space1").DoAndReturn(
		func(_ context.Context, id string) (nodespace.NodeSpace, error) {
			picked <- id
			return nil, errors.New("miss")
		})
	r := &relay{
		nodeSpace:    ns,
		revalPending: make(map[string]struct{}),
		revalWake:    make(chan struct{}, 1),
	}
	r.ctx, r.ctxCancel = context.WithCancel(context.Background())
	r.workerWG.Add(1)
	go r.revalidateWorker()
	defer func() { r.ctxCancel(); r.workerWG.Wait() }()

	r.onAclUpdate("space1")
	select {
	case id := <-picked:
		require.Equal(t, "space1", id)
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not drain the queue")
	}
}
