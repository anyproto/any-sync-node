package pubsubrelay

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/net/pool/mock_pool"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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
	// NodeIds includes self ("self") plus two other responsible nodes
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
