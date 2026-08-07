package resharder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/anyproto/go-chash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/archive"
	"github.com/anyproto/any-sync-node/archive/adopter"
	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodehead/mock_nodehead"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/hotsync"
	"github.com/anyproto/any-sync-node/nodesync/hotsync/mock_hotsync"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

const scenarioNetworkId = "testnet"

// TestScenario_AddNode covers the "add new node(s)" operation end to end over
// real chash rings: nodes A,B,C hold every space (3 nodes, RF=3); node D is
// added in epoch 2. Every node that lost a partition to D hands the affected
// spaces off (2 surviving owners ACK by heads, D adopts the snapshot) and
// deletes its copy; spaces whose ownership did not change stay untouched.
func TestScenario_AddNode(t *testing.T) {
	net := newScenarioNet(t, "A", "B", "C", "D")
	net.setTreeNodes(1, "A", "B", "C")

	spaceIds := scenarioSpaceIds(12)
	for _, id := range spaceIds {
		// with 3 tree nodes and RF=3 every node owns every space
		for _, n := range []string{"A", "B", "C"} {
			net.createSpace(t, n, id)
		}
	}

	// epoch 2: node D joins
	net.setTreeNodes(2, "A", "B", "C", "D")
	for _, n := range net.order {
		net.nodes[n].resharder.drainCycle()
	}

	var movedToD int
	for _, id := range spaceIds {
		owners := net.owners(id)
		for _, n := range []string{"A", "B", "C"} {
			node := net.nodes[n]
			status, err := node.storage.IndexStorage().SpaceStatus(ctx, id)
			require.NoError(t, err)
			if owners[n] {
				assert.Equal(t, nodestorage.SpaceStatusOk, status, "space %s must stay on owner %s", id, n)
				assert.True(t, node.storage.SpaceExists(id))
			} else {
				assert.Equal(t, nodestorage.SpaceStatusMoved, status, "space %s must be drained from %s", id, n)
				assert.False(t, node.storage.SpaceExists(id))
				assert.False(t, net.bucket.has(n+"/"+id), "drained node %s must not keep the archive object of %s", n, id)
			}
		}
		if owners["D"] {
			movedToD++
			entry, err := net.nodes["D"].storage.IndexStorage().SpaceStatusEntry(ctx, id)
			require.NoError(t, err)
			// the eager restore may already have flipped Archived -> Ok
			assert.Contains(t, []nodestorage.SpaceStatus{nodestorage.SpaceStatusArchived, nodestorage.SpaceStatusOk}, entry.Status)
			assert.Equal(t, "new-"+id, entry.NewHash, "space %s adopted by D with the advertised heads", id)
			assert.True(t, net.bucket.has("D/"+id), "space %s must be parked in D's prefix", id)
		}
	}
	require.Greater(t, movedToD, 0, "adding a node must move some partitions to it")
}

// TestScenario_RemoveNode covers the "remove node(s)" operation: nodes
// A,B,C,D hold spaces per the epoch-1 ring; epoch 2 removes D entirely.
// D hands every space it still holds to the current owners — receivers
// authenticate it through the retained configuration history — and ends up
// holding nothing; the member that was not an owner before adopts the space.
func TestScenario_RemoveNode(t *testing.T) {
	net := newScenarioNet(t, "A", "B", "C", "D")
	net.setTreeNodes(1, "A", "B", "C", "D")

	spaceIds := scenarioSpaceIds(12)
	for _, id := range spaceIds {
		for n := range net.owners(id) {
			net.createSpace(t, n, id)
		}
	}

	// epoch 2: node D is removed from the configuration
	net.setTreeNodes(2, "A", "B", "C")
	for _, n := range net.order {
		net.nodes[n].resharder.drainCycle()
	}

	nodeD := net.nodes["D"]
	for _, id := range spaceIds {
		statusD, err := nodeD.storage.IndexStorage().SpaceStatus(ctx, id)
		if err == nil && statusD != nodestorage.SpaceStatusOk {
			// D held this space in epoch 1: it must be fully drained
			assert.Equal(t, nodestorage.SpaceStatusMoved, statusD, "space %s must be drained from removed node D", id)
			assert.False(t, nodeD.storage.SpaceExists(id))
			assert.False(t, net.bucket.has("D/"+id))
		}
		// every space must now be present on all remaining owners
		for _, n := range []string{"A", "B", "C"} {
			entry, err := net.nodes[n].storage.IndexStorage().SpaceStatusEntry(ctx, id)
			require.NoError(t, err, "space %s must have an index entry on %s", id, n)
			assert.Contains(t,
				[]nodestorage.SpaceStatus{nodestorage.SpaceStatusOk, nodestorage.SpaceStatusArchived},
				entry.Status, "space %s must be live or archived on %s", id, n)
			assert.Equal(t, "new-"+id, entry.NewHash)
		}
	}
	// D holds no data at all anymore
	dirs, err := nodeD.storage.AllSpaceIds()
	require.NoError(t, err)
	assert.Empty(t, dirs, "removed node must hold no space dirs")
}

func scenarioSpaceIds(n int) (ids []string) {
	for i := 0; i < n; i++ {
		ids = append(ids, fmt.Sprintf("scn%d.space%d", i, i))
	}
	return
}

// scenarioNet is a small in-process network: every node runs the real
// storage/archive/adopter/resharder stack over a shared in-memory bucket;
// topology comes from ring-backed nodeconf fakes sharing real chash rings.
type scenarioNet struct {
	t          *testing.T
	bucket     *memBucket
	order      []string
	nodes      map[string]*testNode
	confs      map[string]*ringConf
	historyRef *fakeConfHistory
	// ring is the current shared ring (same on every node in these tests)
	ring chash.CHash
}

func newScenarioNet(t *testing.T, names ...string) *scenarioNet {
	net := &scenarioNet{
		t:      t,
		bucket: newMemBucket(),
		order:  names,
		nodes:  map[string]*testNode{},
		confs:  map[string]*ringConf{},
	}
	history := newFakeConfHistory()
	for _, name := range names {
		conf := newRingConf(name)
		net.confs[name] = conf
		net.nodes[name] = newScenarioNode(t, name, net.bucket, conf, history)
	}
	// route adopt calls to the target node's adopter with the sender identity
	for _, name := range names {
		sender := name
		net.nodes[sender].resharder.adoptFn = func(_ context.Context, peerId string, req *nodesyncproto.AdoptArchiveRequest) (*nodesyncproto.AdoptArchiveResponse, error) {
			target, ok := net.nodes[peerId]
			if !ok {
				return nil, fmt.Errorf("unknown peer %s", peerId)
			}
			return target.adopter.AdoptArchive(peer.CtxWithPeerId(ctx, sender), req)
		}
	}
	net.historyRef = history
	return net
}

func (net *scenarioNet) setTreeNodes(epoch uint64, names ...string) {
	ring := newTreeRing(net.t, names)
	net.ring = ring
	conf := nodeconf.Configuration{
		Id:        fmt.Sprintf("cfg-%d", epoch),
		NetworkId: scenarioNetworkId,
		Epoch:     epoch,
	}
	for _, n := range names {
		conf.Nodes = append(conf.Nodes, nodeconf.Node{PeerId: n, Types: []nodeconf.NodeType{nodeconf.NodeTypeTree}})
	}
	net.historyRef.add(conf)
	for _, c := range net.confs {
		c.set(conf, ring)
	}
}

// owners returns the current owner set of a space.
func (net *scenarioNet) owners(spaceId string) map[string]bool {
	members := net.ring.GetMembers(nodeconf.ReplKey(spaceId))
	res := map[string]bool{}
	for _, m := range members {
		res[m.Id()] = true
	}
	return res
}

func (net *scenarioNet) createSpace(t *testing.T, nodeName, spaceId string) {
	node := net.nodes[nodeName]
	spaceDir := node.storage.StoreDir(spaceId)
	require.NoError(t, os.MkdirAll(spaceDir, 0o755))
	db, err := anystore.Open(ctx, filepath.Join(spaceDir, "store.db"), nil)
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	stateColl, err := db.Collection(ctx, "state")
	require.NoError(t, err)
	arena := &anyenc.Arena{}
	stateDoc := arena.NewObject()
	stateDoc.Set("id", arena.NewString(spaceId))
	stateDoc.Set("oh", arena.NewString("old-"+spaceId))
	stateDoc.Set("nh", arena.NewString("new-"+spaceId))
	require.NoError(t, stateColl.Insert(ctx, stateDoc))
	require.NoError(t, db.Close())
	require.NoError(t, node.storage.IndexStorage().UpdateHash(ctx, nodestorage.SpaceUpdate{
		SpaceId: spaceId, NewHash: "new-" + spaceId,
	}))
}

func newScenarioNode(t *testing.T, name string, bucket *memBucket, conf *ringConf, history *fakeConfHistory) *testNode {
	ctrl := gomock.NewController(t)
	n := &testNode{
		name:         name,
		a:            new(app.App),
		storage:      nodestorage.New(),
		archive:      archive.New(),
		archiveStore: newMemStore(bucket, name),
		adopter:      adopter.New(),
		resharder:    New().(*resharder),
	}
	nodeHead := mock_nodehead.NewMockNodeHead(ctrl)
	hotSync := mock_hotsync.NewMockHotSync(ctrl)
	anymock.ExpectComp(nodeHead.EXPECT(), nodehead.CName)
	anymock.ExpectComp(hotSync.EXPECT(), hotsync.CName)
	nodeHead.EXPECT().SetHead(gomock.Any(), gomock.Any()).AnyTimes().Return(0, nil)
	nodeHead.EXPECT().DeleteHeads(gomock.Any()).AnyTimes().Return(nil)
	hotSync.EXPECT().SetMetric(gomock.Any(), gomock.Any()).AnyTimes()
	hotSync.EXPECT().UpdateQueue(gomock.Any()).AnyTimes()

	n.a.Register(testNodeConfig{dir: t.TempDir()}).
		Register(&syncWaiterStub{}).
		Register(n.archiveStore).
		Register(n.archive).
		Register(n.storage).
		Register(nodeHead).
		Register(conf).
		Register(history).
		Register(hotSync).
		Register(rpctest.NewTestPool()).
		Register(n.adopter).
		Register(n.resharder)

	require.NoError(t, n.a.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, n.a.Close(ctx))
	})
	return n
}

// ringConf is a nodeconf.Service fake backed by a real chash ring.
type ringConf struct {
	self      string
	mu        sync.RWMutex
	conf      nodeconf.Configuration
	ring      chash.CHash
	observers []nodeconf.ChangeObserver
}

func newRingConf(self string) *ringConf {
	return &ringConf{self: self}
}

func (c *ringConf) set(conf nodeconf.Configuration, ring chash.CHash) {
	c.mu.Lock()
	c.conf = conf
	c.ring = ring
	c.mu.Unlock()
}

func (c *ringConf) Init(_ *app.App) error         { return nil }
func (c *ringConf) Name() string                  { return nodeconf.CName }
func (c *ringConf) Run(_ context.Context) error   { return nil }
func (c *ringConf) Close(_ context.Context) error { return nil }
func (c *ringConf) Id() string                    { c.mu.RLock(); defer c.mu.RUnlock(); return c.conf.Id }
func (c *ringConf) Configuration() nodeconf.Configuration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conf
}

func (c *ringConf) NodeIds(spaceId string) (res []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, m := range c.ring.GetMembers(nodeconf.ReplKey(spaceId)) {
		if m.Id() != c.self {
			res = append(res, m.Id())
		}
	}
	return
}

func (c *ringConf) IsResponsible(spaceId string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, m := range c.ring.GetMembers(nodeconf.ReplKey(spaceId)) {
		if m.Id() == c.self {
			return true
		}
	}
	return false
}

func (c *ringConf) NodeTypes(nodeId string) []nodeconf.NodeType {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, n := range c.conf.Nodes {
		if n.PeerId == nodeId {
			return n.Types
		}
	}
	return nil
}

func (c *ringConf) FileV2NodeIds(spaceId string) []string { return nil }
func (c *ringConf) FileV2Peers() []string                 { return nil }
func (c *ringConf) FilePeers() []string                   { return nil }
func (c *ringConf) ConsensusPeers() []string              { return nil }
func (c *ringConf) CoordinatorPeers() []string            { return nil }
func (c *ringConf) NamingNodePeers() []string             { return nil }
func (c *ringConf) PaymentProcessingNodePeers() []string  { return nil }
func (c *ringConf) PeerAddresses(string) ([]string, bool) { return nil, false }
func (c *ringConf) CHash() chash.CHash                    { c.mu.RLock(); defer c.mu.RUnlock(); return c.ring }
func (c *ringConf) Partition(spaceId string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.GetPartition(nodeconf.ReplKey(spaceId))
}
func (c *ringConf) ObserveChanges(o nodeconf.ChangeObserver) {
	c.mu.Lock()
	c.observers = append(c.observers, o)
	c.mu.Unlock()
}
func (c *ringConf) NetworkCompatibilityStatus() nodeconf.NetworkCompatibilityStatus {
	return nodeconf.NetworkCompatibilityStatusOk
}

type chMember string

func (m chMember) Id() string        { return string(m) }
func (m chMember) Capacity() float64 { return 1 }

func newTreeRing(t *testing.T, names []string) chash.CHash {
	ring, err := chash.New(chash.Config{
		PartitionCount:    nodeconf.PartitionCount,
		ReplicationFactor: nodeconf.ReplicationFactor,
	})
	require.NoError(t, err)
	members := make([]chash.Member, 0, len(names))
	for _, n := range names {
		members = append(members, chMember(n))
	}
	require.NoError(t, ring.AddMembers(members...))
	return ring
}

// fakeConfHistory implements nodeconf.HistoryStore for the scenario network.
type fakeConfHistory struct {
	mu    sync.Mutex
	confs map[uint64]nodeconf.Configuration
	last  nodeconf.Configuration
}

func newFakeConfHistory() *fakeConfHistory {
	return &fakeConfHistory{confs: map[uint64]nodeconf.Configuration{}}
}

func (h *fakeConfHistory) add(conf nodeconf.Configuration) {
	h.mu.Lock()
	h.confs[conf.Epoch] = conf
	h.last = conf
	h.mu.Unlock()
}

func (h *fakeConfHistory) Init(_ *app.App) error { return nil }
func (h *fakeConfHistory) Name() string          { return nodeconf.CNameStore }

func (h *fakeConfHistory) GetLast(_ context.Context, _ string) (nodeconf.Configuration, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.last, nil
}

func (h *fakeConfHistory) SaveLast(_ context.Context, c nodeconf.Configuration) error {
	h.add(c)
	return nil
}

func (h *fakeConfHistory) GetByEpoch(_ context.Context, _ string, epoch uint64) (nodeconf.Configuration, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.confs[epoch]
	if !ok {
		return c, nodeconf.ErrConfigurationNotFound
	}
	return c, nil
}

func (h *fakeConfHistory) Epochs(_ context.Context, _ string) (epochs []uint64, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for e := range h.confs {
		epochs = append(epochs, e)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	return
}
