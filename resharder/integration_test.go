package resharder

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-node/archive"
	"github.com/anyproto/any-sync-node/archive/adopter"
	"github.com/anyproto/any-sync-node/archive/archivestore"
	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodehead/mock_nodehead"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/hotsync"
	"github.com/anyproto/any-sync-node/nodesync/hotsync/mock_hotsync"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

// TestIntegration_DrainAdoptRestore drives the full resharding data plane
// in-process with two real node stacks (real nodestorage + real archive
// component) and a shared in-memory bucket:
//
//	node A (drainer): live space -> ForceArchive -> AdoptArchive to node B ->
//	verified handoff -> local deletion (status Moved)
//	node B (adopter): server-side copy -> index registration -> eager restore
//	-> live SQLite db on disk
func TestIntegration_DrainAdoptRestore(t *testing.T) {
	bucket := newMemBucket()
	nodeA := newTestNode(t, "nodeA", bucket)
	nodeB := newTestNode(t, "nodeB", bucket)

	const spaceId = "integration.space"

	// create a "space" db on node A and index it
	spaceDir := nodeA.storage.StoreDir(spaceId)
	require.NoError(t, os.MkdirAll(spaceDir, 0o755))
	db, err := anystore.Open(ctx, filepath.Join(spaceDir, "store.db"), nil)
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoError(t, nodeA.storage.IndexStorage().UpdateHash(ctx, nodestorage.SpaceUpdate{
		SpaceId: spaceId, OldHash: "h-old", NewHash: "h-new",
	}))

	// node A is no longer responsible; node B is the only owner (minAcks=1)
	nodeA.nodeConf.EXPECT().IsResponsible(spaceId).AnyTimes().Return(false)
	nodeA.nodeConf.EXPECT().NodeIds(spaceId).Return([]string{"nodeB"})
	// node B accepts adoption from node A (a tree node) and registers heads
	nodeB.nodeConf.EXPECT().NodeTypes("nodeA").Return([]nodeconf.NodeType{nodeconf.NodeTypeTree})
	nodeB.nodeHead.EXPECT().SetHead(spaceId, "h-old", "h-new").Return(0, nil)

	// wire the drainer's adopt call straight into node B's adopter
	nodeA.resharder.adoptFn = func(_ context.Context, peerId string, req *nodesyncproto.AdoptArchiveRequest) (*nodesyncproto.AdoptArchiveResponse, error) {
		require.Equal(t, "nodeB", peerId)
		return nodeB.adopter.AdoptArchive(peer.CtxWithPeerId(ctx, "nodeA"), req)
	}

	ok, err := nodeA.resharder.drainSpace(spaceId)
	require.NoError(t, err)
	require.True(t, ok)

	// node A: local db gone, index says Moved, own archive object deleted
	assert.False(t, nodeA.storage.SpaceExists(spaceId))
	statusA, err := nodeA.storage.IndexStorage().SpaceStatus(ctx, spaceId)
	require.NoError(t, err)
	assert.Equal(t, nodestorage.SpaceStatusMoved, statusA)
	assert.False(t, bucket.has("nodeA/"+spaceId))

	// node B: object in its prefix, index entry Archived with the same heads
	assert.True(t, bucket.has("nodeB/"+spaceId))
	entryB, err := nodeB.storage.IndexStorage().SpaceStatusEntry(ctx, spaceId)
	require.NoError(t, err)
	assert.Equal(t, nodestorage.SpaceStatusArchived, entryB.Status)
	assert.Equal(t, "h-new", entryB.NewHash)

	// eager restore materializes the db on node B
	require.Eventually(t, func() bool {
		statusB, sErr := nodeB.storage.IndexStorage().SpaceStatus(ctx, spaceId)
		return sErr == nil && statusB == nodestorage.SpaceStatusOk
	}, time.Second*10, time.Millisecond*50, "eager restore did not materialize the space on node B")
	assert.True(t, nodeB.storage.SpaceExists(spaceId))

	// the restored db is openable and holds the original collection
	restored, err := anystore.Open(ctx, filepath.Join(nodeB.storage.StoreDir(spaceId), "store.db"), nil)
	require.NoError(t, err)
	defer restored.Close()
	colls, err := restored.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"objects"}, colls)
}

type testNode struct {
	name      string
	a         *app.App
	storage   nodestorage.NodeStorage
	archive   archive.Archive
	adopter   adopter.Adopter
	resharder *resharder
	nodeConf  *mock_nodeconf.MockService
	nodeHead  *mock_nodehead.MockNodeHead
}

func newTestNode(t *testing.T, name string, bucket *memBucket) *testNode {
	ctrl := gomock.NewController(t)
	n := &testNode{
		name:      name,
		a:         new(app.App),
		storage:   nodestorage.New(),
		archive:   archive.New(),
		adopter:   adopter.New(),
		resharder: New().(*resharder),
		nodeConf:  mock_nodeconf.NewMockService(ctrl),
		nodeHead:  mock_nodehead.NewMockNodeHead(ctrl),
	}
	hotSync := mock_hotsync.NewMockHotSync(ctrl)
	anymock.ExpectComp(n.nodeConf.EXPECT(), nodeconf.CName)
	anymock.ExpectComp(n.nodeHead.EXPECT(), nodehead.CName)
	anymock.ExpectComp(hotSync.EXPECT(), hotsync.CName)
	hotSync.EXPECT().SetMetric(gomock.Any(), gomock.Any()).AnyTimes()
	hotSync.EXPECT().UpdateQueue(gomock.Any()).AnyTimes()
	n.nodeConf.EXPECT().ObserveChanges(gomock.Any())

	n.a.Register(testNodeConfig{dir: t.TempDir()}).
		Register(&syncWaiterStub{}).
		Register(newMemStore(bucket, name)).
		Register(n.archive).
		Register(n.storage).
		Register(n.nodeHead).
		Register(n.nodeConf).
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

type testNodeConfig struct {
	dir string
}

func (c testNodeConfig) Init(_ *app.App) error { return nil }
func (c testNodeConfig) Name() string          { return "config" }

func (c testNodeConfig) GetStorage() nodestorage.Config {
	return nodestorage.Config{Path: c.dir, AnyStorePath: c.dir}
}

func (c testNodeConfig) GetArchive() archive.Config {
	// periodic archiving off; the restore worker runs regardless
	return archive.Config{Enabled: false}
}

type syncWaiterStub struct{}

func (s *syncWaiterStub) Init(_ *app.App) error { return nil }
func (s *syncWaiterStub) Name() string          { return "node.nodesync" }

func (s *syncWaiterStub) WaitSyncOnStart() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// memBucket is a shared in-memory "s3 bucket" for the test network.
type memBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	mtimes  map[string]time.Time
}

func newMemBucket() *memBucket {
	return &memBucket{objects: map[string][]byte{}, mtimes: map[string]time.Time{}}
}

func (b *memBucket) has(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.objects[key]
	return ok
}

// memStore implements archivestore.ArchiveStore over the shared bucket.
type memStore struct {
	bucket *memBucket
	prefix string
}

func newMemStore(bucket *memBucket, nodeName string) *memStore {
	return &memStore{bucket: bucket, prefix: nodeName + "/"}
}

func (m *memStore) Init(_ *app.App) error { return nil }
func (m *memStore) Name() string          { return archivestore.CName }

func (m *memStore) Get(_ context.Context, name string) (io.ReadCloser, error) {
	m.bucket.mu.Lock()
	defer m.bucket.mu.Unlock()
	data, ok := m.bucket.objects[m.prefix+name]
	if !ok {
		return nil, archivestore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memStore) Put(_ context.Context, name string, data io.ReadSeeker) error {
	raw, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.bucket.mu.Lock()
	defer m.bucket.mu.Unlock()
	m.bucket.objects[m.prefix+name] = raw
	m.bucket.mtimes[m.prefix+name] = time.Now()
	return nil
}

func (m *memStore) Delete(_ context.Context, name string) error {
	m.bucket.mu.Lock()
	defer m.bucket.mu.Unlock()
	delete(m.bucket.objects, m.prefix+name)
	delete(m.bucket.mtimes, m.prefix+name)
	return nil
}

func (m *memStore) Exists(_ context.Context, name string) (bool, error) {
	return m.bucket.has(m.prefix + name), nil
}

func (m *memStore) Key(name string) string {
	return m.prefix + name
}

func (m *memStore) CopyFrom(_ context.Context, srcKey, name string) error {
	m.bucket.mu.Lock()
	defer m.bucket.mu.Unlock()
	data, ok := m.bucket.objects[srcKey]
	if !ok {
		return archivestore.ErrNotFound
	}
	m.bucket.objects[m.prefix+name] = data
	m.bucket.mtimes[m.prefix+name] = time.Now()
	return nil
}

func (m *memStore) Shared() bool { return true }

func (m *memStore) List(_ context.Context, iter func(name string, lastModified time.Time) (bool, error)) error {
	m.bucket.mu.Lock()
	type obj struct {
		name  string
		mtime time.Time
	}
	var objs []obj
	for key := range m.bucket.objects {
		if strings.HasPrefix(key, m.prefix) {
			objs = append(objs, obj{strings.TrimPrefix(key, m.prefix), m.bucket.mtimes[key]})
		}
	}
	m.bucket.mu.Unlock()
	for _, o := range objs {
		cont, err := iter(o.name, o.mtime)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}
