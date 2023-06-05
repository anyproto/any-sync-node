package nodedebugrpc

import (
	"context"
	"github.com/anyproto/any-sync-node/nodespace"
	nodestorage "github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/net"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/any-sync/nodeconf"
	"storj.io/drpc"
)

const CName = "node.debug.nodedebugrpc"

var log = logger.NewNamed(CName)

func New() NodeDebugRpc {
	return &nodeDebugRpc{}
}

type configGetter interface {
	GetDebugNet() net.Config
}

type NodeDebugRpc interface {
	app.ComponentRunnable
	drpc.Mux
}

type nodeDebugRpc struct {
	transport      secureservice.SecureService
	cfg            net.Config
	treeCache      treemanager.TreeManager
	spaceService   nodespace.Service
	storageService nodestorage.NodeStorage
	nodeSync       nodesync.NodeSync
	nodeConf       nodeconf.Service
}

func (s *nodeDebugRpc) Register(srv interface{}, desc drpc.Description) error {
	// TODO: implement debug rpc
	return nil
}

func (s *nodeDebugRpc) Init(a *app.App) (err error) {
	s.treeCache = a.MustComponent(treemanager.CName).(treemanager.TreeManager)
	s.spaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	s.storageService = a.MustComponent(spacestorage.CName).(nodestorage.NodeStorage)
	s.cfg = a.MustComponent("config").(configGetter).GetDebugNet()
	s.transport = a.MustComponent(secureservice.CName).(secureservice.SecureService)
	s.nodeSync = a.MustComponent(nodesync.CName).(nodesync.NodeSync)
	s.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	return nil
}

func (s *nodeDebugRpc) Name() (name string) {
	return CName
}

func (s *nodeDebugRpc) Run(ctx context.Context) (err error) {
	return nil
}

func (s *nodeDebugRpc) Close(ctx context.Context) (err error) {
	return nil
}
