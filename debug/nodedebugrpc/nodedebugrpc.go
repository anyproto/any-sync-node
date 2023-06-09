package nodedebugrpc

import (
	"context"
	"github.com/anyproto/any-sync-node/debug/nodedebugrpc/nodedebugrpcproto"
	"github.com/anyproto/any-sync-node/nodespace"
	nodestorage "github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/net/rpc/debugserver"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/any-sync/nodeconf"
)

const CName = "node.debug.nodedebugrpc"

var log = logger.NewNamed(CName)

func New() NodeDebugRpc {
	return &nodeDebugRpc{}
}

type NodeDebugRpc interface {
	app.ComponentRunnable
}

type nodeDebugRpc struct {
	transport      secureservice.SecureService
	treeCache      treemanager.TreeManager
	spaceService   nodespace.Service
	storageService nodestorage.NodeStorage
	nodeSync       nodesync.NodeSync
	nodeConf       nodeconf.Service
	server         debugserver.DebugServer
}

func (s *nodeDebugRpc) Init(a *app.App) (err error) {
	s.treeCache = a.MustComponent(treemanager.CName).(treemanager.TreeManager)
	s.spaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	s.storageService = a.MustComponent(spacestorage.CName).(nodestorage.NodeStorage)
	s.transport = a.MustComponent(secureservice.CName).(secureservice.SecureService)
	s.nodeSync = a.MustComponent(nodesync.CName).(nodesync.NodeSync)
	s.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	s.server = a.MustComponent(debugserver.CName).(debugserver.DebugServer)
	return nil
}

func (s *nodeDebugRpc) Name() (name string) {
	return CName
}

func (s *nodeDebugRpc) Run(ctx context.Context) (err error) {
	return nodedebugrpcproto.DRPCRegisterNodeApi(s.server, &rpcHandler{
		s: s,
	})
}

func (s *nodeDebugRpc) Close(ctx context.Context) (err error) {
	return nil
}
