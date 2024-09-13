package nodedebugrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/debugstat"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/net/rpc/debugserver"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/debug/nodedebugrpc/nodedebugrpcproto"
	"github.com/anyproto/any-sync-node/nodespace"
	nodestorage "github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
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
	statService    debugstat.StatService
}

func (s *nodeDebugRpc) Init(a *app.App) (err error) {
	s.treeCache = a.MustComponent(treemanager.CName).(treemanager.TreeManager)
	s.spaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	s.storageService = a.MustComponent(spacestorage.CName).(nodestorage.NodeStorage)
	s.transport = a.MustComponent(secureservice.CName).(secureservice.SecureService)
	s.nodeSync = a.MustComponent(nodesync.CName).(nodesync.NodeSync)
	s.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	s.server = a.MustComponent(debugserver.CName).(debugserver.DebugServer)
	s.statService = a.MustComponent(debugstat.CName).(debugstat.StatService)
	http.HandleFunc("/stats", s.handleStats)
	http.HandleFunc("/spacestats/{spaceId}", s.handleSpaceStats)
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

func (s *nodeDebugRpc) handleStats(rw http.ResponseWriter, req *http.Request) {
	stats := s.statService.GetStat()
	rw.Header().Set("Content-Type", "application/json")
	marshalled, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		log.Error("failed to marshal stat", zap.Error(err))
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"error\": \"failed to marshal stat\"}"))
		return
	}
	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write(marshalled)
}

func (s *nodeDebugRpc) handleSpaceStats(rw http.ResponseWriter, req *http.Request) {
	spaceId := req.PathValue("spaceId")

	rw.Header().Set("Content-Type", "application/json")
	rw.Write([]byte(fmt.Sprintf("{\"spaceId\": \"%s\"}", spaceId)))
	rw.WriteHeader(http.StatusOK)

}
