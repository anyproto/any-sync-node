package nodedebugrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

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
	transport        secureservice.SecureService
	treeCache        treemanager.TreeManager
	spaceService     nodespace.Service
	storageService   nodestorage.NodeStorage
	nodeSpaceService nodespace.Service
	nodeSync         nodesync.NodeSync
	nodeConf         nodeconf.Service
	server           debugserver.DebugServer
	statService      debugstat.StatService
}

type statsError struct {
	Error string `json:"error,omitempty"`
}

func (s *nodeDebugRpc) Init(a *app.App) (err error) {
	s.treeCache = a.MustComponent(treemanager.CName).(treemanager.TreeManager)
	s.spaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	s.storageService = a.MustComponent(spacestorage.CName).(nodestorage.NodeStorage)
	s.nodeSpaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	s.transport = a.MustComponent(secureservice.CName).(secureservice.SecureService)
	s.nodeSync = a.MustComponent(nodesync.CName).(nodesync.NodeSync)
	s.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	s.server = a.MustComponent(debugserver.CName).(debugserver.DebugServer)
	s.statService = a.MustComponent(debugstat.CName).(debugstat.StatService)
	http.HandleFunc("/stat/{spaceId}", s.handleSpaceStats)
	http.HandleFunc("/stats", s.handleStats)
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
		errorStr := "failed to marshal stat"
		errReply := statsError{
			Error: errorStr,
		}
		marshalledErr, _ := json.MarshalIndent(errReply, "", "  ")

		log.Error(errorStr, zap.Error(err))
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write(marshalledErr)
		return
	}
	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write(marshalled)
}

func (s *nodeDebugRpc) handleSpaceStats(rw http.ResponseWriter, req *http.Request) {
	spaceId := req.PathValue("spaceId")
	reqCtx := req.Context()

	var treeTop, _ = strconv.Atoi(req.URL.Query().Get("treeTop"))

	if !s.nodeConf.IsResponsible(spaceId) {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte("{\"error\": \"node is not responsible\"}"))
		return
	}

	spaceStats, err := s.spaceService.GetStats(reqCtx, spaceId, treeTop)

	if err != nil {
		errStatus := http.StatusInternalServerError
		if errors.Is(err, nodestorage.ErrDoesntSupportSpaceStats) {
			errStatus = http.StatusNotImplemented
		}

		if errors.Is(err, nodespace.ErrSpaceStatus) {
			errStatus = http.StatusBadRequest
		}

		errReply := statsError{
			Error: err.Error(),
		}

		rw.WriteHeader(errStatus)
		marshalledErr, _ := json.MarshalIndent(errReply, "", "  ")
		rw.Write(marshalledErr)
		return
	}

	marshalled, err := json.MarshalIndent(spaceStats, "", "  ")
	if err != nil {
		log.Error("failed to marshal stat", zap.Error(err))
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("{\"error\": \"failed to marshal stat\"}"))
		return
	}

	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write(marshalled)
}
