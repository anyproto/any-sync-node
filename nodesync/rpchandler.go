package nodesync

import (
	"github.com/anytypeio/any-sync-node/nodesync/coldsync"
	"github.com/anytypeio/any-sync-node/nodesync/nodesyncproto"
)

var _ nodesyncproto.DRPCNodeSyncServer = (*rpcHandler)(nil)

type rpcHandler struct {
	*nodeRemoteDiffHandler
	coldSync coldsync.ColdSync
}

func (r rpcHandler) ColdSync(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	return r.coldSync.ColdSyncHandle(req, stream)
}
