package nodesync

import (
	"context"
	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodesync/coldsync"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

var _ nodesyncproto.DRPCNodeSyncServer = (*rpcHandler)(nil)

type rpcHandler struct {
	*nodeRemoteDiffHandler
	coldSync  coldsync.ColdSync
	nodeSpace nodespace.Service
}

func (r rpcHandler) ColdSync(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	return r.coldSync.ColdSyncHandle(req, stream)
}

func (r rpcHandler) SpaceDelete(ctx context.Context, request *nodesyncproto.SpaceDeleteRequest) (res *nodesyncproto.SpaceDeleteResponse, err error) {
	err = r.nodeSpace.DeleteSpace(ctx, request.SpaceId, request.ChangeId, request.DeleteChange)
	if err != nil {
		return
	}
	res = &nodesyncproto.SpaceDeleteResponse{}
	return
}
