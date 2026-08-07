package nodesync

import (
	"context"

	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodesync/coldsync"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
)

var _ nodesyncproto.DRPCNodeSyncServer = (*rpcHandler)(nil)

// archiveAdopter is implemented by the archive adopter component
// (archive/adopter); looked up by name to avoid an import cycle.
type archiveAdopter interface {
	AdoptArchive(ctx context.Context, req *nodesyncproto.AdoptArchiveRequest) (*nodesyncproto.AdoptArchiveResponse, error)
}

type rpcHandler struct {
	*nodeRemoteDiffHandler
	coldSync  coldsync.ColdSync
	nodeSpace nodespace.Service
	adopter   archiveAdopter
}

func (r rpcHandler) ColdSync(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	return r.coldSync.ColdSyncHandle(req, stream)
}

func (r rpcHandler) AdoptArchive(ctx context.Context, req *nodesyncproto.AdoptArchiveRequest) (*nodesyncproto.AdoptArchiveResponse, error) {
	if r.adopter == nil {
		return nil, nodesyncproto.ErrArchiveUnavailable
	}
	return r.adopter.AdoptArchive(ctx, req)
}
