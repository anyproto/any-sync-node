package nodespace

import (
	"context"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"github.com/anytypeio/any-sync/net/peer"
	"github.com/anytypeio/any-sync/nodeconf"
)

// checkResponsible returns err if we are connecting with client, and we are not responsible for the space
func checkResponsible(ctx context.Context, confService nodeconf.Service, spaceId string) (err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	isClient := len(confService.NodeTypes(peerId)) == 0
	if isClient && !confService.IsResponsible(spaceId) {
		return spacesyncproto.ErrPeerIsNotResponsible
	}
	return
}
