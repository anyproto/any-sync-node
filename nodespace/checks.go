package nodespace

import (
	"context"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"
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

func checkReceipt(ctx context.Context, confService nodeconf.Service, spaceId string, credential []byte) (err error) {
	accountMarshalled, err := peer.CtxIdentity(ctx)
	if err != nil {
		return
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	receipt := &coordinatorproto.SpaceReceiptWithSignature{}
	err = receipt.UnmarshalVT(credential)
	if err != nil {
		log.Debug("space validation failed", zap.Error(err))
		return spacesyncproto.ErrReceiptInvalid
	}
	// checking if the receipt is valid and properly signed
	err = coordinatorproto.CheckReceipt(peerId, spaceId, accountMarshalled, confService.Configuration().NetworkId, receipt)
	if err != nil {
		log.Debug("space validation failed", zap.Error(err))
		return spacesyncproto.ErrReceiptInvalid
	}
	return nil
}
