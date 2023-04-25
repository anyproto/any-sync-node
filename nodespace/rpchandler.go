package nodespace

import (
	"context"
	"encoding/hex"
	"github.com/anytypeio/any-sync/commonspace"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"github.com/anytypeio/any-sync/coordinator/coordinatorproto"
	"github.com/anytypeio/any-sync/net/peer"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"math"
)

type rpcHandler struct {
	s *service
}

func (r *rpcHandler) SpacePull(ctx context.Context, request *spacesyncproto.SpacePullRequest) (resp *spacesyncproto.SpacePullResponse, err error) {
	accountIdentity, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	log := log.With(zap.String("spaceId", request.Id), zap.String("accountId", accountIdentity.Account()))
	err = checkResponsible(ctx, r.s.confService, request.Id)
	if err != nil {
		log.Debug("space requested from not responsible peer", zap.Error(err))
		return nil, spacesyncproto.ErrPeerIsNotResponsible
	}
	sp, err := r.s.GetSpace(ctx, request.Id)
	if err != nil {
		if err != spacesyncproto.ErrSpaceMissing {
			err = spacesyncproto.ErrUnexpected
		}
		return
	}

	spaceDesc, err := sp.Description()
	if err != nil {
		err = spacesyncproto.ErrUnexpected
		return
	}

	resp = &spacesyncproto.SpacePullResponse{
		Payload: &spacesyncproto.SpacePayload{
			SpaceHeader:            spaceDesc.SpaceHeader,
			AclPayloadId:           spaceDesc.AclId,
			AclPayload:             spaceDesc.AclPayload,
			SpaceSettingsPayload:   spaceDesc.SpaceSettingsPayload,
			SpaceSettingsPayloadId: spaceDesc.SpaceSettingsId,
		},
	}
	return
}

func (r *rpcHandler) SpacePush(ctx context.Context, req *spacesyncproto.SpacePushRequest) (resp *spacesyncproto.SpacePushResponse, err error) {
	accountIdentity, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	spaceId := req.Payload.GetSpaceHeader().GetId()
	log := log.With(zap.String("spaceId", spaceId), zap.String("accountId", accountIdentity.Account()))
	// checking if the node is responsible for the space and the client is pushing
	err = checkResponsible(ctx, r.s.confService, spaceId)
	if err != nil {
		log.Debug("space sent to not responsible peer", zap.Error(err))
		return nil, spacesyncproto.ErrPeerIsNotResponsible
	}
	accountMarshalled, err := peer.CtxIdentity(ctx)
	if err != nil {
		return
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	receipt := &coordinatorproto.SpaceReceiptWithSignature{}
	err = proto.Unmarshal(req.Credential, receipt)
	if err != nil {
		log.Debug("space validation failed", zap.Error(err))
		return nil, spacesyncproto.ErrReceiptInvalid
	}
	// checking if the receipt is valid and properly signed
	err = coordinatorproto.CheckReceipt(peerId, spaceId, accountMarshalled, r.s.confService.Configuration().NetworkId, receipt)
	if err != nil {
		log.Debug("space validation failed", zap.Error(err))
		return nil, spacesyncproto.ErrReceiptInvalid
	}
	description := commonspace.SpaceDescription{
		SpaceHeader:          req.Payload.GetSpaceHeader(),
		AclId:                req.Payload.GetAclPayloadId(),
		AclPayload:           req.Payload.GetAclPayload(),
		SpaceSettingsPayload: req.Payload.GetSpaceSettingsPayload(),
		SpaceSettingsId:      req.Payload.GetSpaceSettingsPayloadId(),
	}
	ctx = context.WithValue(ctx, commonspace.AddSpaceCtxKey, description)
	// calling GetSpace to add space inside the cache, so we this action would be synchronised
	_, err = r.s.GetSpace(ctx, description.SpaceHeader.GetId())
	if err != nil {
		return
	}
	resp = &spacesyncproto.SpacePushResponse{}
	return
}

func (r *rpcHandler) HeadSync(ctx context.Context, req *spacesyncproto.HeadSyncRequest) (resp *spacesyncproto.HeadSyncResponse, err error) {
	accountIdentity, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	err = checkResponsible(ctx, r.s.confService, req.SpaceId)
	if err != nil {
		log.Debug("head sync sent to not responsible peer",
			zap.Error(err),
			zap.String("spaceId", req.SpaceId),
			zap.String("accountId", accountIdentity.Account()))
		return nil, spacesyncproto.ErrPeerIsNotResponsible
	}
	if resp = r.tryStoreHeadSync(req); resp != nil {
		return
	}
	sp, err := r.s.GetSpace(ctx, req.SpaceId)
	if err != nil {
		return
	}
	return sp.HeadSync().HandleRangeRequest(ctx, req)
}

func (r *rpcHandler) tryStoreHeadSync(req *spacesyncproto.HeadSyncRequest) (resp *spacesyncproto.HeadSyncResponse) {
	if len(req.Ranges) == 1 {
		if req.Ranges[0].From == 0 && req.Ranges[0].To == math.MaxUint64 {
			hash, err := r.s.nodeHead.GetHead(req.SpaceId)
			if err != nil {
				return
			}
			hashB, err := hex.DecodeString(hash)
			if err != nil {
				return
			}
			log.Debug("got head sync with nodehead", zap.String("spaceId", req.SpaceId))
			return &spacesyncproto.HeadSyncResponse{
				Results: []*spacesyncproto.HeadSyncResult{
					{
						Hash: hashB,
						// this makes diff not compareResults and create new batch directly (see (d *diff) Diff)
						Count: 1,
					},
				},
			}
		}
	}
	return nil
}

func (r *rpcHandler) ObjectSyncStream(stream spacesyncproto.DRPCSpaceSync_ObjectSyncStreamStream) (err error) {
	peerId, err := peer.CtxPeerId(stream.Context())
	defer func() {
		log.Debug("incoming stream error", zap.Error(err), zap.String("peerId", peerId))
	}()

	if err != nil {
		return err
	}
	log.Debug("open incoming stream", zap.String("peerId", peerId))
	return r.s.streamPool.ReadStream(peerId, stream)
}
