package nodespace

import (
	"context"
	"encoding/hex"
	"github.com/anytypeio/any-sync/commonspace"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"github.com/anytypeio/any-sync/net/peer"
	"go.uber.org/zap"
	"math"
)

type rpcHandler struct {
	s *service
}

func (r *rpcHandler) SpacePull(ctx context.Context, request *spacesyncproto.SpacePullRequest) (resp *spacesyncproto.SpacePullResponse, err error) {
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
	description := commonspace.SpaceDescription{
		SpaceHeader:          req.Payload.SpaceHeader,
		AclId:                req.Payload.AclPayloadId,
		AclPayload:           req.Payload.AclPayload,
		SpaceSettingsPayload: req.Payload.SpaceSettingsPayload,
		SpaceSettingsId:      req.Payload.SpaceSettingsPayloadId,
	}
	ctx = context.WithValue(ctx, commonspace.AddSpaceCtxKey, description)
	_, err = r.s.GetSpace(ctx, description.SpaceHeader.GetId())
	if err != nil {
		return
	}
	resp = &spacesyncproto.SpacePushResponse{}
	return
}

func (r *rpcHandler) HeadSync(ctx context.Context, req *spacesyncproto.HeadSyncRequest) (resp *spacesyncproto.HeadSyncResponse, err error) {
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
		log.Warn("incoming stream error", zap.Error(err), zap.String("peerId", peerId))
	}()

	if err != nil {
		return err
	}
	log.Debug("open incoming stream", zap.String("peerId", peerId))
	return r.s.streamPool.ReadStream(peerId, stream)
}
