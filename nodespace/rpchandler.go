package nodespace

import (
	"context"
	"encoding/hex"
	"math"
	"time"

	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type rpcHandler struct {
	s *service
}

func (r *rpcHandler) AclAddRecord(ctx context.Context, request *spacesyncproto.AclAddRecordRequest) (resp *spacesyncproto.AclAddRecordResponse, err error) {
	space, err := r.s.GetSpace(ctx, request.SpaceId)
	if err != nil {
		return
	}
	rec := &consensusproto.RawRecord{}
	err = proto.Unmarshal(request.Payload, rec)
	if err != nil {
		return
	}
	acl := space.Acl()
	acl.RLock()
	err = acl.ValidateRawRecord(rec)
	if err != nil {
		acl.RUnlock()
		return
	}
	acl.RUnlock()
	rawRecordWithId, err := r.s.consClient.AddRecord(ctx, acl.Id(), rec)
	if err != nil {
		return
	}
	acl.Lock()
	defer acl.Unlock()
	err = acl.AddRawRecord(rawRecordWithId)
	if err != nil {
		return
	}
	resp = &spacesyncproto.AclAddRecordResponse{
		RecordId: rawRecordWithId.Id,
		Payload:  rawRecordWithId.Payload,
	}
	return
}

func (r *rpcHandler) AclGetRecords(ctx context.Context, request *spacesyncproto.AclGetRecordsRequest) (resp *spacesyncproto.AclGetRecordsResponse, err error) {
	space, err := r.s.GetSpace(ctx, request.SpaceId)
	if err != nil {
		return
	}
	acl := space.Acl()
	acl.RLock()
	recordsBefore, err := acl.RecordsBefore(ctx, request.AclHead)
	if err != nil {
		acl.RUnlock()
		return
	}
	acl.RUnlock()
	for _, rec := range recordsBefore {
		marshalled, err := proto.Marshal(rec)
		if err != nil {
			return nil, err
		}
		resp.Records = append(resp.Records, marshalled)
	}
	return
}

func (r *rpcHandler) ObjectSync(ctx context.Context, req *spacesyncproto.ObjectSyncMessage) (resp *spacesyncproto.ObjectSyncMessage, err error) {
	st := time.Now()
	defer func() {
		r.s.metric.RequestLog(ctx, "space.objectSync",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(req.SpaceId),
			metric.ObjectId(req.ObjectId),
			zap.Error(err),
		)
	}()
	accountIdentity, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	err = checkResponsible(ctx, r.s.confService, req.SpaceId)
	if err != nil {
		log.Debug("object sync sent to not responsible peer",
			zap.Error(err),
			zap.String("spaceId", req.SpaceId),
			zap.String("accountId", accountIdentity.Account()))
		return nil, spacesyncproto.ErrPeerIsNotResponsible
	}
	sp, err := r.s.GetSpace(ctx, req.SpaceId)
	if err != nil {
		if err != spacesyncproto.ErrSpaceMissing {
			err = spacesyncproto.ErrUnexpected
		}
		return
	}
	resp, err = sp.HandleSyncRequest(ctx, req)
	return
}

func (r *rpcHandler) SpacePull(ctx context.Context, req *spacesyncproto.SpacePullRequest) (resp *spacesyncproto.SpacePullResponse, err error) {
	st := time.Now()
	defer func() {
		r.s.metric.RequestLog(ctx, "space.spacePull",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(req.Id),
			zap.Error(err),
		)
	}()
	accountIdentity, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}
	log := log.With(zap.String("spaceId", req.Id), zap.String("accountId", accountIdentity.Account()))
	err = checkResponsible(ctx, r.s.confService, req.Id)
	if err != nil {
		log.Debug("space requested from not responsible peer", zap.Error(err))
		err = spacesyncproto.ErrPeerIsNotResponsible
		return nil, err
	}
	sp, err := r.s.GetSpace(ctx, req.Id)
	if err != nil {
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
	var spaceId string
	st := time.Now()
	defer func() {
		r.s.metric.RequestLog(ctx, "space.spacePush",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(spaceId),
			zap.Error(err),
		)
	}()
	if req.Payload != nil {
		spaceId = req.Payload.GetSpaceHeader().GetId()
	}
	if spaceId == "" {
		err = spacesyncproto.ErrUnexpected
		return
	}
	accountIdentity, err := peer.CtxPubKey(ctx)
	if err != nil {
		return
	}

	log := log.With(zap.String("spaceId", spaceId), zap.String("accountId", accountIdentity.Account()))
	// checking if the node is responsible for the space and the client is pushing
	err = checkResponsible(ctx, r.s.confService, spaceId)
	if err != nil {
		log.Debug("space sent to not responsible peer", zap.Error(err))
		err = spacesyncproto.ErrPeerIsNotResponsible
		return nil, err
	}
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}

	if !slices.Contains(r.s.confService.NodeTypes(peerId), nodeconf.NodeTypeTree) {
		// check receipt only for client request
		if err = checkReceipt(ctx, r.s.confService, spaceId, req.Credential); err != nil {
			return nil, err
		}
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
	st := time.Now()
	var deepHeadSync bool
	defer func() {
		r.s.metric.RequestLog(ctx, "space.headSync",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(req.SpaceId),
			zap.Bool("deepHeadSync", deepHeadSync),
			zap.Error(err),
		)
	}()
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
	if resp = r.tryNodeHeadSync(req); resp != nil {
		return
	}
	deepHeadSync = true
	sp, err := r.s.GetSpace(ctx, req.SpaceId)
	if err != nil {
		return
	}
	resp, err = sp.HandleRangeRequest(ctx, req)
	return
}

func (r *rpcHandler) tryNodeHeadSync(req *spacesyncproto.HeadSyncRequest) (resp *spacesyncproto.HeadSyncResponse) {
	if len(req.Ranges) == 1 && (req.Ranges[0].From == 0 && req.Ranges[0].To == math.MaxUint64) {
		switch req.DiffType {
		case spacesyncproto.DiffType_Precalculated:
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
				DiffType: spacesyncproto.DiffType_Precalculated,
				Results: []*spacesyncproto.HeadSyncResult{
					{
						Hash: hashB,
						// this makes diff not compareResults and create new batch directly (see (d *diff) Diff)
						Count: 1,
					},
				},
			}
		case spacesyncproto.DiffType_Initial:
			hash, err := r.s.nodeHead.GetOldHead(req.SpaceId)
			if err != nil {
				return
			}
			hashB, err := hex.DecodeString(hash)
			if err != nil {
				return
			}
			log.Debug("got head sync with old nodehead", zap.String("spaceId", req.SpaceId))
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
	defer func() {
		log.DebugCtx(stream.Context(), "incoming stream error")
	}()

	log.DebugCtx(stream.Context(), "open incoming stream")
	err = r.s.streamPool.ReadStream(stream)
	return
}
