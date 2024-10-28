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
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type rpcHandler struct {
	s *service
}

func (r *rpcHandler) AclAddRecord(ctx context.Context, request *spacesyncproto.AclAddRecordRequest) (resp *spacesyncproto.AclAddRecordResponse, err error) {
	st := time.Now()
	defer func() {
		r.s.metric.RequestLog(ctx, "space.aclAddRecord",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(request.SpaceId),
			zap.Error(err),
		)
	}()
	var record = &consensusproto.RawRecord{}
	if err = record.Unmarshal(request.Payload); err != nil {
		return
	}
	res, err := r.s.coordClient.AclAddRecord(ctx, request.SpaceId, record)
	if err != nil {
		return nil, err
	}

	// wakeup the space to propagate acl sync
	_, _ = r.s.spaceCache.Get(ctx, request.SpaceId)

	return &spacesyncproto.AclAddRecordResponse{
		RecordId: res.Id,
		Payload:  res.Payload,
	}, nil
}

func (r *rpcHandler) AclGetRecords(ctx context.Context, request *spacesyncproto.AclGetRecordsRequest) (resp *spacesyncproto.AclGetRecordsResponse, err error) {
	st := time.Now()
	defer func() {
		r.s.metric.RequestLog(ctx, "space.aclGetRecords",
			metric.TotalDur(time.Since(st)),
			metric.SpaceId(request.SpaceId),
			zap.Error(err),
		)
	}()
	// deprecated - just proxy this call to the coordinator
	res, err := r.s.coordClient.AclGetRecords(ctx, request.SpaceId, request.AclHead)
	if err != nil {
		return nil, err
	}
	resp = &spacesyncproto.AclGetRecordsResponse{
		Records: make([][]byte, len(res)),
	}
	for i, rec := range res {
		if resp.Records[i], err = rec.Marshal(); err != nil {
			return
		}
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
		return nil, err
	}
	return sp.HandleDeprecatedObjectSyncRequest(ctx, req)
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

func (r *rpcHandler) ObjectSyncRequestStream(req *spacesyncproto.ObjectSyncMessage, stream spacesyncproto.DRPCSpaceSync_ObjectSyncRequestStreamStream) (err error) {
	st := time.Now()
	ctx := stream.Context()
	defer func() {
		r.s.metric.RequestLog(ctx, "space.objectSyncRequestStream",
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
		return spacesyncproto.ErrPeerIsNotResponsible
	}
	sp, err := r.s.GetSpace(stream.Context(), req.SpaceId)
	if err != nil {
		return err
	}
	return sp.HandleStreamSyncRequest(stream.Context(), req, stream)
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
	log.DebugCtx(ctx, "deep head sync", zap.String("spaceId", req.SpaceId), zap.Int("type", int(req.DiffType)))
	sp, err := r.s.GetSpace(ctx, req.SpaceId)
	if err != nil {
		return
	}
	resp, err = sp.HandleRangeRequest(ctx, req)
	return
}

func (r *rpcHandler) tryNodeHeadSync(req *spacesyncproto.HeadSyncRequest) (resp *spacesyncproto.HeadSyncResponse) {
	if len(req.Ranges) == 1 && (req.Ranges[0].From == 0 && req.Ranges[0].To == math.MaxUint64) {
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
			DiffType: spacesyncproto.DiffType_Precalculated,
		}
	}
	return nil
}

func (r *rpcHandler) ObjectSyncStream(stream spacesyncproto.DRPCSpaceSync_ObjectSyncStreamStream) (err error) {
	return r.s.streamPool.ReadStream(stream, 100)
}
