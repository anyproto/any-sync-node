package nodespace

import (
	"context"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/consensus/consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"go.uber.org/zap"
)

func newNodeSpace(cc commonspace.Space, consClient consensusclient.Service) (*nodeSpace, error) {
	return &nodeSpace{
		Space:      cc,
		consClient: consClient,
		log:        log.With(zap.String("spaceId", cc.Id())),
	}, nil
}

type nodeSpace struct {
	commonspace.Space
	consClient consensusclient.Service
	log        logger.CtxLogger
}

func (s *nodeSpace) AddConsensusRecords(recs []*consensusproto.RawRecordWithId) {
	log := s.log.With(zap.Int("len(records)", len(recs)), zap.String("firstId", recs[0].Id))
	s.Acl().Lock()
	defer s.Acl().Unlock()
	err := s.Acl().AddRawRecords(recs)
	if err != nil {
		log.Warn("failed to add consensus records")
	} else {
		log.Debug("added consensus records")
	}
}

func (s *nodeSpace) AddConsensusError(err error) {
	s.log.Warn("received consensus error", zap.Error(err))
	return
}

func (s *nodeSpace) Init(ctx context.Context) (err error) {
	err = s.Space.Init(ctx)
	if err != nil {
		return
	}
	err = s.consClient.AddLog(ctx, &consensusproto.RawRecordWithId{
		Payload: s.Acl().Root().Payload,
		Id:      s.Acl().Id(),
	})
	if err != nil && rpcerr.Unwrap(err) != consensuserr.ErrLogExists {
		log.Warn("failed to add consensus record", zap.Error(err))
	}
	return s.consClient.Watch(s.Acl().Id(), s)
}

func (s *nodeSpace) Close() (err error) {
	err = s.consClient.UnWatch(s.Acl().Id())
	if err != nil {
		s.log.Warn("failed to unwatch space")
	}
	return s.Space.Close()
}
