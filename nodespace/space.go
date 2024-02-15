package nodespace

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/consensus/consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodestorage"
)

type NodeSpace interface {
	commonspace.Space
}

func newNodeSpace(cc commonspace.Space, consClient consensusclient.Service, nodeStorage nodestorage.NodeStorage) (*nodeSpace, error) {
	return &nodeSpace{
		Space:       cc,
		consClient:  consClient,
		nodeStorage: nodeStorage,
		log:         log.With(zap.String("spaceId", cc.Id())),
	}, nil
}

type nodeSpace struct {
	commonspace.Space
	consClient  consensusclient.Service
	nodeStorage nodestorage.NodeStorage
	log         logger.CtxLogger
}

func (s *nodeSpace) AddConsensusRecords(recs []*consensusproto.RawRecordWithId) {
	log := s.log.With(zap.Int("len(records)", len(recs)), zap.String("firstId", recs[0].Id))
	s.Acl().Lock()
	defer s.Acl().Unlock()
	err := s.Acl().AddRawRecords(recs)
	if err != nil {
		log.Warn("failed to add consensus records", zap.Error(err))
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
	// TODO: call a coordinator?
	err = s.consClient.AddLog(ctx, s.Id(), &consensusproto.RawRecordWithId{
		Payload: s.Acl().Root().Payload,
		Id:      s.Acl().Id(),
	})
	if err != nil && rpcerr.Unwrap(err) != consensuserr.ErrLogExists {
		log.Warn("failed to add consensus record", zap.Error(err))
	}
	return s.consClient.Watch(s.Acl().Id(), s)
}

func (s *nodeSpace) TryClose(objectTTL time.Duration) (close bool, err error) {
	if close, err = s.Space.TryClose(objectTTL); close {
		unwatchErr := s.consClient.UnWatch(s.Acl().Id())
		if unwatchErr != nil {
			s.log.Warn("failed to unwatch space", zap.Error(unwatchErr))
		}
	}
	return
}

func (s *nodeSpace) Close() (err error) {
	err = s.consClient.UnWatch(s.Acl().Id())
	if err != nil {
		s.log.Warn("failed to unwatch space", zap.Error(err))
	}
	return s.Space.Close()
}
