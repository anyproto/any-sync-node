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

func newNodeSpace(cc commonspace.Space, consClient consensusclient.Service, nodeStorage nodestorage.NodeStorage, onAclUpdate func(spaceId string)) (*nodeSpace, error) {
	return &nodeSpace{
		Space:       cc,
		consClient:  consClient,
		nodeStorage: nodeStorage,
		onAclUpdate: onAclUpdate,
		log:         log.With(zap.String("spaceId", cc.Id())),
	}, nil
}

type nodeSpace struct {
	commonspace.Space
	consClient  consensusclient.Service
	nodeStorage nodestorage.NodeStorage
	onAclUpdate func(spaceId string)
	log         logger.CtxLogger
}

func (s *nodeSpace) AddConsensusRecords(recs []*consensusproto.RawRecordWithId) {
	log := s.log.With(zap.Int("len(records)", len(recs)), zap.String("firstId", recs[0].Id))
	s.Acl().Lock()
	for i := 0; i < len(recs)/2; i++ {
		recs[i], recs[len(recs)-i-1] = recs[len(recs)-i-1], recs[i]
	}
	err := s.Acl().AddRawRecords(recs)
	s.Acl().Unlock()
	if err != nil {
		log.Warn("failed to add consensus records", zap.Error(err))
	} else {
		log.Debug("added consensus records")
	}
	// notify observers (pubsub relay) outside the acl lock so they can re-read it.
	// fire even on error: AddRawRecords applies records one by one, so a partial
	// batch may already have changed membership before failing.
	if s.onAclUpdate != nil {
		s.onAclUpdate(s.Id())
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
	s.addLog(ctx, &consensusproto.RawRecordWithId{
		Payload: s.Acl().Root().Payload,
		Id:      s.Acl().Id(),
	})
	if err = s.consClient.Watch(s.Id(), s); err != nil {
		_ = s.Space.Close()
		return
	}
	return
}

func (s *nodeSpace) TryClose(objectTTL time.Duration) (close bool, err error) {
	if close, err = s.Space.TryClose(objectTTL); close {
		unwatchErr := s.consClient.UnWatch(s.Id())
		if unwatchErr != nil {
			s.log.Warn("failed to unwatch space", zap.Error(unwatchErr))
		}
	}
	return
}

func (s *nodeSpace) Close() (err error) {
	err = s.consClient.UnWatch(s.Id())
	if err != nil {
		s.log.Warn("failed to unwatch space", zap.Error(err))
	}
	return s.Space.Close()
}

const (
	// addLogAttempts bounds the attempts to create the space's consensus log
	addLogAttempts = 3
	// addLogRetryDelay is the wait before the second attempt; it grows linearly with each attempt
	addLogRetryDelay = 100 * time.Millisecond
)

// addLog creates the space's consensus log, or finds it created. Every node that stores the space creates it,
// usually at the same time, and a watch on the log fails until it exists, so a failed attempt is retried.
// A failure is only logged, so that the space still loads.
func (s *nodeSpace) addLog(ctx context.Context, root *consensusproto.RawRecordWithId) {
	var err error
	for attempt := 1; attempt <= addLogAttempts; attempt++ {
		if attempt > 1 && !sleepCtx(ctx, time.Duration(attempt-1)*addLogRetryDelay) {
			break
		}
		err = s.consClient.AddLog(ctx, s.Id(), root)
		switch rpcerr.Unwrap(err) {
		case nil, consensuserr.ErrLogExists:
			return
		}
		if isPermanentAddLogErr(err) {
			break
		}
	}
	s.log.Warn("failed to add consensus record", zap.Error(err))
}

// isPermanentAddLogErr reports whether another AddLog attempt would fail the same way
func isPermanentAddLogErr(err error) bool {
	switch rpcerr.Unwrap(err) {
	case consensuserr.ErrForbidden, consensuserr.ErrInvalidPayload:
		return true
	}
	return false
}

// sleepCtx waits for d and reports false when ctx is done first
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
