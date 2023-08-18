package spacedeleter

import (
	"context"
	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/periodicsync"
	"go.uber.org/zap"
	"time"
)

const CName = "node.nodespace.spacedeleter"

const (
	periodicDeleteSecs = 60
	deleteTimeout      = 100 * time.Second
	logLimit           = 1000
)

var log = logger.NewNamed(CName)

func New() app.Component {
	return &spaceDeleter{}
}

type spaceDeleter struct {
	periodicCall    periodicsync.PeriodicSync
	coordClient     coordinatorclient.CoordinatorClient
	deletionStorage nodestorage.DeletionStorage
	spaceService    nodespace.Service
	storageProvider nodestorage.NodeStorage
	syncWaiter      <-chan struct{}
}

func (s *spaceDeleter) Init(a *app.App) (err error) {
	s.periodicCall = periodicsync.NewPeriodicSync(periodicDeleteSecs, deleteTimeout, s.delete, log)
	s.coordClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	s.spaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	s.storageProvider = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	s.deletionStorage = s.storageProvider.DeletionStorage()
	s.syncWaiter = a.MustComponent(nodesync.CName).(nodesync.NodeSync).WaitSyncOnStart()
	return
}

func (s *spaceDeleter) Name() (name string) {
	return CName
}

func (s *spaceDeleter) Run(ctx context.Context) (err error) {
	s.periodicCall.Run()
	return
}

func (s *spaceDeleter) Close(ctx context.Context) (err error) {
	s.periodicCall.Close()
	return
}

func (s *spaceDeleter) delete(ctx context.Context) error {
	select {
	// waiting for nodes to sync before we start deletion process
	case <-s.syncWaiter:
	case <-ctx.Done():
		return ctx.Err()
	}
	lastRecordId, err := s.deletionStorage.LastRecordId()
	if err != nil && err != nodestorage.ErrNoLastRecordId {
		return err
	}
	log.Debug("getting deletion log", zap.Int("limit", logLimit), zap.String("lastRecordId", lastRecordId))
	recs, err := s.coordClient.DeletionLog(ctx, lastRecordId, logLimit)
	if err != nil {
		return err
	}
	for _, rec := range recs {
		err = s.processDeletionRecord(ctx, rec)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *spaceDeleter) processDeletionRecord(ctx context.Context, rec *coordinatorproto.DeletionLogRecord) (err error) {
	log := log.With(zap.String("spaceId", rec.SpaceId))
	updateCachedSpace := func(status nodestorage.SpaceStatus, deleted bool) error {
		err := s.deletionStorage.SetSpaceStatus(rec.SpaceId, status)
		if err != nil {
			return err
		}
		space, err := s.spaceService.PickSpace(ctx, rec.SpaceId)
		if err != nil {
			if err == ocache.ErrNotExists {
				return nil
			}
			return err
		}
		space.SetDeleted(deleted)
		return nil
	}
	switch rec.Status {
	case coordinatorproto.DeletionLogRecordStatus_Ok:
		log.Debug("received deletion cancel record")
		err := updateCachedSpace(nodestorage.SpaceStatusOk, false)
		if err != nil {
			return err
		}
	case coordinatorproto.DeletionLogRecordStatus_RemovePrepare:
		log.Debug("received deletion prepare record")
		err := updateCachedSpace(nodestorage.SpaceStatusRemovePrepare, true)
		if err != nil {
			return err
		}
	case coordinatorproto.DeletionLogRecordStatus_Remove:
		log.Debug("received deletion record")
		err := s.storageProvider.DeleteSpaceStorage(ctx, rec.SpaceId)
		if err != nil && err != spacestorage.ErrSpaceStorageMissing {
			return err
		}
		err = s.deletionStorage.SetSpaceStatus(rec.SpaceId, nodestorage.SpaceStatusRemove)
		if err != nil {
			return err
		}
	}
	return s.deletionStorage.SetLastRecordId(rec.Id)
}
