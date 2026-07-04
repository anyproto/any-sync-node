package spacedeleter

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/periodicsync"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/archive/archivestore"
	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
)

const CName = "node.nodespace.spacedeleter"

const (
	periodicDeleteSecs = 60
	deleteTimeout      = 100 * time.Second
	logLimit           = 1000
)

var log = logger.NewNamed(CName)

func New() app.Component {
	return &spaceDeleter{testChan: make(chan struct{})}
}

type spaceDeleter struct {
	periodicCall    periodicsync.PeriodicSync
	coordClient     coordinatorclient.CoordinatorClient
	deletionStorage nodestorage.IndexStorage
	spaceService    nodespace.Service
	storageProvider nodestorage.NodeStorage
	nodeConf        nodeconf.Service
	archiveStore    archivestore.ArchiveStore
	nodeHead        nodehead.NodeHead
	syncWaiter      <-chan struct{}

	testOnce sync.Once
	testChan chan struct{}
}

func (s *spaceDeleter) Init(a *app.App) (err error) {
	s.periodicCall = periodicsync.NewPeriodicSync(periodicDeleteSecs, deleteTimeout, s.delete, log)
	s.coordClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	s.spaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	s.storageProvider = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	s.syncWaiter = a.MustComponent(nodesync.CName).(nodesync.NodeSync).WaitSyncOnStart()
	s.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	s.archiveStore = a.MustComponent(archivestore.CName).(archivestore.ArchiveStore)
	s.nodeHead = a.MustComponent(nodehead.CName).(nodehead.NodeHead)
	return
}

func (s *spaceDeleter) Name() (name string) {
	return CName
}

func (s *spaceDeleter) Run(ctx context.Context) (err error) {
	s.deletionStorage = s.storageProvider.IndexStorage()
	s.periodicCall.Run()
	return
}

func (s *spaceDeleter) Close(ctx context.Context) (err error) {
	s.periodicCall.Close()
	return
}

func (s *spaceDeleter) delete(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			log.Error("deletion process failed", zap.Error(err))
		} else {
			log.Debug("deletion process finished")
		}
		// this is needed to reliably test the deletion process
		s.testOnce.Do(func() {
			close(s.testChan)
		})
	}()
	select {
	// waiting for nodes to sync before we start deletion process
	case <-s.syncWaiter:
	case <-ctx.Done():
		return ctx.Err()
	}
	lastRecordId, err := s.deletionStorage.DeletionLogId(ctx)
	if err != nil && !errors.Is(err, nodestorage.ErrNoDeletionLogId) {
		return err
	}
	log.Debug("getting deletion log", zap.Int("limit", logLimit), zap.String("lastRecordId", lastRecordId))
	recs, err := s.coordClient.DeletionLog(ctx, lastRecordId, logLimit)
	if err != nil {
		return err
	}
	log.Debug("got deletion records", zap.String("lastRecordId", lastRecordId), zap.Int("len(records)", len(recs)))
	for _, rec := range recs {
		err = s.processDeletionRecord(ctx, rec)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *spaceDeleter) processDeletionRecord(ctx context.Context, rec *coordinatorproto.DeletionLogRecord) (err error) {
	log := log.With(zap.String("spaceId", rec.SpaceId), zap.String("deletionLogId", rec.Id), zap.String("status", rec.Status.String()))

	prevStatus, err := s.deletionStorage.SpaceStatus(ctx, rec.SpaceId)
	if err != nil {
		return err
	}

	deleteSpace := func() error {
		if prevStatus == nodestorage.SpaceStatusArchived {
			// the space lives in the archive store, not on disk: delete the
			// object directly instead of restoring it just to delete the db
			if aErr := s.archiveStore.Delete(ctx, rec.SpaceId); aErr != nil && !errors.Is(aErr, archivestore.ErrNotFound) && !errors.Is(aErr, archivestore.ErrDisabled) {
				return aErr
			}
			if hErr := s.nodeHead.DeleteHeads(rec.SpaceId); hErr != nil {
				log.Warn("can't delete heads for archived space", zap.Error(hErr))
			}
			return s.deletionStorage.SetSpaceStatus(ctx, rec.SpaceId, nodestorage.SpaceStatusRemove, rec.Id)
		}
		// deleting space storage
		err = s.storageProvider.DeleteSpaceStorage(ctx, rec.SpaceId)
		if err != nil && !errors.Is(err, spacestorage.ErrSpaceStorageMissing) {
			return err
		}
		// remove a leftover archive object if any (restore keeps objects)
		if aErr := s.archiveStore.Delete(ctx, rec.SpaceId); aErr != nil && !errors.Is(aErr, archivestore.ErrNotFound) && !errors.Is(aErr, archivestore.ErrDisabled) {
			log.Warn("can't delete archive object", zap.Error(aErr))
		}
		return s.deletionStorage.SetSpaceStatus(ctx, rec.SpaceId, nodestorage.SpaceStatusRemove, rec.Id)
	}
	if prevStatus == nodestorage.SpaceStatusRemove {
		log.Debug("space is already removed")
		err := s.deletionStorage.SetDeletionLogId(ctx, rec.Id)
		if err != nil {
			return err
		}
		return nil
	}

	switch rec.Status {
	case coordinatorproto.DeletionLogRecordStatus_Ok:
		log.Debug("received deletion cancel record")
		status := nodestorage.SpaceStatusOk
		if !s.nodeConf.IsResponsible(rec.SpaceId) {
			status = nodestorage.SpaceStatusNotResponsible
		}
		err := s.deletionStorage.SetSpaceStatus(ctx, rec.SpaceId, status, rec.Id)
		if err != nil {
			return err
		}
	case coordinatorproto.DeletionLogRecordStatus_RemovePrepare:
		log.Debug("received deletion prepare record")
		err := s.deletionStorage.SetSpaceStatus(ctx, rec.SpaceId, nodestorage.SpaceStatusRemovePrepare, rec.Id)
		if err != nil {
			return err
		}
	case coordinatorproto.DeletionLogRecordStatus_Remove:
		log.Debug("received deletion record")
		err := deleteSpace()
		if err != nil {
			return err
		}
	case coordinatorproto.DeletionLogRecordStatus_OwnershipChange:
		log.Debug("received ownership change record")
		err := s.deletionStorage.SetDeletionLogId(ctx, rec.Id)
		if err != nil {
			return err
		}
	}
	return nil
}
