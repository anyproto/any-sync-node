package spacedeleter

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/consensus/consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/periodicsync"
	"go.uber.org/zap"

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
	consClient      consensusclient.Service
	deletionStorage nodestorage.IndexStorage
	spaceService    nodespace.Service
	storageProvider nodestorage.NodeStorage
	syncWaiter      <-chan struct{}

	testOnce sync.Once
	testChan chan struct{}
}

func (s *spaceDeleter) Init(a *app.App) (err error) {
	s.periodicCall = periodicsync.NewPeriodicSync(periodicDeleteSecs, deleteTimeout, s.delete, log)
	s.coordClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	s.spaceService = a.MustComponent(nodespace.CName).(nodespace.Service)
	s.consClient = a.MustComponent(consensusclient.CName).(consensusclient.Service)
	s.storageProvider = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	s.syncWaiter = a.MustComponent(nodesync.CName).(nodesync.NodeSync).WaitSyncOnStart()
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
	lastRecordId, err := s.deletionStorage.LastRecordId(ctx)
	if err != nil && !errors.Is(err, nodestorage.ErrNoLastRecordId) {
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
	log := log.With(zap.String("spaceId", rec.SpaceId))
	deleteSpace := func() error {
		// trying to get the storage
		st, err := s.storageProvider.WaitSpaceStorage(ctx, rec.SpaceId)
		if err == nil {
			// getting the log Id
			acl, err := st.AclStorage()
			if err != nil {
				st.Close(ctx)
				return err
			}
			logId := acl.Id()
			st.Close(ctx)
			// deleting log from consensus
			err = s.consClient.DeleteLog(ctx, logId)
			if err != nil && !errors.Is(err, consensuserr.ErrLogNotFound) {
				return err
			}
		}
		// deleting space storage
		err = s.storageProvider.DeleteSpaceStorage(ctx, rec.SpaceId)
		if err != nil && !errors.Is(err, spacestorage.ErrSpaceStorageMissing) {
			return err
		}
		return s.deletionStorage.SetSpaceStatus(ctx, rec.SpaceId, nodestorage.SpaceStatusRemove, rec.Id)
	}
	switch rec.Status {
	case coordinatorproto.DeletionLogRecordStatus_Ok:
		log.Debug("received deletion cancel record")
		err := s.deletionStorage.SetSpaceStatus(ctx, rec.SpaceId, nodestorage.SpaceStatusOk, rec.Id)
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
	}
	return nil
}
