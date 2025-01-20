//go:generate mockgen -destination mock_nodespace/mock_nodespace.go github.com/anyproto/any-sync-node/nodespace Service,NodeSpace
package nodespace

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/config"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/syncstatus"
	"github.com/anyproto/any-sync/consensus/consensusclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodespace/treesyncer"
	"github.com/anyproto/any-sync-node/nodestorage"
)

const CName = "node.nodespace"

var log = logger.NewNamed(CName)

func New() Service {
	return &service{}
}

type Service interface {
	GetSpace(ctx context.Context, id string) (NodeSpace, error)
	PickSpace(ctx context.Context, id string) (NodeSpace, error)
	EvictSpace(ctx context.Context, id string) error
	Cache() ocache.OCache
	GetStats(ctx context.Context, id string, treeTop int) (SpaceStats, error)
	app.ComponentRunnable
}

type service struct {
	conf                 config.Config
	spaceCache           ocache.OCache
	commonSpace          commonspace.SpaceService
	confService          nodeconf.Service
	consClient           consensusclient.Service
	spaceStorageProvider nodestorage.NodeStorage
	streamPool           streampool.StreamPool
	nodeHead             nodehead.NodeHead
	metric               metric.Metric
	coordClient          coordinatorclient.CoordinatorClient
}

func (s *service) Init(a *app.App) (err error) {
	s.conf = a.MustComponent("config").(config.ConfigGetter).GetSpace()
	s.commonSpace = a.MustComponent(commonspace.CName).(commonspace.SpaceService)
	s.confService = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	s.spaceStorageProvider = a.MustComponent(spacestorage.CName).(nodestorage.NodeStorage)
	s.nodeHead = a.MustComponent(nodehead.CName).(nodehead.NodeHead)
	s.consClient = a.MustComponent(consensusclient.CName).(consensusclient.Service)
	s.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
	s.spaceCache = ocache.New(
		s.loadSpace,
		ocache.WithLogger(log.Sugar()),
		ocache.WithGCPeriod(time.Minute),
		ocache.WithTTL(time.Duration(s.conf.GCTTL)*time.Second),
		ocache.WithPrometheus(a.MustComponent(metric.CName).(metric.Metric).Registry(), "space", "cache"),
	)
	s.metric = a.MustComponent(metric.CName).(metric.Metric)
	s.coordClient = app.MustComponent[coordinatorclient.CoordinatorClient](a)
	return spacesyncproto.DRPCRegisterSpaceSync(a.MustComponent(server.CName).(server.DRPCServer), &rpcHandler{s})
}

func (s *service) Name() (name string) {
	return CName
}

func (s *service) Run(ctx context.Context) (err error) {
	return
}

func (s *service) EvictSpace(ctx context.Context, id string) (err error) {
	_, err = s.spaceCache.Remove(ctx, id)
	return
}

func (s *service) PickSpace(ctx context.Context, id string) (NodeSpace, error) {
	v, err := s.spaceCache.Pick(ctx, id)
	if err != nil {
		return nil, err
	}
	return v.(NodeSpace), nil
}

var (
	ErrDoesntSupportStats   = errors.New("SpaceStorage doesn't support nodestorage.SpaceStats")
	ErrSpaceStorageIsLocked = errors.New("SpaceStorage is locked, try again later")
)

type SpaceStats struct {
	Storage nodestorage.SpaceStats `json:"storage"`
	Acl     struct {
		Readers int `json:"readers"`
		Writers int `json:"writers"`
	} `json:"acl"`
}

func (s *service) GetStats(ctx context.Context, id string, treeTop int) (spaceStats SpaceStats, err error) {
	space, err := s.GetSpace(ctx, id)
	if err != nil {
		return
	}

	storage, ok := space.Storage().(nodestorage.NodeStorageStats)
	if ok {
		spaceStats.Storage, err = storage.GetSpaceStats(treeTop)
		if err != nil {
			return
		}
	} else {
		err = ErrDoesntSupportStats
		return
	}
	space.Acl().Lock()
	defer space.Acl().Unlock()
	for _, acc := range space.Acl().AclState().CurrentAccounts() {
		if !acc.Permissions.NoPermissions() {
			if acc.Permissions.CanWrite() {
				spaceStats.Acl.Writers++
			} else {
				spaceStats.Acl.Readers++
			}
		}
	}

	return
}

func (s *service) GetSpace(ctx context.Context, id string) (NodeSpace, error) {
	v, err := s.spaceCache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	space := v.(NodeSpace)
	return space, nil
}

func (s *service) loadSpace(ctx context.Context, id string) (value ocache.Object, err error) {
	defer func() {
		log.InfoCtx(ctx, "space loaded", zap.String("id", id), zap.Error(err))
	}()
	if err = s.checkDeletionStatus(ctx, id); err != nil {
		return nil, err
	}
	cc, err := s.commonSpace.NewSpace(ctx, id, commonspace.Deps{
		TreeSyncer: treesyncer.New(id),
		SyncStatus: syncstatus.NewNoOpSyncStatus(),
	})
	if err != nil {
		return
	}
	ns, err := newNodeSpace(cc, s.consClient, s.spaceStorageProvider)
	if err != nil {
		return
	}
	if err = ns.Init(ctx); err != nil {
		return
	}
	return ns, nil
}

func (s *service) checkDeletionStatus(ctx context.Context, spaceId string) (err error) {
	delStorage := s.spaceStorageProvider.IndexStorage()
	status, err := delStorage.SpaceStatus(ctx, spaceId)
	if err != nil {
		if errors.Is(err, nodestorage.ErrUnknownSpaceId) {
			return nil
		}
		return err
	}
	if status == nodestorage.SpaceStatusRemove {
		return spacesyncproto.ErrSpaceIsDeleted
	}
	return nil
}

func (s *service) Close(ctx context.Context) (err error) {
	return s.spaceCache.Close()
}

func (s *service) Cache() ocache.OCache {
	return s.spaceCache
}
