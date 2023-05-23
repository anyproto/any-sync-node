//go:generate mockgen -destination mock_nodespace/mock_nodespace.go github.com/anyproto/any-sync-node/nodespace Service
package nodespace

import (
	"context"
	"github.com/anyproto/any-sync-node/nodehead"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/nodeconf"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"time"
)

const CName = "node.nodespace"

var log = logger.NewNamed(CName)

func New() Service {
	return &service{}
}

type Service interface {
	GetSpace(ctx context.Context, id string) (commonspace.Space, error)
	DeleteSpace(ctx context.Context, spaceId, changeId string, changePayload []byte) (err error)
	Cache() ocache.OCache
	StreamPool() streampool.StreamPool
	app.ComponentRunnable
}

type service struct {
	conf                 commonspace.Config
	spaceCache           ocache.OCache
	commonSpace          commonspace.SpaceService
	confService          nodeconf.Service
	spaceStorageProvider nodestorage.NodeStorage
	streamPool           streampool.StreamPool
	nodeHead             nodehead.NodeHead
	metric               metric.Metric
}

func (s *service) Init(a *app.App) (err error) {
	s.conf = a.MustComponent("config").(commonspace.ConfigGetter).GetSpace()
	s.commonSpace = a.MustComponent(commonspace.CName).(commonspace.SpaceService)
	s.confService = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	s.spaceStorageProvider = a.MustComponent(spacestorage.CName).(nodestorage.NodeStorage)
	s.nodeHead = a.MustComponent(nodehead.CName).(nodehead.NodeHead)
	s.streamPool = a.MustComponent(streampool.CName).(streampool.Service).NewStreamPool(&streamHandler{s: s}, streampool.StreamConfig{
		SendQueueSize:    100,
		DialQueueWorkers: 4,
		DialQueueSize:    1000,
	})
	s.spaceCache = ocache.New(
		s.loadSpace,
		ocache.WithLogger(log.Sugar()),
		ocache.WithGCPeriod(time.Minute),
		ocache.WithTTL(time.Duration(s.conf.GCTTL)*time.Second),
		ocache.WithPrometheus(a.MustComponent(metric.CName).(metric.Metric).Registry(), "space", "cache"),
	)
	s.metric = a.MustComponent(metric.CName).(metric.Metric)
	return spacesyncproto.DRPCRegisterSpaceSync(a.MustComponent(server.CName).(server.DRPCServer), &rpcHandler{s})
}

func (s *service) Name() (name string) {
	return CName
}

func (s *service) Run(ctx context.Context) (err error) {
	return
}

func (s *service) StreamPool() streampool.StreamPool {
	return s.streamPool
}

func (s *service) GetSpace(ctx context.Context, id string) (commonspace.Space, error) {
	v, err := s.spaceCache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return v.(commonspace.Space), nil
}

func (s *service) HandleMessage(ctx context.Context, senderId string, req *spacesyncproto.ObjectSyncMessage) (err error) {
	var msg = &spacesyncproto.SpaceSubscription{}
	if err = msg.Unmarshal(req.Payload); err != nil {
		return
	}
	log.InfoCtx(ctx, "got subscription message", zap.Strings("spaceIds", msg.SpaceIds))
	if msg.Action == spacesyncproto.SpaceSubscriptionAction_Subscribe {
		return s.streamPool.AddTagsCtx(ctx, msg.SpaceIds...)
	} else {
		return s.streamPool.RemoveTagsCtx(ctx, msg.SpaceIds...)
	}
}

func (s *service) DeleteSpace(ctx context.Context, spaceId, changeId string, changePayload []byte) (err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return
	}
	if !slices.Contains(s.confService.CoordinatorPeers(), peerId) {
		err = nodesyncproto.ErrExpectedCoordinator
		return
	}
	space, err := s.spaceCache.Get(ctx, spaceId)
	if err != nil {
		return
	}
	return space.(commonspace.Space).DeleteSpace(ctx, &treechangeproto.RawTreeChangeWithId{
		RawChange: changePayload,
		Id:        changeId,
	})
}

func (s *service) loadSpace(ctx context.Context, id string) (value ocache.Object, err error) {
	defer func() {
		log.InfoCtx(ctx, "space loaded", zap.String("id", id), zap.Error(err))
	}()
	cc, err := s.commonSpace.NewSpace(ctx, id)
	if err != nil {
		return
	}
	ns, err := newNodeSpace(cc)
	if err != nil {
		return
	}
	if err = ns.Init(ctx); err != nil {
		return
	}
	return ns, nil
}

func (s *service) Close(ctx context.Context) (err error) {
	return s.spaceCache.Close()
}

func (s *service) Cache() ocache.OCache {
	return s.spaceCache
}
