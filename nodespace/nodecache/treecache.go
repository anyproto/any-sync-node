package nodecache

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/metric"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodespace"
)

var log = logger.NewNamed("treecache")
var ErrCacheObjectWithoutTree = errors.New("cache object contains no tree")

type ctxKey int

const (
	spaceKey ctxKey = iota
	payloadKey
)

type treeCache struct {
	gcttl       int
	cache       ocache.OCache
	nodeService nodespace.Service
}

func (c *treeCache) ValidateAndPutTree(ctx context.Context, spaceId string, payload treestorage.TreeStorageCreatePayload) error {
	ctx = context.WithValue(ctx, spaceKey, spaceId)
	ctx = context.WithValue(ctx, payloadKey, payload)
	_, err := c.cache.Get(ctx, payload.RootRawChange.Id)
	return err
}

func New(ttl int) treemanager.TreeManager {
	return &treeCache{
		gcttl: ttl,
	}
}

func (c *treeCache) Run(ctx context.Context) (err error) {
	return nil
}

func (c *treeCache) Close(ctx context.Context) (err error) {
	return c.cache.Close()
}

func (c *treeCache) Init(a *app.App) (err error) {
	c.nodeService = a.MustComponent(nodespace.CName).(nodespace.Service)
	c.cache = ocache.New(
		func(ctx context.Context, id string) (value ocache.Object, err error) {
			spaceId := ctx.Value(spaceKey).(string)
			space, err := c.nodeService.GetSpace(ctx, spaceId)
			if err != nil {
				return
			}
			payload, ok := ctx.Value(payloadKey).(treestorage.TreeStorageCreatePayload)
			if ok {
				return space.TreeBuilder().PutTree(ctx, payload, nil)
			}
			return space.TreeBuilder().BuildTree(ctx, id, objecttreebuilder.BuildTreeOpts{})
		},
		ocache.WithLogger(log.Sugar()),
		ocache.WithGCPeriod(time.Minute),
		ocache.WithTTL(time.Duration(c.gcttl)*time.Second),
		ocache.WithPrometheus(a.MustComponent(metric.CName).(metric.Metric).Registry(), "tree", "cache"),
	)
	return nil
}

func (c *treeCache) Name() (name string) {
	return treemanager.CName
}

func (c *treeCache) GetTree(ctx context.Context, spaceId, id string) (tr objecttree.ObjectTree, err error) {
	// TODO: check that tree is in space
	ctx = context.WithValue(ctx, spaceKey, spaceId)
	value, err := c.cache.Get(ctx, id)
	if err != nil {
		return
	}
	tr = value.(objecttree.ObjectTree)
	return
}

func (c *treeCache) DeleteTree(ctx context.Context, spaceId, treeId string) (err error) {
	tr, err := c.GetTree(ctx, spaceId, treeId)
	if err != nil {
		return
	}
	err = tr.Delete()
	if err != nil {
		return
	}
	_, err = c.cache.Remove(ctx, treeId)
	return
}

func (c *treeCache) DeleteSpace(ctx context.Context, spaceId string) error {
	log.Debug("space deleted", zap.String("spaceId", spaceId))
	return nil
}

func (c *treeCache) MarkTreeDeleted(ctx context.Context, spaceId, treeId string) error {
	return nil
}
