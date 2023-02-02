package coldsync

import (
	"context"
	"fmt"
	"github.com/anytypeio/any-sync-node/nodestorage"
	"github.com/anytypeio/any-sync-node/nodesync/nodesyncproto"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/net/pool"
	"os"
)

const CName = "node.nodesync.coldsync"

func New() ColdSync {
	return new(coldSync)
}

type ColdSync interface {
	Sync(ctx context.Context, spaceId string, peerId string) (err error)
	ColdSyncHandle(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error
	app.Component
}

type coldSync struct {
	pool    pool.Pool
	storage nodestorage.NodeStorage
}

func (c *coldSync) Init(a *app.App) (err error) {
	c.pool = a.MustComponent(pool.CName).(pool.Service).NewPool("coldsync")
	c.storage = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	return
}

func (c *coldSync) Name() (name string) {
	return CName
}

func (c *coldSync) Sync(ctx context.Context, spaceId, peerId string) (err error) {
	return c.storage.TryLockAndDo(spaceId, func() error {
		if c.storage.SpaceExists(spaceId) {
			return fmt.Errorf("unable to cold sync: space exists")
		}
		return c.coldSync(ctx, spaceId, peerId)
	})
}

func (c *coldSync) coldSync(ctx context.Context, spaceId, peerId string) (err error) {
	p, err := c.pool.Dial(ctx, peerId)
	if err != nil {
		return
	}
	defer func() {
		_ = p.Close()
	}()
	stream, err := nodesyncproto.NewDRPCNodeSyncClient(p).ColdSync(ctx, &nodesyncproto.ColdSyncRequest{
		SpaceId: spaceId,
	})
	if err != nil {
		return
	}
	rd := &streamReader{
		dir:    c.storage.StoreDir("." + spaceId),
		stream: stream,
	}
	if err = rd.Read(ctx); err != nil {
		_ = os.RemoveAll(rd.dir)
		return
	}
	return os.Rename(rd.dir, c.storage.StoreDir(spaceId))
}

func (c *coldSync) ColdSyncHandle(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	err := c.storage.TryLockAndDo(req.SpaceId, func() error {
		return c.coldSyncHandle(req.SpaceId, stream)
	})
	// TODO: if unable to lock - force headsync should be called
	if err != nil {
		return err
	}
	return nil
}

func (c *coldSync) coldSyncHandle(spaceId string, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	sw := &streamWriter{
		dir:    c.storage.StoreDir(spaceId),
		stream: stream,
	}
	return sw.Write()
}
