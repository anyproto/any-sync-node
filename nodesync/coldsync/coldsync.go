//go:generate mockgen -destination mock_coldsync/mock_coldsync.go github.com/anyproto/any-sync-node/nodesync/coldsync ColdSync
package coldsync

import (
	"context"
	"errors"
	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/net/pool"
	"go.uber.org/zap"
	"io"
	"os"
)

const CName = "node.nodesync.coldsync"

var log = logger.NewNamed(CName)

var (
	ErrSpaceExistsLocally = errors.New("space exists locally")
	ErrRemoteSpaceLocked  = errors.New("remote space locked")
)

func New() ColdSync {
	return new(coldSync)
}

type ColdSync interface {
	Sync(ctx context.Context, spaceId string, peerId string) (err error)
	ColdSyncHandle(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error
	app.Component
}

type coldSync struct {
	pool      pool.Pool
	storage   nodestorage.NodeStorage
	nodespace nodespace.Service
}

func (c *coldSync) Init(a *app.App) (err error) {
	c.pool = a.MustComponent(pool.CName).(pool.Service).NewPool("coldsync")
	c.storage = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	c.nodespace = a.MustComponent(nodespace.CName).(nodespace.Service)
	return
}

func (c *coldSync) Name() (name string) {
	return CName
}

func (c *coldSync) Sync(ctx context.Context, spaceId, peerId string) (err error) {
	return c.storage.TryLockAndDo(spaceId, func() error {
		if c.storage.SpaceExists(spaceId) {
			return ErrSpaceExistsLocally
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
		if err == io.EOF {
			return ErrRemoteSpaceLocked
		} else {
			return err
		}
	}
	return os.Rename(rd.dir, c.storage.StoreDir(spaceId))
}

func (c *coldSync) ColdSyncHandle(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	err := c.storage.TryLockAndDo(req.SpaceId, func() error {
		return c.coldSyncHandle(req.SpaceId, stream)
	})
	if err == nodestorage.ErrLocked {
		// TODO: we need to force headSync here
		_, err = c.nodespace.GetSpace(stream.Context(), req.SpaceId)
	}
	if err != nil {
		log.Info("handle error", zap.Error(err))
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
