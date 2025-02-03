//go:generate mockgen -destination mock_coldsync/mock_coldsync.go github.com/anyproto/any-sync-node/nodesync/coldsync ColdSync
package coldsync

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/net/pool"
	"go.uber.org/zap"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-node/nodespace"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync/nodesyncproto"
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
	c.pool = a.MustComponent(pool.CName).(pool.Pool)
	c.storage = a.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	c.nodespace = a.MustComponent(nodespace.CName).(nodespace.Service)
	return
}

func (c *coldSync) Name() (name string) {
	return CName
}

func (c *coldSync) Sync(ctx context.Context, spaceId, peerId string) (err error) {
	return c.storage.TryLockAndDo(ctx, spaceId, func() error {
		if c.storage.SpaceExists(spaceId) {
			return ErrSpaceExistsLocally
		}
		return c.coldSync(ctx, spaceId, peerId)
	})
}

func (c *coldSync) coldSync(ctx context.Context, spaceId, peerId string) (err error) {
	p, err := c.pool.GetOneOf(ctx, []string{peerId})
	if err != nil {
		return
	}
	return p.DoDrpc(ctx, func(conn drpc.Conn) error {
		stream, err := nodesyncproto.NewDRPCNodeSyncClient(conn).ColdSync(ctx, &nodesyncproto.ColdSyncRequest{
			SpaceId:      spaceId,
			ProtocolType: nodesyncproto.ColdSyncProtocolType_AnystoreSqlite,
		})
		if err != nil {
			return err
		}
		rd := &streamReader{
			dir:    c.storage.StoreDir("." + spaceId),
			stream: stream,
		}
		if err = rd.Read(ctx); err != nil {
			_ = os.RemoveAll(rd.dir)
			_ = stream.Close()
			if err == io.EOF {
				return ErrRemoteSpaceLocked
			} else {
				return err
			}
		}
		return os.Rename(rd.dir, c.storage.StoreDir(spaceId))
	})
}

func (c *coldSync) ColdSyncHandle(req *nodesyncproto.ColdSyncRequest, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	if req.ProtocolType != nodesyncproto.ColdSyncProtocolType_AnystoreSqlite {
		return errors.New("unsupported protocol type")
	}
	err := c.storage.DumpStorage(context.Background(), req.SpaceId, func(path string) error {
		return c.coldSyncHandle(req.SpaceId, path, stream)
	})
	if err != nil {
		log.Info("handle error", zap.Error(err))
		return err
	}
	return nil
}

func (c *coldSync) coldSyncHandle(spaceId, path string, stream nodesyncproto.DRPCNodeSync_ColdSyncStream) error {
	sw := &streamWriter{
		dir:    path,
		stream: stream,
	}
	return sw.Write()
}
