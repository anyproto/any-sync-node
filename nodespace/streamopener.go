package nodespace

import (
	"errors"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/net/streampool/streamhandler"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"storj.io/drpc"
)

var (
	errUnexpectedMessage = errors.New("unexpected message")
)

func NewStreamOpener() streamhandler.StreamHandler {
	return &streamOpener{}
}

type streamOpener struct {
	streamPool  streampool.StreamPool
	spaceGetter Service
}

func (s *streamOpener) Init(a *app.App) (err error) {
	s.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
	s.spaceGetter = a.MustComponent(CName).(Service)
	return
}

func (s *streamOpener) Name() (name string) {
	return streamhandler.CName
}

func (s *streamOpener) OpenStream(ctx context.Context, p peer.Peer) (stream drpc.Stream, tags []string, err error) {
	log.DebugCtx(ctx, "open outgoing stream", zap.String("peerId", p.Id()))
	ctx = peer.CtxWithPeerId(ctx, p.Id())
	conn, err := p.AcquireDrpcConn(ctx)
	if err != nil {
		return
	}
	if stream, err = spacesyncproto.NewDRPCSpaceSyncClient(conn).ObjectSyncStream(ctx); err != nil {
		log.WarnCtx(ctx, "open outgoing stream error", zap.String("peerId", p.Id()), zap.Error(err))
		return
	}
	log.DebugCtx(ctx, "outgoing stream opened", zap.String("peerId", p.Id()))
	return
}

func (s *streamOpener) HandleMessage(peerCtx context.Context, peerId string, msg drpc.Message) (err error) {
	syncMsg, ok := msg.(*objectmessages.HeadUpdate)
	if !ok {
		err = errUnexpectedMessage
		return
	}
	if syncMsg.SpaceId() == "" {
		var msg = &spacesyncproto.SpaceSubscription{}
		if err = msg.Unmarshal(syncMsg.Bytes); err != nil {
			return
		}
		log.InfoCtx(peerCtx, "got subscription message", zap.Strings("spaceIds", msg.SpaceIds))
		if msg.Action == spacesyncproto.SpaceSubscriptionAction_Subscribe {
			return s.streamPool.AddTagsCtx(peerCtx, msg.SpaceIds...)
		} else {
			return s.streamPool.RemoveTagsCtx(peerCtx, msg.SpaceIds...)
		}
	}
	sp, err := s.spaceGetter.GetSpace(peerCtx, syncMsg.SpaceId())
	if err != nil {
		return
	}
	return sp.HandleMessage(peerCtx, syncMsg)
}

func (s *streamOpener) NewReadMessage() drpc.Message {
	return &objectmessages.HeadUpdate{}
}
