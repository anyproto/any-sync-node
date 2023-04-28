package nodespace

import (
	"errors"
	"github.com/anytypeio/any-sync/commonspace"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"github.com/anytypeio/any-sync/net/peer"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"storj.io/drpc"
	"sync/atomic"
	"time"
)

var (
	errUnexpectedMessage = errors.New("unexpected message")
)

var lastMsgId = atomic.Uint64{}

type streamHandler struct {
	s *service
}

func (s *streamHandler) OpenStream(ctx context.Context, p peer.Peer) (stream drpc.Stream, tags []string, err error) {
	log.DebugCtx(ctx, "open outgoing stream", zap.String("peerId", p.Id()))
	if stream, err = spacesyncproto.NewDRPCSpaceSyncClient(p).ObjectSyncStream(ctx); err != nil {
		log.WarnCtx(ctx, "open outgoing stream error", zap.String("peerId", p.Id()), zap.Error(err))
		return
	}
	log.DebugCtx(ctx, "outgoing stream opened", zap.String("peerId", p.Id()))
	return
}

func (s *streamHandler) HandleMessage(ctx context.Context, peerId string, msg drpc.Message) (err error) {
	syncMsg, ok := msg.(*spacesyncproto.ObjectSyncMessage)
	if !ok {
		err = errUnexpectedMessage
		return
	}
	ctx = peer.CtxWithPeerId(ctx, peerId)
	if syncMsg.SpaceId == "" {
		return s.s.HandleMessage(ctx, peerId, syncMsg)
	}
	err = checkResponsible(ctx, s.s.confService, syncMsg.SpaceId)
	if err != nil {
		log.Debug("message sent to not responsible peer",
			zap.Error(err),
			zap.String("spaceId", syncMsg.SpaceId),
			zap.String("peerId", peerId))
		return spacesyncproto.ErrPeerIsNotResponsible
	}

	space, err := s.s.GetSpace(ctx, syncMsg.SpaceId)
	if err != nil {
		return
	}
	err = space.HandleMessage(ctx, commonspace.HandleMessage{
		Id:       lastMsgId.Add(1),
		Deadline: time.Now().Add(time.Minute),
		SenderId: peerId,
		Message:  syncMsg,
	})
	return
}

func (s *streamHandler) NewReadMessage() drpc.Message {
	// TODO: we can use sync.Pool here
	return new(spacesyncproto.ObjectSyncMessage)
}
