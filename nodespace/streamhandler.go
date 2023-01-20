package nodespace

import (
	"errors"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"github.com/anytypeio/any-sync/net/peer"
	"golang.org/x/net/context"
	"storj.io/drpc"
)

var (
	errUnexpectedMessage = errors.New("unexpected message")
)

type streamHandler struct {
	s *service
}

func (s *streamHandler) OpenStream(ctx context.Context, p peer.Peer) (stream drpc.Stream, tags []string, err error) {
	if stream, err = spacesyncproto.NewDRPCSpaceSyncClient(p).ObjectSyncStream(ctx); err != nil {
		return
	}
	return
}

func (s *streamHandler) HandleMessage(ctx context.Context, peerId string, msg drpc.Message) (err error) {
	syncMsg, ok := msg.(*spacesyncproto.ObjectSyncMessage)
	if !ok {
		err = errUnexpectedMessage
		return
	}
	ctx = peer.CtxWithPeerId(ctx, peerId)
	space, err := s.s.GetSpace(ctx, syncMsg.SpaceId)
	if err != nil {
		return
	}
	err = space.ObjectSync().HandleMessage(ctx, peerId, syncMsg)
	return
}

func (s *streamHandler) NewReadMessage() drpc.Message {
	// TODO: we can use sync.Pool here
	return new(spacesyncproto.ObjectSyncMessage)
}
