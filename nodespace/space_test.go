package nodespace

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-sync/consensus/consensusclient/mock_consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNodeSpace_addLog(t *testing.T) {
	const spaceId = "spaceId"
	root := &consensusproto.RawRecordWithId{Id: "aclId", Payload: []byte("root")}

	newSpace := func(t *testing.T) (*nodeSpace, *mock_consensusclient.MockService) {
		cons := mock_consensusclient.NewMockService(gomock.NewController(t))
		return &nodeSpace{consClient: cons, log: log.With()}, cons
	}

	t.Run("a created log needs one attempt", func(t *testing.T) {
		s, cons := newSpace(t)
		cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(nil)
		s.addLog(context.Background(), spaceId, root)
	})
	t.Run("an existing log needs one attempt", func(t *testing.T) {
		s, cons := newSpace(t)
		cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(consensuserr.ErrLogExists)
		s.addLog(context.Background(), spaceId, root)
	})
	t.Run("a failed attempt is retried until the log exists", func(t *testing.T) {
		s, cons := newSpace(t)
		gomock.InOrder(
			cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(consensuserr.ErrUnexpected),
			cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(consensuserr.ErrLogExists),
		)
		s.addLog(context.Background(), spaceId, root)
	})
	t.Run("attempts stop after addLogAttempts", func(t *testing.T) {
		s, cons := newSpace(t)
		cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(errors.New("network")).Times(addLogAttempts)
		s.addLog(context.Background(), spaceId, root)
	})
	t.Run("a permanent error is not retried", func(t *testing.T) {
		s, cons := newSpace(t)
		cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(consensuserr.ErrForbidden)
		s.addLog(context.Background(), spaceId, root)
	})
	t.Run("a done context stops the retries", func(t *testing.T) {
		s, cons := newSpace(t)
		ctx, cancel := context.WithCancel(context.Background())
		cons.EXPECT().AddLog(gomock.Any(), spaceId, root).DoAndReturn(
			func(context.Context, string, *consensusproto.RawRecordWithId) error {
				cancel()
				return consensuserr.ErrUnexpected
			})
		start := time.Now()
		s.addLog(ctx, spaceId, root)
		assert.Less(t, time.Since(start), addLogRetryDelay)
	})
}
