package nodespace

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/mock_commonspace"
	"github.com/anyproto/any-sync/commonspace/object/acl/syncacl/mock_syncacl"
	"github.com/anyproto/any-sync/consensus/consensusclient/mock_consensusclient"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/stretchr/testify/require"
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
		// one attempt only, which the mock checks
		s.addLog(ctx, spaceId, root)
	})
}

func TestNodeSpace_Init(t *testing.T) {
	const spaceId = "spaceId"
	root := &consensusproto.RawRecordWithId{Id: "aclId", Payload: []byte("root")}

	newSpace := func(t *testing.T) (*nodeSpace, *mock_commonspace.MockSpace, *mock_consensusclient.MockService) {
		ctrl := gomock.NewController(t)
		sp := mock_commonspace.NewMockSpace(ctrl)
		acl := mock_syncacl.NewMockSyncAcl(ctrl)
		cons := mock_consensusclient.NewMockService(ctrl)
		sp.EXPECT().Id().Return(spaceId).AnyTimes()
		sp.EXPECT().Acl().Return(acl).AnyTimes()
		acl.EXPECT().Root().Return(root).AnyTimes()
		acl.EXPECT().Id().Return(root.Id).AnyTimes()
		s, err := newNodeSpace(sp, cons, nil, nil)
		require.NoError(t, err)
		return s, sp, cons
	}

	t.Run("the log is created before the space watches it", func(t *testing.T) {
		s, sp, cons := newSpace(t)
		gomock.InOrder(
			sp.EXPECT().Init(gomock.Any()).Return(nil),
			cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(consensuserr.ErrUnexpected),
			cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(consensuserr.ErrLogExists),
			cons.EXPECT().Watch(spaceId, s).Return(nil),
		)
		require.NoError(t, s.Init(context.Background()))
	})
	t.Run("the space loads when every attempt fails", func(t *testing.T) {
		s, sp, cons := newSpace(t)
		gomock.InOrder(
			sp.EXPECT().Init(gomock.Any()).Return(nil),
			cons.EXPECT().AddLog(gomock.Any(), spaceId, root).Return(consensuserr.ErrUnexpected).Times(addLogAttempts),
			cons.EXPECT().Watch(spaceId, s).Return(nil),
		)
		require.NoError(t, s.Init(context.Background()))
	})
}
