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

const testSpaceId = "spaceId"

var testRoot = &consensusproto.RawRecordWithId{Id: "aclId", Payload: []byte("root")}

// newTestSpace returns a node space whose space and acl are mocks with testSpaceId and testRoot
func newTestSpace(t *testing.T) (*nodeSpace, *mock_commonspace.MockSpace, *mock_consensusclient.MockService) {
	ctrl := gomock.NewController(t)
	sp := mock_commonspace.NewMockSpace(ctrl)
	acl := mock_syncacl.NewMockSyncAcl(ctrl)
	cons := mock_consensusclient.NewMockService(ctrl)
	sp.EXPECT().Id().Return(testSpaceId).AnyTimes()
	sp.EXPECT().Acl().Return(acl).AnyTimes()
	acl.EXPECT().Root().Return(testRoot).AnyTimes()
	acl.EXPECT().Id().Return(testRoot.Id).AnyTimes()
	s, err := newNodeSpace(sp, cons, nil, nil)
	require.NoError(t, err)
	return s, sp, cons
}

func TestNodeSpace_addLog(t *testing.T) {
	t.Run("a created log needs one attempt", func(t *testing.T) {
		s, _, cons := newTestSpace(t)
		cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(nil)
		s.addLog(context.Background(), testRoot)
	})
	t.Run("an existing log needs one attempt", func(t *testing.T) {
		s, _, cons := newTestSpace(t)
		cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(consensuserr.ErrLogExists)
		s.addLog(context.Background(), testRoot)
	})
	t.Run("a failed attempt is retried until the log exists", func(t *testing.T) {
		s, _, cons := newTestSpace(t)
		gomock.InOrder(
			cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(consensuserr.ErrUnexpected),
			cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(consensuserr.ErrLogExists),
		)
		s.addLog(context.Background(), testRoot)
	})
	t.Run("attempts stop after addLogAttempts", func(t *testing.T) {
		s, _, cons := newTestSpace(t)
		cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(errors.New("network")).Times(addLogAttempts)
		s.addLog(context.Background(), testRoot)
	})
	t.Run("a permanent error is not retried", func(t *testing.T) {
		s, _, cons := newTestSpace(t)
		cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(consensuserr.ErrForbidden)
		s.addLog(context.Background(), testRoot)
	})
	t.Run("a done context stops the retries", func(t *testing.T) {
		s, _, cons := newTestSpace(t)
		ctx, cancel := context.WithCancel(context.Background())
		cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).DoAndReturn(
			func(context.Context, string, *consensusproto.RawRecordWithId) error {
				cancel()
				return consensuserr.ErrUnexpected
			})
		// one attempt only, which the mock checks
		s.addLog(ctx, testRoot)
	})
}

func TestNodeSpace_Init(t *testing.T) {
	t.Run("the log is created before the space watches it", func(t *testing.T) {
		s, sp, cons := newTestSpace(t)
		gomock.InOrder(
			sp.EXPECT().Init(gomock.Any()).Return(nil),
			cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(consensuserr.ErrUnexpected),
			cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(consensuserr.ErrLogExists),
			cons.EXPECT().Watch(testSpaceId, s).Return(nil),
		)
		require.NoError(t, s.Init(context.Background()))
	})
	t.Run("the space loads when every attempt fails", func(t *testing.T) {
		s, sp, cons := newTestSpace(t)
		gomock.InOrder(
			sp.EXPECT().Init(gomock.Any()).Return(nil),
			cons.EXPECT().AddLog(gomock.Any(), testSpaceId, testRoot).Return(consensuserr.ErrUnexpected).Times(addLogAttempts),
			cons.EXPECT().Watch(testSpaceId, s).Return(nil),
		)
		require.NoError(t, s.Init(context.Background()))
	})
}
