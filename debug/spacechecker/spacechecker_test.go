package spacechecker

import (
	"testing"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-node/nodestorage"
)

func TestLocalStatusString(t *testing.T) {
	assert.Equal(t, "ok", localStatusString(nodestorage.SpaceStatusOk))
	assert.Equal(t, "removed", localStatusString(nodestorage.SpaceStatusRemove))
	assert.Equal(t, "remPrepare", localStatusString(nodestorage.SpaceStatusRemovePrepare))
	assert.Equal(t, "archived", localStatusString(nodestorage.SpaceStatusArchived))
	assert.Equal(t, "error", localStatusString(nodestorage.SpaceStatusError))
	assert.Equal(t, "notResponsible", localStatusString(nodestorage.SpaceStatusNotResponsible))
}

func TestCoordStatusString(t *testing.T) {
	assert.Equal(t, "ok", coordStatusString(coordinatorproto.SpaceStatus_SpaceStatusCreated))
	assert.Equal(t, "remPrepare", coordStatusString(coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion))
	assert.Equal(t, "remPrepare", coordStatusString(coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted))
	assert.Equal(t, "removed", coordStatusString(coordinatorproto.SpaceStatus_SpaceStatusDeleted))
	assert.Equal(t, "notExists", coordStatusString(coordinatorproto.SpaceStatus_SpaceStatusNotExists))
}

func TestValidate(t *testing.T) {
	s := &spaceChecker{}

	tests := []struct {
		name        string
		local       string
		coord       string
		responsible bool
		exists      bool
		hasProblems bool
	}{
		{"ok-ok-true-true", "ok", "ok", true, true, false},
		{"ok-archived-true-false", "archived", "ok", true, false, false},
		{"remPrepare-remPrepare-true-true", "remPrepare", "remPrepare", true, true, false},
		{"remPrepare-remPrepare-false-false", "remPrepare", "remPrepare", false, false, false},
		{"removed-removed-false", "removed", "removed", false, false, false},
		{"removed-removed-true-false", "removed", "removed", true, false, false},
		// invalid states
		{"ok-ok-true-false", "ok", "ok", true, false, true},
		{"ok-ok-false-true", "ok", "ok", false, true, true},
		{"removed-ok-true-true", "ok", "removed", true, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := Result{
				IsResponsible:      tt.responsible,
				SpaceStorageExists: tt.exists,
			}
			s.validate(&res, tt.local, tt.coord)
			if tt.hasProblems {
				assert.NotEmpty(t, res.Problems)
			} else {
				assert.Empty(t, res.Problems)
			}
		})
	}
}
