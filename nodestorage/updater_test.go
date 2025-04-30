package nodestorage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpaceUpdater(t *testing.T) {
	var processedUpdates []SpaceUpdate
	var mu sync.Mutex
	updateFunc := func(updates []SpaceUpdate) {
		mu.Lock()
		defer mu.Unlock()
		processedUpdates = append(processedUpdates, updates...)
	}
	updater := newSpaceUpdater(updateFunc)
	now := time.Now()
	updates := []SpaceUpdate{
		{SpaceId: "space1", OldHash: "old1", NewHash: "new1", Updated: now.Add(-2 * time.Hour)},
		{SpaceId: "space1", OldHash: "old2", NewHash: "new2", Updated: now.Add(-1 * time.Hour)},
		{SpaceId: "space1", OldHash: "old3", NewHash: "new3", Updated: now},

		{SpaceId: "space2", OldHash: "old1", NewHash: "new1", Updated: now.Add(-1 * time.Hour)},
		{SpaceId: "space2", OldHash: "old2", NewHash: "new2", Updated: now},

		{SpaceId: "space3", OldHash: "old1", NewHash: "new1", Updated: now},
	}
	for _, update := range updates {
		err := updater.Add(update)
		require.NoError(t, err, "Failed to add update")
	}
	updater.Run()
	err := updater.Close()
	require.NoError(t, err, "Failed to close updater")
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, processedUpdates, 3, "Should have exactly 3 updates")
	spaceUpdates := make(map[string]SpaceUpdate)
	for _, update := range processedUpdates {
		spaceUpdates[update.SpaceId] = update
	}
	update, exists := spaceUpdates["space1"]
	require.True(t, exists, "Missing update for space1")
	require.Equal(t, "new3", update.NewHash, "For space1, should have the latest update")
	require.Equal(t, "old3", update.OldHash, "For space1, should have the latest update")

	update, exists = spaceUpdates["space2"]
	require.True(t, exists, "Missing update for space2")
	require.Equal(t, "new2", update.NewHash, "For space2, should have the latest update")
	require.Equal(t, "old2", update.OldHash, "For space2, should have the latest update")

	update, exists = spaceUpdates["space3"]
	require.True(t, exists, "Missing update for space3")
	require.Equal(t, "new1", update.NewHash, "For space3, should have the latest update")
	require.Equal(t, "old1", update.OldHash, "For space3, should have the latest update")
}
