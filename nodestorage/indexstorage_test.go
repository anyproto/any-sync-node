package nodestorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexStorage_UpdateLastAccess(t *testing.T) {
	tempDir := t.TempDir()
	fx, err := createTestIndexStorage(ctx, tempDir)
	require.NoError(t, err)
	defer fx.Close()

	require.NoError(t, fx.UpdateLastAccess(ctx, "space1"))
	require.NoError(t, fx.UpdateLastAccess(ctx, "space1"))

	require.NoError(t, fx.UpdateHash(ctx, SpaceUpdate{
		SpaceId: "space2",
		OldHash: "old",
		NewHash: "new",
		Updated: time.Now(),
	}))
	require.NoError(t, fx.UpdateLastAccess(ctx, "space2"))
	time.Sleep(time.Second)
	require.NoError(t, fx.ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
		assert.True(t, update.Updated.Before(time.Now()))
		assert.True(t, update.SpaceId == "space1" || update.SpaceId == "space2")
		if update.SpaceId == "space2" {
			assert.Equal(t, "old", update.OldHash)
			assert.Equal(t, "new", update.NewHash)
		}
		return true, nil
	}))
}

func Test_migrateToSingleCollection(t *testing.T) {
	tempDir := t.TempDir()
	data, err := os.ReadFile("./testdata/index_store_v1.db")
	require.NoError(t, err)
	anyStorePath := filepath.Join(tempDir, "index_store_v1.db")
	require.NoError(t, os.WriteFile(anyStorePath, data, 0644))
	db, err := anystore.Open(ctx, anyStorePath, anyStoreConfig())
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, migrateToSingleCollection(ctx, db))

	collNames, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.NotContains(t, collNames, "hashesIndex")
	assert.NotContains(t, collNames, "deletionIndex")
	assert.Contains(t, collNames, spaceCollName)
	assert.Contains(t, collNames, settingsCollName)
}
