package nodestorage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffMigration(t *testing.T) {
	ctx := context.Background()

	testLogger := logger.NewNamed("test")

	t.Run("migration_skipped_when_already_completed", func(t *testing.T) {
		tempDir := t.TempDir()
		// Create index storage
		is, err := createTestIndexStorage(ctx, tempDir)
		require.NoError(t, err)
		defer is.Close()

		// Set migration state to version 3
		err = setTestMigrationVersion(ctx, is, targetMigrationVersion)
		require.NoError(t, err)

		// Run migration
		migration, err := newDiffMigration(is, testLogger)
		require.NoError(t, err)

		err = migration.Run(ctx)
		require.NoError(t, err)

		// Verify migration state is still 3
		version, err := getTestMigrationVersion(ctx, is)
		require.NoError(t, err)
		assert.Equal(t, targetMigrationVersion, version)
	})

	t.Run("migration_copies_nh_to_oh", func(t *testing.T) {
		tempDir := t.TempDir()
		// Create index storage
		is, err := createTestIndexStorage(ctx, tempDir)
		require.NoError(t, err)
		defer is.Close()

		// Add test data with only 'nh' field
		testData := []SpaceUpdate{
			{SpaceId: "space1", NewHash: "hash1", OldHash: ""},
			{SpaceId: "space2", NewHash: "hash2", OldHash: ""},
			{SpaceId: "space3", NewHash: "hash3", OldHash: ""},
		}

		for _, update := range testData {
			err := addTestHashEntry(ctx, is.(*indexStorage), update.SpaceId, update.NewHash, "")
			require.NoError(t, err)
		}

		// Run migration
		migration, err := newDiffMigration(is, testLogger)
		require.NoError(t, err)

		err = migration.Run(ctx)
		require.NoError(t, err)

		// Verify all entries have 'oh' copied from 'nh'
		err = is.ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
			for _, expected := range testData {
				if update.SpaceId == expected.SpaceId {
					assert.Equal(t, expected.NewHash, update.NewHash)
					assert.Equal(t, expected.NewHash, update.OldHash) // oh should be copied from nh
					break
				}
			}
			return true, nil
		})
		require.NoError(t, err)

		// Verify migration state is updated
		version, err := getTestMigrationVersion(ctx, is)
		require.NoError(t, err)
		assert.Equal(t, targetMigrationVersion, version)
	})

	t.Run("migration_always_copies_nh_to_oh", func(t *testing.T) {
		tempDir := t.TempDir()
		// Create index storage
		is, err := createTestIndexStorage(ctx, tempDir)
		require.NoError(t, err)
		defer is.Close()

		// Add test data with mixed entries
		testData := []SpaceUpdate{
			{SpaceId: "space1", NewHash: "hash1", OldHash: ""},            // Should be migrated
			{SpaceId: "space2", NewHash: "hash2new", OldHash: "hash2old"}, // Should be overwritten
			{SpaceId: "space3", NewHash: "hash3", OldHash: ""},            // Should be migrated
		}

		for _, update := range testData {
			err := addTestHashEntry(ctx, is.(*indexStorage), update.SpaceId, update.NewHash, update.OldHash)
			require.NoError(t, err)
		}

		// Run migration
		migration, err := newDiffMigration(is, testLogger)
		require.NoError(t, err)

		err = migration.Run(ctx)
		require.NoError(t, err)

		// Verify results
		results := make(map[string]SpaceUpdate)
		err = is.ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
			results[update.SpaceId] = update
			return true, nil
		})
		require.NoError(t, err)

		// space1: oh should be copied from nh
		assert.Equal(t, "hash1", results["space1"].NewHash)
		assert.Equal(t, "hash1", results["space1"].OldHash)

		// space2: oh should be overwritten with nh
		assert.Equal(t, "hash2new", results["space2"].NewHash)
		assert.Equal(t, "hash2new", results["space2"].OldHash)

		// space3: oh should be copied from nh
		assert.Equal(t, "hash3", results["space3"].NewHash)
		assert.Equal(t, "hash3", results["space3"].OldHash)
	})

	t.Run("migration_handles_empty_hashesIndex", func(t *testing.T) {
		tempDir := t.TempDir()
		// Create index storage
		is, err := createTestIndexStorage(ctx, tempDir)
		require.NoError(t, err)
		defer is.Close()

		// Run migration on empty storage
		migration, err := newDiffMigration(is, testLogger)
		require.NoError(t, err)

		err = migration.Run(ctx)
		require.NoError(t, err)

		// Verify migration state is updated
		version, err := getTestMigrationVersion(ctx, is)
		require.NoError(t, err)
		assert.Equal(t, targetMigrationVersion, version)
	})

	t.Run("migration_runs_only_once", func(t *testing.T) {
		tempDir := t.TempDir()
		// Create index storage
		is, err := createTestIndexStorage(ctx, tempDir)
		require.NoError(t, err)
		defer is.Close()

		// Add test data
		err = addTestHashEntry(ctx, is.(*indexStorage), "space1", "hash1", "")
		require.NoError(t, err)

		// Run migration first time
		migration, err := newDiffMigration(is, testLogger)
		require.NoError(t, err)

		err = migration.Run(ctx)
		require.NoError(t, err)

		// Modify data after migration
		err = is.UpdateHash(ctx, SpaceUpdate{
			SpaceId: "space1",
			NewHash: "hash1_new",
			OldHash: "hash1",
		})
		require.NoError(t, err)

		// Run migration second time
		err = migration.Run(ctx)
		require.NoError(t, err)

		// Verify data wasn't changed by second migration
		var result SpaceUpdate
		err = is.ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
			if update.SpaceId == "space1" {
				result = update
				return false, nil
			}
			return true, nil
		})
		require.NoError(t, err)

		assert.Equal(t, "hash1_new", result.NewHash)
		assert.Equal(t, "hash1", result.OldHash)
	})
}

// Helper functions

func createTestIndexStorage(ctx context.Context, tempDir string) (IndexStorage, error) {
	dbPath := filepath.Join(tempDir, "test_index")
	err := os.MkdirAll(dbPath, 0755)
	if err != nil {
		return nil, err
	}
	return OpenIndexStorage(ctx, dbPath)
}

func addTestHashEntry(ctx context.Context, is *indexStorage, spaceId, newHash, oldHash string) error {
	tx, err := is.spaceColl.WriteTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	arena := is.arenaPool.Get()
	defer is.arenaPool.Put(arena)

	doc := arena.NewObject()
	doc.Set("id", arena.NewString(spaceId))
	doc.Set(newHashKey, arena.NewString(newHash))
	if oldHash != "" {
		doc.Set(oldHashKey, arena.NewString(oldHash))
	}

	err = is.spaceColl.UpsertOne(tx.Context(), doc)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func setTestMigrationVersion(ctx context.Context, is IndexStorage, version int) error {
	return is.SetDiffMigrationVersion(ctx, version)
}

func getTestMigrationVersion(ctx context.Context, is IndexStorage) (int, error) {
	return is.GetDiffMigrationVersion(ctx)
}

func TestRunMigrations(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	t.Run("RunMigrations_calls_diff_migration", func(t *testing.T) {
		// Create index storage
		is, err := OpenIndexStorage(ctx, tempDir)
		require.NoError(t, err)
		defer is.Close()

		// Add test data
		err = is.UpdateHash(ctx, SpaceUpdate{
			SpaceId: "space1",
			NewHash: "hash1",
			OldHash: "",
		})
		require.NoError(t, err)

		// Run migrations through the interface
		err = is.RunMigrations(ctx)
		require.NoError(t, err)

		// Verify migration was executed
		var result SpaceUpdate
		err = is.ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
			if update.SpaceId == "space1" {
				result = update
				return false, nil
			}
			return true, nil
		})
		require.NoError(t, err)

		assert.Equal(t, "hash1", result.NewHash)
		assert.Equal(t, "hash1", result.OldHash)
	})
}
