package nodestorage

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync/app/logger"
	"go.uber.org/zap"
)

const (
	targetMigrationVersion = 3
)

type Migration interface {
	Run(ctx context.Context) error
}

type diffMigration struct {
	indexStorage IndexStorage
	logger       logger.CtxLogger
}

func newDiffMigration(is IndexStorage, log logger.CtxLogger) (*diffMigration, error) {
	return &diffMigration{
		indexStorage: is,
		logger:       log,
	}, nil
}

func (m *diffMigration) Run(ctx context.Context) error {
	m.logger.Info("starting diff migration v2 to v3")

	currentVersion, err := m.indexStorage.GetDiffMigrationVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get migration version: %w", err)
	}

	if currentVersion >= targetMigrationVersion {
		m.logger.Info("diff migration already completed", zap.Int("version", currentVersion))
		return nil
	}

	err = m.migrateHashIndex(ctx)
	if err != nil {
		return fmt.Errorf("failed to migrate hash index: %w", err)
	}

	err = m.indexStorage.SetDiffMigrationVersion(ctx, targetMigrationVersion)
	if err != nil {
		return fmt.Errorf("failed to set migration version: %w", err)
	}

	m.logger.Info("diff migration completed successfully")
	return nil
}

func (m *diffMigration) migrateHashIndex(ctx context.Context) error {
	migratedCount := 0

	err := m.indexStorage.UpdateHashes(ctx, func(spaceId, newHash, oldHash string) (newNewHash, newOldHash string, shouldUpdate bool) {
		migratedCount++
		return newHash, newHash, true
	})

	if err != nil {
		return fmt.Errorf("failed to update hashes: %w", err)
	}

	m.logger.Info("hash index migration completed", zap.Int("migrated_count", migratedCount))
	return nil
}
