package migrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/spacestorage/migration"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/oldstorage"
)

const CName = "node.nodespace.migrator"

var log = logger.NewNamed(CName)

const (
	spaceMigrationColl = "migrationSpace"
	migratedStateColl  = "migrationState"
	migratedTimeKey    = "time"
	migratedDoc        = "state"
	statusKey          = "status"
)

type noOpProgress struct{}

func (n noOpProgress) AddDone(done int64) {}

type migrator struct {
	oldStorage oldstorage.NodeStorage
	newStorage nodestorage.NodeStorage
	path       string
	oldPath    string
}

type configGetter interface {
	GetStorage() nodestorage.Config
}

func New() app.ComponentRunnable {
	return &migrator{}
}

func (m *migrator) Init(a *app.App) (err error) {
	cfg := a.MustComponent("config").(configGetter)
	m.path = cfg.GetStorage().AnyStorePath
	m.oldPath = cfg.GetStorage().AnyStorePath
	m.oldStorage = app.MustComponent[oldstorage.NodeStorage](a)
	m.newStorage = app.MustComponent[nodestorage.NodeStorage](a)
	return nil
}

func (m *migrator) Name() (name string) {
	return CName
}

func (m *migrator) Run(ctx context.Context) (err error) {
	dirPath := path.Join(m.oldPath, nodestorage.IndexStorageName)
	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}
	dbPath := path.Join(dirPath, "store.db")
	migrateDb, err := anystore.Open(ctx, dbPath, nil)
	if err != nil {
		return err
	}
	defer migrateDb.Close()
	if m.checkMigrated(ctx, migrateDb) {
		return nil
	}
	migrator := migration.NewSpaceMigrator(m.oldStorage, m.newStorage, 40, m.path)
	allIds, err := m.oldStorage.AllSpaceIds()
	if err != nil {
		return err
	}
	for idx, id := range allIds {
		tm := time.Now()
		err := migrator.MigrateId(ctx, id, noOpProgress{})
		if err != nil {
			log.Error("failed to migrate space", zap.String("spaceId", id), zap.Error(err))
			if errors.Is(err, migration.ErrAlreadyMigrated) {
				continue
			}
			err := m.setSpaceMigrated(ctx, id, migrateDb, err)
			if err != nil {
				return err
			}
			continue
		}
		err = m.setSpaceMigrated(ctx, id, migrateDb, nil)
		if err != nil {
			return err
		}
		log.Info("migrated space", zap.String("spaceId", id), zap.String("total", fmt.Sprintf("%d/%d", idx, len(allIds))), zap.String("time", time.Since(tm).String()))
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return m.setAllMigrated(ctx, migrateDb)
}

func (m *migrator) Close(ctx context.Context) (err error) {
	return nil
}

func (m *migrator) checkMigrated(ctx context.Context, anyStore anystore.DB) bool {
	coll, err := anyStore.OpenCollection(ctx, migratedStateColl)
	if err != nil {
		return false
	}
	_, err = coll.FindId(ctx, migratedDoc)
	if err != nil {
		return false
	}
	return true
}

func (m *migrator) setSpaceMigrated(ctx context.Context, id string, anyStore anystore.DB, migrationErr error) error {
	coll, err := anyStore.Collection(ctx, spaceMigrationColl)
	if err != nil {
		return fmt.Errorf("migration: failed to get collection: %w", err)
	}
	arena := &anyenc.Arena{}
	tx, err := coll.WriteTx(ctx)
	if err != nil {
		return err
	}
	newVal := arena.NewObject()
	newVal.Set("id", arena.NewString(id))
	if migrationErr != nil {
		newVal.Set(statusKey, arena.NewString(migrationErr.Error()))
	} else {
		newVal.Set(statusKey, arena.NewString("ok"))
	}
	err = coll.Insert(tx.Context(), newVal)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (m *migrator) setAllMigrated(ctx context.Context, anyStore anystore.DB) error {
	coll, err := anyStore.Collection(ctx, migratedStateColl)
	if err != nil {
		return fmt.Errorf("migration: failed to get collection: %w", err)
	}
	arena := &anyenc.Arena{}
	tx, err := coll.WriteTx(ctx)
	if err != nil {
		return err
	}
	newVal := arena.NewObject()
	newVal.Set(migratedTimeKey, arena.NewNumberFloat64(float64(time.Now().Unix())))
	newVal.Set("id", arena.NewString(migratedDoc))
	err = coll.Insert(tx.Context(), newVal)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		return nil
	}
	return anyStore.Checkpoint(ctx, true)
}
