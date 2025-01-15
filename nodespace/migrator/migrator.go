package migrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage/migration"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/oldstorage"
)

const CName = "node.nodespace.migrator"

var log = logger.NewNamed(CName)

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
	migrator := migration.NewSpaceMigrator(m.oldStorage, m.newStorage, 40, m.path)
	allIds, err := m.oldStorage.AllSpaceIds()
	if err != nil {
		return err
	}
	for idx, id := range allIds {
		if strings.Contains(id, "deletion") {
			continue
		}
		tm := time.Now()
		err := migrator.MigrateId(ctx, id, noOpProgress{})
		fmt.Println("[y]: migrated space", id, fmt.Sprintf("%d/%d", idx, len(allIds)), time.Since(tm))
		if err != nil {
			if errors.Is(err, migration.ErrAlreadyMigrated) || errors.Is(err, spacestorage.ErrSpaceStorageMissing) {
				continue
			}
			log.Error("failed to migrate space", zap.String("spaceId", id), zap.Error(err))
			return err
		}
		log.Info("migrated space", zap.String("spaceId", id), zap.String("total", fmt.Sprintf("%d/%d", idx, len(allIds))))
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (m *migrator) Close(ctx context.Context) (err error) {
	return nil
}
