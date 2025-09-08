package archive

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/util/periodicsync"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/archive/archivestore"
	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
)

const CName = "node.archive"

var log = logger.NewNamed(CName)

func New() Archive {
	return new(archive)
}

type Archive interface {
	app.ComponentRunnable
	Restore(ctx context.Context, spaceId string) (err error)
}

type archive struct {
	storageProvider nodestorage.NodeStorage
	archiveStore    archivestore.ArchiveStore
	config          Config
	checker         periodicsync.PeriodicSync
	accessDurCutoff time.Duration
	syncWaiter      <-chan struct{}
}

func (a *archive) Init(ap *app.App) (err error) {
	a.storageProvider = ap.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	a.archiveStore = ap.MustComponent(archivestore.CName).(archivestore.ArchiveStore)
	a.config = ap.MustComponent("config").(configSource).GetArchive()
	if a.config.ArchiveAfterDays <= 0 {
		a.config.ArchiveAfterDays = 7
	}
	a.accessDurCutoff = time.Duration(a.config.ArchiveAfterDays) * time.Hour * 24
	a.syncWaiter = ap.MustComponent(nodesync.CName).(nodesync.NodeSync).WaitSyncOnStart()
	return
}

func (a *archive) Name() (name string) {
	return CName
}

func (a *archive) Run(_ context.Context) (err error) {
	if !a.config.Enabled {
		return
	}
	if a.config.CheckPeriodMinutes <= 0 {
		a.config.CheckPeriodMinutes = 2
	}
	period := time.Minute * time.Duration(a.config.CheckPeriodMinutes)
	a.checker = periodicsync.NewPeriodicSync(int(period.Seconds()), time.Hour, a.Check, log)
	return
}

func (a *archive) Archive(ctx context.Context, spaceId string) (err error) {
	var gzSize, dbSize int64

	err = a.storageProvider.DumpStorage(ctx, spaceId, func(p string) (err error) {
		gzPath, gzSz, dbSz, err := a.createGzipFromStore(p)
		if err != nil {
			return err
		}
		gzSize, dbSize = gzSz, dbSz

		r, err := os.Open(gzPath)
		if err != nil {
			return err
		}
		defer func() {
			if cerr := r.Close(); err == nil && cerr != nil {
				err = cerr
			}
		}()

		if err = a.archiveStore.Put(ctx, spaceId, r); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err = a.storageProvider.IndexStorage().MarkArchived(ctx, spaceId, gzSize, dbSize); err != nil {
		return err
	}
	_ = a.storageProvider.ForceRemove(spaceId)
	return os.RemoveAll(a.storageProvider.StoreDir(spaceId))
}

// createGzipFromStore creates store.gz from store.db inside spaceDir.
// Returns path to .gz, its size and original db size.
func (a *archive) createGzipFromStore(spaceDir string) (gzPath string, gzSize, dbSize int64, err error) {
	storePath := filepath.Join(spaceDir, "store.db")
	gzPath = filepath.Join(spaceDir, "store.gz")

	storeFile, err := os.Open(storePath)
	if err != nil {
		return "", 0, 0, err
	}
	defer func() {
		if cerr := storeFile.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	gzFile, err := os.Create(gzPath)
	if err != nil {
		return "", 0, 0, err
	}
	defer func() {
		if cerr := gzFile.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	gw := gzip.NewWriter(gzFile)
	dbSize, err = io.Copy(gw, storeFile)
	if err != nil {
		_ = gw.Close()
		return "", 0, 0, err
	}

	if err = gw.Close(); err != nil {
		return "", 0, 0, err
	}

	info, err := gzFile.Stat()
	if err != nil {
		return "", 0, 0, err
	}
	gzSize = info.Size()

	return gzPath, gzSize, dbSize, nil
}

func (a *archive) Restore(ctx context.Context, spaceId string) (err error) {
	if err = a.restoreFile(ctx, spaceId); err != nil {
		_ = os.RemoveAll(a.storageProvider.StoreDir(spaceId))
		return err
	}
	if err = a.storageProvider.IndexStorage().SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusOk, ""); err != nil {
		return
	}
	return a.archiveStore.Delete(ctx, spaceId)
}

func (a *archive) restoreFile(ctx context.Context, spaceId string) (err error) {
	reader, err := a.archiveStore.Get(ctx, spaceId)
	if err != nil {
		return
	}
	defer func() {
		_ = reader.Close()
	}()

	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return
	}

	defer func() {
		_ = gzipReader.Close()
	}()

	storeDir := a.storageProvider.StoreDir(spaceId)
	storePath := filepath.Join(storeDir, "store.db")
	if err = os.MkdirAll(storeDir, 0755); err != nil {
		return
	}

	var cleanup = func() {
		_ = os.RemoveAll(storePath)
	}

	storeFile, err := os.Create(storePath)
	if err != nil {
		cleanup()
		return
	}
	defer func() {
		_ = storeFile.Close()
	}()

	if _, err = io.Copy(storeFile, gzipReader); err != nil {
		cleanup()
		return
	}
	return
}

func (a *archive) Check(ctx context.Context) error {
	indexStore := a.storageProvider.IndexStorage()
	deadline, _ := ctx.Deadline()
	for {
		spaceId, err := indexStore.FindOldestInactiveSpace(ctx, a.accessDurCutoff)
		if err != nil {
			if errors.Is(err, anystore.ErrDocNotFound) {
				return nil
			}
			return err
		}
		st := time.Now()
		if err = a.Archive(ctx, spaceId); err != nil {
			log.Error("space archive failed", zap.String("spaceId", spaceId), zap.Error(err))
			return err
		}
		log.Info("space is archived", zap.String("spaceId", spaceId), zap.Duration("dur", time.Since(st)))
		if deadline.Sub(time.Now()) < time.Minute*10 {
			return nil
		}
	}
}

func (a *archive) Close(_ context.Context) (err error) {
	if a.checker != nil {
		a.checker.Close()
	}
	return
}
