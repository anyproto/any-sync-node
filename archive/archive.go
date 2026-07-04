//go:generate mockgen -destination mock_archive/mock_archive.go github.com/anyproto/any-sync-node/archive Archive

package archive

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/util/periodicsync"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-node/archive/archivestore"
	"github.com/anyproto/any-sync-node/nodestorage"
)

const CName = "node.archive"

// nodeSyncCName is nodesync.CName; the nodesync package is referenced by name
// to avoid an import cycle (nodesync -> nodehead -> ... -> archive).
const nodeSyncCName = "node.nodesync"

// syncWaiter is implemented by nodesync.NodeSync.
type syncWaiter interface {
	WaitSyncOnStart() <-chan struct{}
}

var log = logger.NewNamed(CName)

func New() Archive {
	return new(archive)
}

type Archive interface {
	app.ComponentRunnable
	Restore(ctx context.Context, spaceId string) (err error)
	// ForceArchive uploads a snapshot of the space to the archive store while
	// keeping the local DB and its status intact. Used to ship live spaces
	// through the shared archive store during migration. The caller must ensure
	// the space is not already archived: opening an archived space triggers a
	// restore. Returns the heads read from the snapshot itself (they exactly
	// describe the uploaded object, unlike the asynchronously updated index)
	// and the snapshot sizes.
	// The return types are primitives on purpose: a struct would make the
	// generated mock import this package and create test-only import cycles.
	ForceArchive(ctx context.Context, spaceId string) (oldHash, newHash string, compressedSize, uncompressedSize int64, err error)
	// QueueRestore schedules a background restore of an archived space
	// (used for eager adoption of migrated spaces).
	QueueRestore(spaceId string)
}

type archive struct {
	storageProvider nodestorage.NodeStorage
	archiveStore    archivestore.ArchiveStore
	config          Config
	checker         periodicsync.PeriodicSync
	sweeper         periodicsync.PeriodicSync
	accessDurCutoff time.Duration
	stat            *archiveStat
	syncWaiter      <-chan struct{}
	runCtx          context.Context
	runCtxCancel    context.CancelFunc

	restoreQueue  chan string
	restoreQueued sync.Map
	// archiveMu serializes the periodic archiver with the prefix sweeper so a
	// sweep decision can't race a concurrent re-archive of the same object
	archiveMu sync.Mutex
}

func (a *archive) Init(ap *app.App) (err error) {
	a.storageProvider = ap.MustComponent(nodestorage.CName).(nodestorage.NodeStorage)
	a.archiveStore = ap.MustComponent(archivestore.CName).(archivestore.ArchiveStore)
	a.config = ap.MustComponent("config").(configSource).GetArchive()
	if a.config.ArchiveAfterDays <= 0 {
		a.config.ArchiveAfterDays = 7
	}
	a.accessDurCutoff = time.Duration(a.config.ArchiveAfterDays) * time.Hour * 24
	a.syncWaiter = ap.MustComponent(nodeSyncCName).(syncWaiter).WaitSyncOnStart()
	a.runCtx, a.runCtxCancel = context.WithCancel(context.Background())
	if a.config.CheckPeriodMinutes <= 0 {
		a.config.CheckPeriodMinutes = 2
	}
	period := time.Minute * time.Duration(a.config.CheckPeriodMinutes)
	a.checker = periodicsync.NewPeriodicSyncDuration(period, time.Hour, a.check, log)
	if a.config.SweepPeriodHours <= 0 {
		a.config.SweepPeriodHours = 24
	}
	a.sweeper = periodicsync.NewPeriodicSyncDuration(
		time.Duration(a.config.SweepPeriodHours)*time.Hour, time.Hour, a.sweep, log)
	a.stat = new(archiveStat)
	a.restoreQueue = make(chan string, 1000)
	if m := ap.Component(metric.CName); m != nil {
		registerMetric(a.stat, m.(metric.Metric).Registry())
	}
	return
}

func (a *archive) Name() (name string) {
	return CName
}

func (a *archive) Run(_ context.Context) (err error) {
	go a.restoreWorker()
	if !a.config.Enabled {
		return
	}
	go func() {
		select {
		case <-a.runCtx.Done():
			return
		case <-a.syncWaiter:
		}
		a.checker.Run()
		a.sweeper.Run()
	}()
	return
}

func (a *archive) QueueRestore(spaceId string) {
	if _, loaded := a.restoreQueued.LoadOrStore(spaceId, struct{}{}); loaded {
		return
	}
	select {
	case a.restoreQueue <- spaceId:
	default:
		// queue is full: drop, the space will be restored lazily on first access
		a.restoreQueued.Delete(spaceId)
	}
}

func (a *archive) restoreWorker() {
	for {
		select {
		case <-a.runCtx.Done():
			return
		case spaceId := <-a.restoreQueue:
			ctx, cancel := context.WithTimeout(a.runCtx, time.Minute*10)
			// opening the space storage restores it if archived; reuses the
			// storage cache's single-flight so concurrent lazy restores are safe
			err := a.storageProvider.TryLockAndOpenDb(ctx, spaceId, func(db anystore.DB) error {
				return nil
			})
			cancel()
			a.restoreQueued.Delete(spaceId)
			if err != nil && !errors.Is(err, nodestorage.ErrLocked) && !errors.Is(err, context.Canceled) {
				log.Warn("eager restore failed, will restore lazily on access", zap.String("spaceId", spaceId), zap.Error(err))
			}
		}
	}
}

var errArchived = errors.New("archived")

func (a *archive) Archive(ctx context.Context, spaceId string) (err error) {
	a.archiveMu.Lock()
	defer a.archiveMu.Unlock()
	var gzSize, dbSize int64
	tmpDir, err := os.MkdirTemp("", spaceId)
	if err != nil {
		return
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()
	err = a.storageProvider.TryLockAndOpenDb(ctx, spaceId, func(db anystore.DB) error {
		storePath := filepath.Join(tmpDir, "store.db")
		if err = db.Backup(ctx, storePath); err != nil {
			return err
		}
		gzPath, gzSz, dbSz, err := a.createGzipFromStore(tmpDir)
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

		if err = a.storageProvider.IndexStorage().MarkArchived(ctx, spaceId, gzSize, dbSize); err != nil {
			return err
		}

		_ = db.Close()
		_ = os.RemoveAll(a.storageProvider.StoreDir(spaceId))
		a.stat.archived.Add(1)
		return errArchived
	})

	if errors.Is(err, errArchived) {
		return nil
	}
	return
}

func (a *archive) ForceArchive(ctx context.Context, spaceId string) (oldHash, newHash string, compressedSize, uncompressedSize int64, err error) {
	// DumpStorage backups the db into a temp dir; it works for open spaces too
	err = a.storageProvider.DumpStorage(ctx, spaceId, func(path string) error {
		// read the heads from the snapshot: they describe exactly what the
		// uploaded object contains
		oldHash, newHash, err = readSnapshotHeads(ctx, spaceId, filepath.Join(path, "store.db"))
		if err != nil {
			return err
		}
		gzPath, gzSize, dbSize, err := a.createGzipFromStore(path)
		if err != nil {
			return err
		}
		compressedSize, uncompressedSize = gzSize, dbSize

		r, err := os.Open(gzPath)
		if err != nil {
			return err
		}
		defer func() {
			_ = r.Close()
		}()
		return a.archiveStore.Put(ctx, spaceId, r)
	})
	if err == nil {
		a.stat.forceArchived.Add(1)
	}
	return
}

// readSnapshotHeads reads the space state directly from the snapshot db
// (schema of commonspace/headsync/statestorage, including the legacy
// single-hash fallback).
func readSnapshotHeads(ctx context.Context, spaceId, dbPath string) (oldHash, newHash string, err error) {
	db, err := anystore.Open(ctx, dbPath, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = db.Close()
	}()
	coll, err := db.OpenCollection(ctx, "state")
	if err != nil {
		return
	}
	doc, err := coll.FindId(ctx, spaceId)
	if err != nil {
		return
	}
	oldHash = doc.Value().GetString("oh")
	newHash = doc.Value().GetString("nh")
	if oldHash == "" || newHash == "" {
		oldHash = doc.Value().GetString("h")
		newHash = oldHash
	}
	return oldHash, newHash, nil
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
		if !errors.Is(err, archivestore.ErrNotFound) {
			// clean up a partially extracted db; a missing archive object wrote
			// nothing, and the dir may hold a pre-existing db that must survive
			_ = os.RemoveAll(a.storageProvider.StoreDir(spaceId))
		}
		return err
	}
	if err = a.storageProvider.IndexStorage().SetSpaceStatus(ctx, spaceId, nodestorage.SpaceStatusOk, ""); err != nil {
		return
	}
	a.stat.restored.Add(1)
	// the archive object is intentionally kept (restore is a copy, not a move):
	// it is overwritten by the next archive cycle and makes restore idempotent;
	// stale objects of live spaces are garbage-collected by the archive sweeper
	return nil
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
		if cErr := storeFile.Close(); cErr != nil {
			err = errors.Join(err, cErr)
		}
		if err != nil {
			cleanup()
		}
	}()

	if _, err = io.Copy(storeFile, gzipReader); err != nil {
		return
	}
	return
}

func (a *archive) check(ctx context.Context) error {
	indexStore := a.storageProvider.IndexStorage()
	deadline, _ := ctx.Deadline()
	var skip int
	for {
		log.Info("check spaces", zap.Time("lastAccessTime", time.Now().Add(-a.accessDurCutoff)))
		spaceId, err := indexStore.FindOldestInactiveSpace(ctx, a.accessDurCutoff, skip)
		if err != nil {
			if errors.Is(err, anystore.ErrDocNotFound) {
				return nil
			}
			return err
		}
		st := time.Now()
		if err = a.Archive(ctx, spaceId); err != nil {
			log.Error("space archive failed", zap.String("spaceId", spaceId), zap.Error(err))
			if errors.Is(err, nodestorage.ErrLocked) {
				skip++
				continue
			}
			a.stat.archiveError.Add(1)
			return indexStore.MarkError(ctx, spaceId, err.Error())
		}
		log.Info("space is archived", zap.String("spaceId", spaceId), zap.Duration("dur", time.Since(st)))
		if !deadline.IsZero() && deadline.Sub(time.Now()) < time.Minute*10 {
			return nil
		}
	}
}

func (a *archive) Close(_ context.Context) (err error) {
	if a.checker != nil {
		a.checker.Close()
	}
	if a.sweeper != nil {
		a.sweeper.Close()
	}
	if a.runCtxCancel != nil {
		a.runCtxCancel()
	}
	return
}
