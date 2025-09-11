//go:generate mockgen -destination mock_nodestorage/mock_nodestorage.go github.com/anyproto/any-sync-node/nodestorage NodeStorage
package nodestorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/debugstat"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/util/slice"
	"go.uber.org/zap"
)

const CName = spacestorage.CName

var (
	ErrClosed                  = errors.New("space storage closed")
	ErrLocked                  = errors.New("space storage locked")
	ErrArchived                = errors.New("space is archived")
	ErrSpaceIdIsEmpty          = errors.New("space id is empty")
	ErrDoesntSupportSpaceStats = errors.New("doesn't support nodestorage.ObjectSpaceStats")
	ErrDeleted                 = errors.New("space storage deleted")
)

func New() NodeStorage {
	return &storageService{}
}

type optKey int

const (
	createKeyVal optKey = 0
	doKeyVal     optKey = 1
	doAfterOpen  optKey = 2
)

func anyStoreConfig() *anystore.Config {
	return &anystore.Config{
		ReadConnections: 4,
		SQLiteConnectionOptions: map[string]string{
			"synchronous": "off",
		},
	}
}

type (
	doFunc          = func() error
	doAfterOpenFunc = func(db anystore.DB) error
)

type NodeStorage interface {
	spacestorage.SpaceStorageProvider
	IndexStorage() IndexStorage
	IndexSpace(ctx context.Context, spaceId string, setHead bool) (spacestorage.SpaceStorage, error)
	SpaceStorage(ctx context.Context, spaceId string) (spacestorage.SpaceStorage, error)
	TryLockAndDo(ctx context.Context, spaceId string, do doFunc) (err error)
	TryLockAndOpenDb(ctx context.Context, spaceId string, do doAfterOpenFunc) (err error)
	DumpStorage(ctx context.Context, id string, do func(path string) error) (err error)
	AllSpaceIds() (ids []string, err error)
	OnDeleteStorage(onDelete func(ctx context.Context, spaceId string))
	OnWriteHash(onWrite func(ctx context.Context, spaceId, oldHash, newHash string))
	StoreDir(spaceId string) (path string)
	DeleteSpaceStorage(ctx context.Context, spaceId string) error
	ForceRemove(id string) (err error)
	GetStats(ctx context.Context, id string, treeTop int) (spaceStats SpaceStats, err error)
}

type StorageStats struct {
	Total  int                `json:"total"`
	Spaces []SpaceStorageStat `json:"spaces"`
}

type SpaceStorageStat struct {
	Id            string `json:"id"`
	Refs          int    `json:"refs"`
	Info          string `json:"info"`
	InStorageSecs int    `json:"inStorageSecs"`
}

type archiveService interface {
	Restore(ctx context.Context, spaceId string) error
}

const archiveCName = "node.archive"

type storageService struct {
	rootPath        string
	cache           ocache.OCache
	indexStorage    IndexStorage
	updater         *spaceUpdater
	onWriteHash     func(ctx context.Context, spaceId, oldHash, newHash string)
	onDeleteStorage func(ctx context.Context, spaceId string)
	currentSpaces   map[string]*storageContainer
	mu              sync.Mutex
	statService     debugstat.StatService
	archive         archiveService
}

func (s *storageService) Init(a *app.App) (err error) {
	cfg := a.MustComponent("config").(configGetter).GetStorage()
	s.archive = a.MustComponent(archiveCName).(archiveService)
	s.updater = newSpaceUpdater(func(updates []SpaceUpdate) {
		if s.indexStorage == nil {
			return
		}
		if err := s.indexStorage.UpdateHash(context.Background(), updates...); err != nil {
			log.Error("failed to update hashes", zap.Error(err))
		}
		if s.onWriteHash != nil {
			for _, update := range updates {
				s.onWriteHash(context.Background(), update.SpaceId, update.OldHash, update.NewHash)
			}
		}
	})
	s.rootPath = cfg.AnyStorePath
	if _, err = os.Stat(s.rootPath); err != nil {
		err = os.MkdirAll(s.rootPath, 0755)
		if err != nil {
			return err
		}
	}
	comp, ok := a.Component(debugstat.CName).(debugstat.StatService)
	if !ok {
		comp = debugstat.NewNoOp()
	}
	s.statService = comp
	s.statService.AddProvider(s)
	s.cache = ocache.New(s.loadFunc,
		ocache.WithLogger(log.Sugar()),
		ocache.WithGCPeriod(time.Minute),
		ocache.WithTTL(60*time.Second))
	if m := a.Component(metric.CName); m != nil {
		registerMetric(&StorageStat{cache: s.cache}, m.(metric.Metric).Registry())
	}
	return nil
}

func (s *storageService) Name() (name string) {
	return CName
}

func (s *storageService) Run(ctx context.Context) (err error) {
	s.updater.Run()
	s.indexStorage, err = OpenIndexStorage(ctx, s.rootPath)
	if err != nil {
		log.Error("failed to open index storage", zap.Error(err))
		return err
	}

	// Run migrations
	if err := s.indexStorage.RunMigrations(ctx); err != nil {
		log.Error("failed to run migrations", zap.Error(err))
		return err
	}
	allIds, err := s.AllSpaceIds()
	if err != nil {
		log.Error("failed to get all space ids", zap.Error(err))
		return err
	}
	var (
		toUpdate   []string
		currentIds = make([]string, 0, len(allIds))
	)
	err = s.indexStorage.ReadHashes(ctx, func(update SpaceUpdate) (bool, error) {
		currentIds = append(currentIds, update.SpaceId)
		return true, nil
	})
	_, toUpdate = slice.DifferenceRemovedAdded(currentIds, allIds)
	if err != nil {
		log.Error("failed to read hashes", zap.Error(err))
		return err
	}
	for _, id := range toUpdate {
		_, err := s.IndexSpace(ctx, id, false)
		if err != nil {
			log.Error("failed to index space", zap.String("spaceId", id), zap.Error(err))
			continue
		}
		err = s.ForceRemove(id)
		if err != nil {
			log.Error("failed to remove space", zap.String("spaceId", id), zap.Error(err))
		}
	}
	return
}

func (s *storageService) ProvideStat() any {
	stat := &StorageStats{}
	s.cache.ForEach(func(v ocache.Object) (isContinue bool) {
		cont := v.(*storageContainer)
		cont.mx.Lock()
		defer cont.mx.Unlock()
		stat.Total++
		stat.Spaces = append(stat.Spaces, SpaceStorageStat{
			Id:            cont.id,
			Refs:          cont.handlers,
			Info:          cont.debugInfo,
			InStorageSecs: int(time.Since(cont.created).Seconds()),
		})
		return true
	})
	return stat
}

func (s *storageService) StatId() string {
	return CName
}

func (s *storageService) StatType() string {
	return CName
}

func (s *storageService) onHashChange(spaceId, oldHash, newHash string) {
	_ = s.updater.Add(SpaceUpdate{
		SpaceId: spaceId,
		OldHash: oldHash,
		NewHash: newHash,
		Updated: time.Now(),
	})
}

func (s *storageService) IndexStorage() IndexStorage {
	return s.indexStorage
}

func (s *storageService) openDb(ctx context.Context, id string) (db anystore.DB, err error) {
	dbPath := filepath.Join(s.StoreDir(id), "store.db")
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, spacestorage.ErrSpaceStorageMissing
		}
		return nil, err
	}
	return anystore.Open(ctx, dbPath, anyStoreConfig())
}

func (s *storageService) createDb(ctx context.Context, id string) (db anystore.DB, err error) {
	dirPath := s.StoreDir(id)
	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		return nil, err
	}
	dbPath := path.Join(dirPath, "store.db")
	return anystore.Open(ctx, dbPath, anyStoreConfig())
}

const (
	debugInfoIsCreate  = "create"
	debugInfoIsOpen    = "open"
	debugInfoIsAfterDo = "afterDo"
)

func (s *storageService) loadFunc(ctx context.Context, id string) (value ocache.Object, err error) {
	var (
		info string
		cont *storageContainer
	)
	defer func() {
		if cont != nil {
			cont.debugInfo = info
		}
	}()

	statusErr := s.checkStatus(ctx, id)
	if statusErr != nil {
		if errors.Is(statusErr, ErrArchived) {
			if err = s.archive.Restore(ctx, id); err != nil {
				return nil, err
			}
		} else {
			return nil, statusErr
		}
	}

	if fn, ok := ctx.Value(doKeyVal).(doFunc); ok {
		err := fn()
		if err != nil {
			return nil, err
		}
		info = debugInfoIsAfterDo
		// continue to open
	} else if ctx.Value(createKeyVal) != nil {
		if s.SpaceExists(id) {
			return nil, spacestorage.ErrSpaceStorageExists
		}
		db, err := s.createDb(ctx, id)
		if err != nil {
			return nil, err
		}
		info = debugInfoIsCreate
		cont = newStorageContainer(db, id)
		return cont, nil
	} else {
		info = debugInfoIsOpen
	}
	// we assume that the database is not empty
	db, err := s.openDb(ctx, id)
	if err != nil {
		return nil, err
	}
	collNames, err := db.GetCollectionNames(ctx)
	if len(collNames) == 0 {
		_ = os.RemoveAll(s.StoreDir(id))
		return nil, spacestorage.ErrSpaceStorageMissing
	}
	cont = newStorageContainer(db, id)

	if fn, ok := ctx.Value(doAfterOpen).(doAfterOpenFunc); ok {
		if err = fn(db); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return cont, nil
}

func (s *storageService) get(ctx context.Context, id string) (container *storageContainer, err error) {
	cont, err := s.cache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return cont.(*storageContainer), nil
}

func (s *storageService) checkStatus(ctx context.Context, spaceId string) (err error) {
	status, err := s.indexStorage.SpaceStatus(ctx, spaceId)
	if err != nil {
		return
	}
	switch status {
	case SpaceStatusOk:
		return nil
	case SpaceStatusRemove, SpaceStatusRemovePrepare:
		return spacestorage.ErrSpaceStorageMissing
	case SpaceStatusArchived:
		return ErrArchived
	default:
		return fmt.Errorf("unknown status: %v", status)
	}
}

func (s *storageService) SpaceStorage(ctx context.Context, spaceId string) (spacestorage.SpaceStorage, error) {
	return s.WaitSpaceStorage(ctx, spaceId)
}

func (s *storageService) WaitSpaceStorage(ctx context.Context, id string) (spacestorage.SpaceStorage, error) {
	cont, err := s.get(ctx, id)
	if err != nil {
		return nil, err
	}
	db, err := cont.Acquire()
	if err != nil {
		return nil, err
	}
	st, err := spacestorage.New(ctx, id, db)
	if err != nil {
		log.Error("can't wait for space storage", zap.Error(err))
		cont.Release()
		return nil, err
	}
	return newNodeStorage(st, cont, s.onHashChange), nil
}

func (s *storageService) SpaceExists(id string) bool {
	if id == "" {
		return false
	}
	dbPath := filepath.Join(s.StoreDir(id), "store.db")
	if _, err := os.Stat(dbPath); err != nil {
		return false
	}
	return true
}

func (s *storageService) IndexSpace(ctx context.Context, spaceId string, setHead bool) (ss spacestorage.SpaceStorage, err error) {
	ss, err = s.SpaceStorage(ctx, spaceId)
	if err != nil {
		return
	}
	state, err := ss.StateStorage().GetState(ctx)
	if err != nil {
		return
	}
	err = s.indexStorage.UpdateHash(ctx, SpaceUpdate{
		SpaceId: spaceId,
		OldHash: state.OldHash,
		NewHash: state.NewHash,
	})
	if err != nil {
		log.Error("can't update hash", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	if setHead && s.onWriteHash != nil {
		s.onWriteHash(ctx, spaceId, state.OldHash, state.NewHash)
	}
	return
}

func (s *storageService) CreateSpaceStorage(ctx context.Context, payload spacestorage.SpaceStorageCreatePayload) (spacestorage.SpaceStorage, error) {
	ctx = context.WithValue(ctx, createKeyVal, true)
	cont, err := s.get(ctx, payload.SpaceHeaderWithId.Id)
	if err != nil {
		return nil, err
	}
	db, err := cont.Acquire()
	if err != nil {
		return nil, err
	}
	st, err := spacestorage.Create(ctx, db, payload)
	if err != nil {
		log.Error("can't create space storage", zap.Error(err))
		cont.Release()
		return nil, err
	}
	return newNodeStorage(st, cont, s.onHashChange), nil
}

func (s *storageService) GetStats(ctx context.Context, id string, treeTop int) (spaceStats SpaceStats, err error) {
	storage, err := s.WaitSpaceStorage(ctx, id)
	if err != nil {
		err = fmt.Errorf("can't get space storage: %w", err)
		return
	}
	defer storage.Close(ctx)
	st, ok := storage.(NodeStorageStats)
	if !ok {
		err = ErrDoesntSupportSpaceStats
		return
	}
	res, err := st.GetSpaceStats(ctx, treeTop)
	if err != nil {
		err = fmt.Errorf("can't get space stats: %w", err)
		return
	}
	spaceStats.Storage = res
	aclStorage, err := storage.AclStorage()
	if err != nil {
		err = fmt.Errorf("can't get aclStorage storage: %w", err)
		return
	}
	identity, err := accountdata.NewRandom()
	if err != nil {
		err = fmt.Errorf("can't get random identity: %w", err)
		return
	}
	aclList, err := list.BuildAclListWithIdentity(identity, aclStorage, recordverifier.NewValidateFull())
	if err != nil {
		err = fmt.Errorf("can't build aclList: %w", err)
		return
	}
	for _, acc := range aclList.AclState().CurrentAccounts() {
		if !acc.Permissions.NoPermissions() {
			if acc.Permissions.CanWrite() {
				spaceStats.Acl.Writers++
			} else {
				spaceStats.Acl.Readers++
			}
		}
	}
	return
}

func (s *storageService) ForceRemove(id string) (err error) {
	ctx := context.Background()
	ss, err := s.cache.Pick(ctx, id)
	if err != nil {
		return nil
	}
	anyStore := ss.(*storageContainer).db
	_, err = s.cache.Remove(ctx, id)
	anyStore.Close()
	return
}

func (s *storageService) TryLockAndDo(ctx context.Context, spaceId string, do doFunc) (err error) {
	var called bool
	ctx = context.WithValue(ctx, doKeyVal, func() error {
		called = true
		return do()
	})
	if _, err = s.get(ctx, spaceId); err != nil {
		return
	}
	if !called {
		return ErrLocked
	}
	return nil
}

func (s *storageService) TryLockAndOpenDb(ctx context.Context, spaceId string, do doAfterOpenFunc) (err error) {
	var called bool
	ctx = context.WithValue(ctx, doAfterOpen, func(db anystore.DB) error {
		called = true
		return do(db)
	})
	if _, err = s.get(ctx, spaceId); err != nil {
		return
	}
	if !called {
		return ErrLocked
	}
	return nil
}

func (s *storageService) DumpStorage(ctx context.Context, id string, do func(path string) error) (err error) {
	cont, err := s.get(ctx, id)
	if err != nil {
		return err
	}
	db, err := cont.Acquire()
	if err != nil {
		return err
	}
	defer cont.Release()
	tempDir, err := os.MkdirTemp("", id)
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	err = db.Backup(ctx, filepath.Join(tempDir, "store.db"))
	if err != nil {
		return
	}
	return do(tempDir)
}

func (s *storageService) DeleteSpaceStorage(ctx context.Context, spaceId string) error {
	db, err := s.get(ctx, spaceId)
	if err == nil {
		db.Close()
	}
	spacePath := s.StoreDir(spaceId)
	if s.onDeleteStorage != nil {
		s.onDeleteStorage(ctx, spaceId)
	}
	return os.RemoveAll(spacePath)
}

func (s *storageService) AllSpaceIds() (ids []string, err error) {
	var files []string
	fileInfo, err := os.ReadDir(s.rootPath)
	if err != nil {
		return files, fmt.Errorf("can't read datadir '%v': %v", s.rootPath, err)
	}
	for _, file := range fileInfo {
		if !strings.HasPrefix(file.Name(), ".") {
			files = append(files, file.Name())
		}
	}
	return files, nil
}

func (s *storageService) StoreDir(spaceId string) (path string) {
	return filepath.Join(s.rootPath, spaceId)
}

func (s *storageService) OnWriteHash(onWrite func(ctx context.Context, spaceId string, oldHash, newHash string)) {
	s.onWriteHash = onWrite
}

func (s *storageService) OnDeleteStorage(onDelete func(ctx context.Context, spaceId string)) {
	s.onDeleteStorage = onDelete
}

func (s *storageService) Close(ctx context.Context) (err error) {
	err = s.updater.Close()
	if err != nil {
		log.Error("failed to close updater", zap.Error(err))
	}
	if s.indexStorage != nil {
		return s.indexStorage.Close()
	}
	s.statService.RemoveProvider(s)
	return
}

type storageContainer struct {
	db        anystore.DB
	mx        sync.Mutex
	id        string
	debugInfo string
	created   time.Time
	handlers  int
	isClosing bool
	closeCh   chan struct{}
}

func newStorageContainer(db anystore.DB, id string) *storageContainer {
	return &storageContainer{
		db:      db,
		id:      id,
		created: time.Now(),
	}
}

func (s *storageContainer) Close() (err error) {
	return s.db.Close()
}

func (s *storageContainer) Acquire() (anystore.DB, error) {
	s.mx.Lock()
	if s.isClosing {
		ch := s.closeCh
		s.mx.Unlock()
		<-ch
		return nil, ErrClosed
	}
	s.handlers++
	s.mx.Unlock()
	return s.db, nil
}

func (s *storageContainer) Release() {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.handlers--
}

func (s *storageContainer) TryClose(objectTTL time.Duration) (res bool, err error) {
	s.mx.Lock()
	if s.handlers > 0 {
		s.mx.Unlock()
		return false, nil
	}
	s.isClosing = true
	s.closeCh = make(chan struct{})
	ch := s.closeCh
	db := s.db
	s.mx.Unlock()
	if db != nil {
		if err := db.Close(); err != nil {
			log.Warn("failed to close db", zap.Error(err))
		}
	}
	close(ch)
	return true, nil
}
