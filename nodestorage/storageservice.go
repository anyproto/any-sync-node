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
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/metric"
	"go.uber.org/zap"
)

var (
	ErrClosed  = errors.New("space storage closed")
	ErrDeleted = errors.New("space storage deleted")
)

type optKey int

const (
	createKeyVal optKey = 0
	doKeyVal     optKey = 1
)

type doFunc = func() error

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

var (
	ErrLocked                  = errors.New("space storage locked")
	ErrSpaceIdIsEmpty          = errors.New("space id is empty")
	ErrDoesntSupportSpaceStats = errors.New("doesn't support nodestorage.ObjectSpaceStats")
)

const CName = spacestorage.CName

func anyStoreConfig() *anystore.Config {
	return &anystore.Config{
		ReadConnections: 4,
		SQLiteConnectionOptions: map[string]string{
			"synchronous": "off",
		},
	}
}

func New() NodeStorage {
	return &storageService{}
}

type NodeStorage interface {
	spacestorage.SpaceStorageProvider
	IndexStorage() IndexStorage
	SpaceStorage(ctx context.Context, spaceId string) (spacestorage.SpaceStorage, error)
	TryLockAndDo(ctx context.Context, spaceId string, do func() error) (err error)
	DumpStorage(ctx context.Context, id string, do func(path string) error) (err error)
	AllSpaceIds() (ids []string, err error)
	OnDeleteStorage(onDelete func(ctx context.Context, spaceId string))
	OnWriteHash(onWrite func(ctx context.Context, spaceId, oldHash, newHash string))
	StoreDir(spaceId string) (path string)
	DeleteSpaceStorage(ctx context.Context, spaceId string) error
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

func (s *storageService) Run(ctx context.Context) (err error) {
	s.updater.Run()
	s.indexStorage, err = OpenIndexStorage(ctx, s.rootPath)
	return
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

func (s *storageService) IndexStorage() IndexStorage {
	return s.indexStorage
}

func (s *storageService) Init(a *app.App) (err error) {
	cfg := a.MustComponent("config").(configGetter).GetStorage()
	s.updater = newSpaceUpdater(func(updates []SpaceUpdate) {
		if s.indexStorage == nil {
			return
		}
		for _, update := range updates {
			if err := s.indexStorage.UpdateHash(context.Background(), update); err != nil {
				log.Error("failed to update hash", zap.String("spaceId", update.SpaceId), zap.Error(err))
			}
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
		os.RemoveAll(s.StoreDir(id))
		return nil, spacestorage.ErrSpaceStorageMissing
	}
	cont = newStorageContainer(db, id)
	return cont, nil
}

func (s *storageService) get(ctx context.Context, id string) (container *storageContainer, err error) {
	cont, err := s.cache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return cont.(*storageContainer), nil
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
	aclList, err := list.BuildAclListWithIdentity(identity, aclStorage, list.NoOpAcceptorVerifier{})
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
	_, err = s.cache.Remove(context.Background(), id)
	return
}

func (s *storageService) TryLockAndDo(ctx context.Context, spaceId string, do func() error) (err error) {
	ctx = context.WithValue(ctx, doKeyVal, do)
	_, err = s.get(ctx, spaceId)
	return err
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
