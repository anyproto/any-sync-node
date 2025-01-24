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
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
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
	path      string
	handlers  int
	isClosing bool
	closeCh   chan struct{}
}

func newStorageContainer(db anystore.DB, path string) *storageContainer {
	return &storageContainer{
		db:      db,
		closeCh: make(chan struct{}),
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
	ErrLocked         = errors.New("space storage locked")
	ErrSpaceIdIsEmpty = errors.New("space id is empty")
)

const CName = spacestorage.CName

var anyStoreConfig *anystore.Config = &anystore.Config{
	ReadConnections: 4,
	SQLiteConnectionOptions: map[string]string{
		"synchronous": "off",
	},
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
	OnWriteHash(onWrite func(ctx context.Context, spaceId, hash string))
	StoreDir(spaceId string) (path string)
	DeleteSpaceStorage(ctx context.Context, spaceId string) error
}

type storageService struct {
	rootPath        string
	cache           ocache.OCache
	indexStorage    IndexStorage
	onWriteHash     func(ctx context.Context, spaceId, hash string)
	onDeleteStorage func(ctx context.Context, spaceId string)
	onWriteOldHash  func(ctx context.Context, spaceId, hash string)
	currentSpaces   map[string]*storageContainer
	mu              sync.Mutex
}

func (s *storageService) onHashChange(spaceId, hash string) {
	if s.onWriteHash != nil {
		s.onWriteHash(context.Background(), spaceId, hash)
	}
}

func (s *storageService) Run(ctx context.Context) (err error) {
	s.indexStorage, err = OpenIndexStorage(ctx, s.rootPath)
	return
}

func (s *storageService) Close(ctx context.Context) (err error) {
	if s.indexStorage != nil {
		return s.indexStorage.Close()
	}
	return
}

func (s *storageService) IndexStorage() IndexStorage {
	return s.indexStorage
}

func (s *storageService) Init(a *app.App) (err error) {
	cfg := a.MustComponent("config").(configGetter).GetStorage()
	s.rootPath = cfg.AnyStorePath
	if _, err = os.Stat(s.rootPath); err != nil {
		err = os.MkdirAll(s.rootPath, 0755)
		if err != nil {
			return err
		}
	}
	s.cache = ocache.New(s.loadFunc,
		ocache.WithLogger(log.Sugar()),
		ocache.WithGCPeriod(time.Minute),
		ocache.WithTTL(60*time.Second))
	return nil
}

func (s *storageService) Name() (name string) {
	return CName
}

func (s *storageService) openDb(ctx context.Context, id string) (db anystore.DB, err error) {
	dbPath := path.Join(s.rootPath, id, "store.db")
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, spacestorage.ErrSpaceStorageMissing
		}
		return nil, err
	}
	return anystore.Open(ctx, dbPath, anyStoreConfig)
}

func (s *storageService) createDb(ctx context.Context, id string) (db anystore.DB, err error) {
	dirPath := path.Join(s.rootPath, id)
	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		return nil, err
	}
	dbPath := path.Join(dirPath, "store.db")
	return anystore.Open(ctx, dbPath, anyStoreConfig)
}

func (s *storageService) loadFunc(ctx context.Context, id string) (value ocache.Object, err error) {
	if fn, ok := ctx.Value(doKeyVal).(doFunc); ok {
		err := fn()
		if err != nil {
			return nil, err
		}
		// continue to open
	} else if ctx.Value(createKeyVal) != nil {
		if s.SpaceExists(id) {
			return nil, spacestorage.ErrSpaceStorageExists
		}
		db, err := s.createDb(ctx, id)
		if err != nil {
			return nil, err
		}
		cont := &storageContainer{
			path: path.Join(s.rootPath, id),
			db:   db,
		}
		return cont, nil
	}
	db, err := s.openDb(ctx, id)
	if err != nil {
		return nil, err
	}
	return newStorageContainer(db, path.Join(s.rootPath, id)), nil
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
		return nil, err
	}
	return newNodeStorage(st, cont, s.onHashChange), nil
}

func (s *storageService) SpaceExists(id string) bool {
	if id == "" {
		return false
	}
	dbPath := path.Join(s.rootPath, id, "store.db")
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
		return nil, err
	}
	return newNodeStorage(st, cont, s.onHashChange), nil
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
	if _, err := os.Stat(spacePath); err != nil {
		if os.IsNotExist(err) {
			return spacestorage.ErrSpaceStorageMissing
		}
		return fmt.Errorf("can't delete datadir '%s': %w", spacePath, err)
	}
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

func (s *storageService) OnWriteHash(onWrite func(ctx context.Context, spaceId string, hash string)) {
	s.onWriteHash = onWrite
}

func (s *storageService) OnWriteOldHash(onWrite func(ctx context.Context, spaceId string, hash string)) {
	s.onWriteOldHash = onWrite
}

func (s *storageService) OnDeleteStorage(onDelete func(ctx context.Context, spaceId string)) {
	s.onDeleteStorage = onDelete
}
