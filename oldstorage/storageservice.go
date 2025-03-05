//go:generate mockgen -destination mock_nodestorage/mock_nodestorage.go github.com/anyproto/any-sync-node/nodestorage NodeStorage
package oldstorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage/oldstorage"
)

var (
	ErrLocked         = errors.New("space storage locked")
	ErrSpaceIdIsEmpty = errors.New("space id is empty")
)

const CName = "node.oldstorage"

func New() NodeStorage {
	return &storageService{}
}

type NodeStorage interface {
	oldstorage.SpaceStorageProvider
	DeletionStorage() DeletionStorage
	SpaceStorage(spaceId string) (oldstorage.SpaceStorage, error)
	TryLockAndDo(spaceId string, do func() error) (err error)
	AllSpaceIds() (ids []string, err error)
	OnDeleteStorage(onDelete func(ctx context.Context, spaceId string))
	OnWriteHash(onWrite func(ctx context.Context, spaceId, hash string))
	StoreDir(spaceId string) (path string)
	DeleteSpaceStorage(ctx context.Context, spaceId string) error
}

type lockSpace struct {
	ch  chan struct{}
	err error
}

type storageService struct {
	rootPath        string
	delStorage      DeletionStorage
	onWriteHash     func(ctx context.Context, spaceId, hash string)
	onDeleteStorage func(ctx context.Context, spaceId string)
	onWriteOldHash  func(ctx context.Context, spaceId, hash string)
	lockedSpaces    map[string]*lockSpace
	mu              sync.Mutex
}

func (s *storageService) Run(ctx context.Context) (err error) {
	s.delStorage, err = OpenDeletionStorage(s.rootPath)
	return
}

func (s *storageService) Close(ctx context.Context) (err error) {
	if s.delStorage != nil {
		return s.delStorage.Close()
	}
	return
}

func (s *storageService) DeletionStorage() DeletionStorage {
	return s.delStorage
}

func (s *storageService) Init(a *app.App) (err error) {
	cfg := a.MustComponent("config").(configGetter).GetStorage()
	s.rootPath = cfg.Path
	s.lockedSpaces = map[string]*lockSpace{}
	return nil
}

func (s *storageService) Name() (name string) {
	return CName
}

func (s *storageService) SpaceStorage(id string) (store oldstorage.SpaceStorage, err error) {
	_, err = s.checkLock(id, func() error {
		store, err = newSpaceStorage(s, id)
		return err
	})
	return
}

func (s *storageService) WaitSpaceStorage(ctx context.Context, id string) (store oldstorage.SpaceStorage, err error) {
	var ls *lockSpace
	ls, err = s.checkLock(id, func() error {
		store, err = newSpaceStorage(s, id)
		return err
	})
	if err == ErrLocked {
		select {
		case <-ls.ch:
			if ls.err != nil {
				return nil, err
			}
			return s.WaitSpaceStorage(ctx, id)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return
}

func (s *storageService) SpaceExists(id string) bool {
	if id == "" {
		return false
	}
	dbPath := path.Join(s.rootPath, id)
	if _, err := os.Stat(dbPath); err != nil {
		return false
	}
	return true
}

func (s *storageService) CreateSpaceStorage(payload spacestorage.SpaceStorageCreatePayload) (store oldstorage.SpaceStorage, err error) {
	_, err = s.checkLock(payload.SpaceHeaderWithId.Id, func() error {
		store, err = createSpaceStorage(s, payload)
		return err
	})
	return
}

func (s *storageService) TryLockAndDo(spaceId string, do func() error) (err error) {
	if _, err = s.checkLock(spaceId, do); err == nil {
		s.unlockSpaceStorage(spaceId)
	}
	return err
}

func (s *storageService) checkLock(id string, openFunc func() error) (ls *lockSpace, err error) {
	if id == "" {
		return nil, ErrSpaceIdIsEmpty
	}
	s.mu.Lock()
	var ok bool
	if ls, ok = s.lockedSpaces[id]; ok {
		s.mu.Unlock()
		return ls, ErrLocked
	}
	ch := make(chan struct{})
	ls = &lockSpace{
		ch: ch,
	}
	s.lockedSpaces[id] = ls
	s.mu.Unlock()
	if err = openFunc(); err != nil {
		s.unlockSpaceStorage(id)
		return nil, err
	}
	return nil, nil
}

func (s *storageService) DeleteSpaceStorage(ctx context.Context, spaceId string) error {
	err := s.waitLock(ctx, spaceId, func() error {
		dbPath := s.StoreDir(spaceId)
		if _, err := os.Stat(dbPath); err != nil {
			if os.IsNotExist(err) {
				return spacestorage.ErrSpaceStorageMissing
			}
			return fmt.Errorf("can't delete datadir '%s': %w", dbPath, err)
		}
		if s.onDeleteStorage != nil {
			s.onDeleteStorage(ctx, spaceId)
		}
		return os.RemoveAll(dbPath)
	})
	if err == nil {
		s.unlockSpaceStorage(spaceId)
	}
	return err
}

func (s *storageService) waitLock(ctx context.Context, id string, action func() error) (err error) {
	var ls *lockSpace
	ls, err = s.checkLock(id, action)
	if err == ErrLocked {
		select {
		case <-ls.ch:
			if ls.err != nil {
				return ls.err
			}
			return s.waitLock(ctx, id, action)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return
}

func (s *storageService) unlockSpaceStorage(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ls, ok := s.lockedSpaces[id]; ok {
		close(ls.ch)
		delete(s.lockedSpaces, id)
	}
}

func (s *storageService) AllSpaceIds() (ids []string, err error) {
	var files []string
	fileInfo, err := os.ReadDir(s.rootPath)
	if err != nil {
		return files, fmt.Errorf("can't read datadir '%v': %v", s.rootPath, err)
	}
	for _, file := range fileInfo {
		parts := strings.Split(file.Name(), ".")
		if !strings.HasPrefix(file.Name(), ".") && len(parts) == 2 && len(parts[0]) == 59 {
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
