package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/commonspace/spacestorage"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

var ErrLocked = errors.New("space storage locked")

const CName = spacestorage.CName

func New() NodeStorage {
	return &storageService{}
}

type NodeStorage interface {
	spacestorage.SpaceStorageProvider
	SpaceStorage(spaceId string) (spacestorage.SpaceStorage, error)
	TryLockAndDo(spaceId string, do func() error) (err error)
	AllSpaceIds() (ids []string, err error)
	OnWriteHash(onWrite func(ctx context.Context, spaceId, hash string))
	StoreDir(spaceId string) (path string)
}

type lockSpace struct {
	ch  chan struct{}
	err error
}

type storageService struct {
	rootPath     string
	onWriteHash  func(ctx context.Context, spaceId, hash string)
	lockedSpaces map[string]*lockSpace
	mu           sync.Mutex
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

func (s *storageService) SpaceStorage(id string) (store spacestorage.SpaceStorage, err error) {
	_, err = s.checkLock(id, func() error {
		store, err = newSpaceStorage(s, id)
		return err
	})
	return
}

func (s *storageService) WaitSpaceStorage(ctx context.Context, id string) (store spacestorage.SpaceStorage, err error) {
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
	dbPath := path.Join(s.rootPath, id)
	if _, err := os.Stat(dbPath); err != nil {
		return false
	}
	return true
}

func (s *storageService) CreateSpaceStorage(payload spacestorage.SpaceStorageCreatePayload) (store spacestorage.SpaceStorage, err error) {
	_, err = s.checkLock(payload.SpaceHeaderWithId.Id, func() error {
		store, err = createSpaceStorage(s, payload)
		return err
	})
	return
}

func (s *storageService) TryLockAndDo(spaceId string, do func() error) (err error) {
	_, err = s.checkLock(spaceId, do)
	return err
}

func (s *storageService) checkLock(id string, openFunc func() error) (ls *lockSpace, err error) {
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
