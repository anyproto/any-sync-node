package storage

import (
	"context"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/commonspace/spacestorage"
	"os"
	"path"
	"sync"
)

func New() NodeStorage {
	return &storageService{}
}

type NodeStorage interface {
	spacestorage.SpaceStorageProvider
	AllSpaceIds() (ids []string, err error)
	OnWriteHash(onWrite func(ctx context.Context, spaceId, hash string))
}

type storageService struct {
	rootPath     string
	onWriteHash  func(ctx context.Context, spaceId, hash string)
	lockedSpaces map[string]chan struct{}
	mu           sync.Mutex
}

func (s *storageService) Init(a *app.App) (err error) {
	cfg := a.MustComponent("config").(configGetter).GetStorage()
	s.rootPath = cfg.Path
	s.lockedSpaces = map[string]chan struct{}{}
	return nil
}

func (s *storageService) Name() (name string) {
	return spacestorage.CName
}

func (s *storageService) SpaceStorage(id string) (spacestorage.SpaceStorage, error) {
	return newSpaceStorage(s, id)
}

func (s *storageService) SpaceExists(id string) bool {
	dbPath := path.Join(s.rootPath, id)
	if _, err := os.Stat(dbPath); err != nil {
		return false
	}
	return true
}

func (s *storageService) CreateSpaceStorage(payload spacestorage.SpaceStorageCreatePayload) (spacestorage.SpaceStorage, error) {
	return createSpaceStorage(s.rootPath, payload)
}

func (s *storageService) AllSpaceIds() (ids []string, err error) {
	var files []string
	fileInfo, err := os.ReadDir(s.rootPath)
	if err != nil {
		return files, err
	}

	for _, file := range fileInfo {
		files = append(files, file.Name())
	}
	return files, nil
}

func (s *storageService) OnWriteHash(onWrite func(ctx context.Context, spaceId string, hash string)) {
	s.onWriteHash = onWrite
}
