package nodestorage

import (
	"context"
	"github.com/akrylysov/pogreb"
	"github.com/anytypeio/any-sync/app/logger"
	"github.com/anytypeio/any-sync/commonspace/object/acl/liststorage"
	"github.com/anytypeio/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anytypeio/any-sync/commonspace/object/tree/treestorage"
	spacestorage "github.com/anytypeio/any-sync/commonspace/spacestorage"
	"github.com/anytypeio/any-sync/commonspace/spacesyncproto"
	"go.uber.org/zap"
	"os"
	"path"
	"time"
)

var (
	defPogrebOptions    = &pogreb.Options{BackgroundCompactionInterval: time.Minute * 5}
	log                 = logger.NewNamed("storage.spacestorage")
	spaceValidationFunc = spacestorage.ValidateSpaceStorageCreatePayload
)

type spaceStorage struct {
	spaceId         string
	spaceSettingsId string
	objDb           *pogreb.DB
	keys            spaceKeys
	aclStorage      liststorage.ListStorage
	header          *spacesyncproto.RawSpaceHeaderWithId
	service         *storageService
}

func newSpaceStorage(s *storageService, spaceId string) (store spacestorage.SpaceStorage, err error) {
	dbPath := path.Join(s.rootPath, spaceId)
	if _, err = os.Stat(dbPath); err != nil {
		err = spacestorage.ErrSpaceStorageMissing
		return
	}

	objDb, err := pogreb.Open(dbPath, defPogrebOptions)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			log.With(zap.String("id", spaceId), zap.Error(err)).Warn("failed to open storage")
			objDb.Close()
		}
	}()

	keys := newSpaceKeys(spaceId)
	has, err := objDb.Has(keys.SpaceIdKey())
	if err != nil {
		return
	}
	if !has {
		err = spacestorage.ErrSpaceStorageMissing
		return
	}

	header, err := objDb.Get(keys.HeaderKey())
	if err != nil {
		return
	}
	if header == nil {
		err = spacestorage.ErrSpaceStorageMissing
		return
	}

	spaceSettingsId, err := objDb.Get(keys.SpaceSettingsIdKey())
	if err != nil {
		return
	}
	if spaceSettingsId == nil {
		err = spacestorage.ErrSpaceStorageMissing
		return
	}

	aclStorage, err := newListStorage(objDb)
	if err != nil {
		return
	}

	store = &spaceStorage{
		spaceId:         spaceId,
		spaceSettingsId: string(spaceSettingsId),
		objDb:           objDb,
		keys:            keys,
		header: &spacesyncproto.RawSpaceHeaderWithId{
			RawHeader: header,
			Id:        spaceId,
		},
		aclStorage: aclStorage,
		service:    s,
	}
	return
}

func createSpaceStorage(s *storageService, payload spacestorage.SpaceStorageCreatePayload) (store spacestorage.SpaceStorage, err error) {
	log.With(zap.String("id", payload.SpaceHeaderWithId.Id)).Debug("space storage creating")
	dbPath := path.Join(s.rootPath, payload.SpaceHeaderWithId.Id)
	db, err := pogreb.Open(dbPath, defPogrebOptions)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			log.With(zap.String("id", payload.SpaceHeaderWithId.Id), zap.Error(err)).Warn("failed to create storage")
			db.Close()
		}
	}()

	keys := newSpaceKeys(payload.SpaceHeaderWithId.Id)
	has, err := db.Has(keys.SpaceIdKey())
	if err != nil {
		return
	}
	if has {
		err = spacestorage.ErrSpaceStorageExists
		return
	}
	err = spaceValidationFunc(payload)
	if err != nil {
		return
	}

	aclStorage, err := createListStorage(db, payload.AclWithId)
	if err != nil {
		return
	}

	store = &spaceStorage{
		spaceId:         payload.SpaceHeaderWithId.Id,
		objDb:           db,
		keys:            keys,
		aclStorage:      aclStorage,
		spaceSettingsId: payload.SpaceSettingsWithId.Id,
		header:          payload.SpaceHeaderWithId,
		service:         s,
	}

	_, err = store.CreateTreeStorage(treestorage.TreeStorageCreatePayload{
		RootRawChange: payload.SpaceSettingsWithId,
		Changes:       []*treechangeproto.RawTreeChangeWithId{payload.SpaceSettingsWithId},
		Heads:         []string{payload.SpaceSettingsWithId.Id},
	})
	if err != nil {
		return
	}

	err = db.Put(keys.SpaceSettingsIdKey(), []byte(payload.SpaceSettingsWithId.Id))
	if err != nil {
		return
	}

	err = db.Put(keys.HeaderKey(), payload.SpaceHeaderWithId.RawHeader)
	if err != nil {
		return
	}

	err = db.Put(keys.SpaceIdKey(), []byte(payload.SpaceHeaderWithId.Id))
	if err != nil {
		return
	}

	return
}

func (s *spaceStorage) Id() string {
	return s.spaceId
}

func (s *spaceStorage) SpaceSettingsId() string {
	return s.spaceSettingsId
}

func (s *spaceStorage) TreeStorage(id string) (treestorage.TreeStorage, error) {
	return newTreeStorage(s.objDb, id)
}

func (s *spaceStorage) TreeRoot(id string) (*treechangeproto.RawTreeChangeWithId, error) {
	panic("should not be implemented")
}

func (s *spaceStorage) CreateTreeStorage(payload treestorage.TreeStorageCreatePayload) (ts treestorage.TreeStorage, err error) {
	return createTreeStorage(s.objDb, payload)
}

func (s *spaceStorage) AclStorage() (liststorage.ListStorage, error) {
	return s.aclStorage, nil
}

func (s *spaceStorage) SpaceHeader() (header *spacesyncproto.RawSpaceHeaderWithId, err error) {
	return s.header, nil
}

func (s *spaceStorage) SetTreeDeletedStatus(id, state string) (err error) {
	return s.objDb.Put(s.keys.TreeDeletedKey(id), []byte(state))
}

func (s *spaceStorage) TreeDeletedStatus(id string) (status string, err error) {
	res, err := s.objDb.Get(s.keys.TreeDeletedKey(id))
	if err != nil {
		return
	}
	status = string(res)
	return
}

func (s *spaceStorage) StoredIds() (ids []string, err error) {
	index := s.objDb.Items()

	key, _, err := index.Next()
	for err == nil {
		strKey := string(key)
		if isTreeHeadsKey(strKey) {
			ids = append(ids, getRootId(strKey))
		}
		key, _, err = index.Next()
	}

	if err != pogreb.ErrIterationDone {
		return
	}
	err = nil
	return
}

func (s *spaceStorage) WriteSpaceHash(hash string) error {
	if s.service.onWriteHash != nil {
		defer s.service.onWriteHash(context.Background(), s.spaceId, hash)
	}
	return s.objDb.Put(spaceHashKey, []byte(hash))
}

func (s *spaceStorage) ReadSpaceHash() (hash string, err error) {
	v, err := s.objDb.Get(spaceHashKey)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func (s *spaceStorage) Close() (err error) {
	defer s.service.unlockSpaceStorage(s.spaceId)
	return s.objDb.Close()
}