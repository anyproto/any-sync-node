package nodestorage

import (
	"context"
	"os"
	"path"
	"sort"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/acl/liststorage"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	spacestorage "github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"go.uber.org/zap"
)

var (
	defPogrebOptions = &pogreb.Options{BackgroundCompactionInterval: time.Minute * 5}
	log              = logger.NewNamed("storage.spacestorage")
)

type ChangeSizeStats struct {
	MaxLen int     `json:"maxLen,omitempty"`
	P95    float64 `json:"p95,omitempty"`
	Avg    float64 `json:"avg,omitempty"`
	Median float64 `json:"median,omitempty"`
}

type SpaceStats struct {
	DocsCount  int             `json:"docsCount,omitempty"`
	ChangeSize ChangeSizeStats `json:"changeSizeStats,omitempty"`
}

type NodeStorageStats interface {
	GetSpaceStats() (SpaceStats, error)
}

type spaceStorage struct {
	spaceId         string
	spaceSettingsId string
	objDb           *pogreb.DB
	keys            spaceKeys
	aclStorage      liststorage.ListStorage
	header          *spacesyncproto.RawSpaceHeaderWithId
	service         *storageService
}

func (s *spaceStorage) Run(ctx context.Context) (err error) {
	return nil
}

func (s *spaceStorage) Init(a *app.App) (err error) {
	return nil
}

func (s *spaceStorage) Name() (name string) {
	return spacestorage.CName
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

func (s *spaceStorage) SetSpaceDeleted() error {
	return s.objDb.Put(s.keys.SpaceDeletedKey(), s.keys.SpaceDeletedKey())
}

func (s *spaceStorage) IsSpaceDeleted() (bool, error) {
	res, err := s.objDb.Get(s.keys.SpaceDeletedKey())
	if err != nil {
		return false, err
	}
	return res != nil, err
}

func (s *spaceStorage) HasTree(id string) (bool, error) {
	keys := newTreeKeys(id)
	heads, err := s.objDb.Get(keys.HeadsKey())
	if err != nil {
		return false, err
	}
	return heads != nil, nil
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

func (s *spaceStorage) Close(ctx context.Context) (err error) {
	defer s.service.unlockSpaceStorage(s.spaceId)
	return s.objDb.Close()
}

func calcMedian(sortedLengths []int) (median float64) {
	mid := len(sortedLengths) / 2
	if len(sortedLengths)%2 == 0 {
		median = float64(sortedLengths[mid-1]+sortedLengths[mid]) / 2.0
	} else {
		median = float64(sortedLengths[mid])
	}
	return
}

func calcAvg(lengths []int) (avg float64) {
	sum := 0
	for _, n := range lengths {
		sum += n
	}

	avg = float64(sum) / float64(len(lengths))
	return
}

func calcP95(sortedLengths []int) (percentile float64) {
	if len(sortedLengths) == 1 {
		percentile = float64(sortedLengths[0])
		return
	}

	p := 95.0
	r := (p/100)*(float64(len(sortedLengths))-1.0) + 1
	ri := int(r)
	if r == float64(int64(r)) {
		percentile = float64(sortedLengths[ri-1])
	} else if r > 1 {
		rf := r - float64(ri)
		percentile = float64(sortedLengths[ri-1]) + rf*float64(sortedLengths[ri]-sortedLengths[ri-1])
	}

	return
}

func (s *spaceStorage) GetSpaceStats() (spaceStats SpaceStats, err error) {
	index := s.objDb.Items()
	maxLen := 0
	docsCount := 0
	lengths := make([]int, 0, 100)
	_, val, err := index.Next()
	for err == nil {
		docsCount += 1
		curLen := len(val)
		lengths = append(lengths, curLen)
		if curLen > maxLen {
			maxLen = curLen
		}
		_, val, err = index.Next()
	}

	if err != pogreb.ErrIterationDone {
		return
	}
	err = nil

	sort.Ints(lengths)
	median := calcMedian(lengths)
	avg := calcAvg(lengths)
	p95 := calcP95(lengths)
	spaceStats = SpaceStats{
		DocsCount: docsCount,
		ChangeSize: ChangeSizeStats{
			MaxLen: maxLen,
			Avg:    avg,
			Median: median,
			P95:    p95,
		},
	}

	return
}
