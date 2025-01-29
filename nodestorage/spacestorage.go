package nodestorage

import (
	"context"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
)

var (
	defPogrebOptions = &pogreb.Options{BackgroundCompactionInterval: time.Minute * 5}
	log              = logger.NewNamed("storage.spacestorage")
)

type ChangeSizeStats struct {
	MaxLen int     `json:"maxLen"`
	P95    float64 `json:"p95"`
	Avg    float64 `json:"avg"`
	Median float64 `json:"median"`
	Total  int     `json:"total"`
}

type ObjectSpaceStats struct {
	ObjectsCount        int             `json:"objectsCount,omitempty"`
	DeletedObjectsCount int             `json:"deletedObjectsCount"`
	ChangesCount        int             `json:"changesCount"`
	ChangeSize          ChangeSizeStats `json:"changeSizeStats,omitempty"`
	TreeStats           []TreeStat      `json:"treeStats,omitempty"`
	treeMap             map[string]TreeStat
}

type TreeStat struct {
	Id                 string `json:"id"`
	ChangesCount       int    `json:"changesCount"`
	SnapshotsCount     int    `json:"snapshotsCount"`
	MaxSnapshotCounter int    `json:"maxSnapshotCounter"`
	ChangesSumSize     int    `json:"payloadSize"`
}

type SpaceStats struct {
	Storage ObjectSpaceStats `json:"storage"`
	Acl     struct {
		Readers int `json:"readers"`
		Writers int `json:"writers"`
	} `json:"acl"`
}

type NodeStorageStats interface {
	GetSpaceStats(ctx context.Context, treeTop int) (ObjectSpaceStats, error)
}

type nodeStorage struct {
	spacestorage.SpaceStorage
	cont     *storageContainer
	observer hashObserver
}

func (r *nodeStorage) OnHashChange(hash string) {
	r.observer(r.Id(), hash)
}

type hashObserver = func(spaceId, hash string)

func newNodeStorage(spaceStorage spacestorage.SpaceStorage, cont *storageContainer, observer hashObserver) *nodeStorage {
	st := &nodeStorage{
		SpaceStorage: spaceStorage,
		cont:         cont,
		observer:     observer,
	}
	st.StateStorage().SetObserver(st)
	return st
}

func (r *nodeStorage) Close(ctx context.Context) (err error) {
	defer r.cont.Release()
	return r.SpaceStorage.Close(ctx)
}
