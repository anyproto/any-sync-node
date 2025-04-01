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

type PerObjectSizeStats struct {
	LenTotalMax     int     `json:"len"`
	LenTotalP95     float64 `json:"lenTotalP95"`
	LenTotalMedian  float64 `json:"lenTotalMedian"`
	SizeTotalMax    int     `json:"sizeTotal"`
	SizeTotalP95    float64 `json:"SizeTotalP95"`
	SizeTotalMedian float64 `json:"SizeTotalMedian"`
	SizeMax         int     `json:"sizeMax"`
	SizeP95         float64 `json:"sizeP95"`
	SizeMedian      float64 `json:"sizeMedian"`
}

type ObjectSpaceStats struct {
	ObjectsCount        int                `json:"objectsCount,omitempty"`
	DeletedObjectsCount int                `json:"deletedObjectsCount"`
	ChangesCount        int                `json:"changesCount"`
	ChangeSize          ChangeSizeStats    `json:"changeSizeStats,omitempty"`
	PerObjectSize       PerObjectSizeStats `json:"perObjectSizeStats,omitempty"`
	TreeStats           []TreeStat         `json:"treeStats,omitempty"`
	treeMap             map[string]TreeStat
}

type TreeStat struct {
	Id                 string `json:"id"`
	ChangesCount       int    `json:"changesCount"`
	SnapshotsCount     int    `json:"snapshotsCount"`
	MaxSnapshotCounter int    `json:"maxSnapshotCounter"`
	ChangesSumSize     int    `json:"payloadSize"`
	ChangeMaxSize      int    `json:"changeMaxSize"`
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

func (st *nodeStorage) OnHashChange(oldHash, newHash string) {
	st.observer(st.Id(), oldHash, newHash)
}

type hashObserver = func(spaceId, oldHash, newHash string)

func newNodeStorage(spaceStorage spacestorage.SpaceStorage, cont *storageContainer, observer hashObserver) *nodeStorage {
	st := &nodeStorage{
		SpaceStorage: spaceStorage,
		cont:         cont,
		observer:     observer,
	}
	st.StateStorage().SetObserver(st)
	return st
}

func (st *nodeStorage) Close(ctx context.Context) (err error) {
	defer st.cont.Release()
	return st.SpaceStorage.Close(ctx)
}
