package nodestorage

import (
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/anyproto/any-sync/app/logger"
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

type SpaceStats struct {
	ObjectsCount        int             `json:"objectsCount,omitempty"`
	DeletedObjectsCount int             `json:"deletedObjectsCount"`
	ChangesCount        int             `json:"changesCount"`
	ChangeSize          ChangeSizeStats `json:"changeSizeStats,omitempty"`
	TreeStats           []TreeStat      `json:"treeStats,omitempty"`
	treeMap             map[string]TreeStat
}

type TreeStat struct {
	Id             string `json:"id"`
	ChangesCount   int    `json:"changesCount"`
	SnapshotsCount int    `json:"snapshotsCount"`
	ChangesSumSize int    `json:"payloadSize"`
}

type NodeStorageStats interface {
	GetSpaceStats(treeTop int) (SpaceStats, error)
}
