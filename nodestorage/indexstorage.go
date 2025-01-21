package nodestorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

type SpaceStatus int

const (
	SpaceStatusOk SpaceStatus = iota
	SpaceStatusRemove
)

var (
	ErrUnknownSpaceId = errors.New("unknown space id")
	ErrNoLastRecordId = errors.New("no last record id")
)

const (
	IndexStorageName = ".index"
	collectionName   = "deletionIndex"
	statusKey        = "s"
	recordIdKey      = "r"
)

type IndexStorage interface {
	SetSpaceStatus(ctx context.Context, spaceId string, status SpaceStatus, recId string) (err error)
	SpaceStatus(ctx context.Context, spaceId string) (status SpaceStatus, err error)
	LastRecordId(ctx context.Context) (id string, err error)
	Close() (err error)
}

type indexStorage struct {
	db    anystore.DB
	coll  anystore.Collection
	arena *anyenc.Arena
}

func (d *indexStorage) SpaceStatus(ctx context.Context, spaceId string) (status SpaceStatus, err error) {
	doc, err := d.coll.FindId(ctx, spaceId)
	if err != nil {
		err = fmt.Errorf("find id: %w, %w", err, ErrUnknownSpaceId)
		return
	}
	return SpaceStatus(doc.Value().GetInt(statusKey)), nil
}

func (d *indexStorage) SetSpaceStatus(ctx context.Context, spaceId string, status SpaceStatus, recId string) (err error) {
	tx, err := d.coll.WriteTx(ctx)
	if err != nil {
		return
	}
	defer d.arena.Reset()
	doc := d.arena.NewObject()
	doc.Set("id", d.arena.NewString(spaceId))
	doc.Set(statusKey, d.arena.NewNumberFloat64(float64(status)))
	doc.Set(recordIdKey, d.arena.NewString(recId))
	err = d.coll.Insert(tx.Context(), doc)
	if err != nil {
		tx.Rollback()
		return
	}
	return tx.Commit()
}

func (d *indexStorage) LastRecordId(ctx context.Context) (id string, err error) {
	iter, err := d.coll.Find(query.All{}).Sort(recordIdKey).Iter(ctx)
	if err != nil {
		return
	}
	if iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return "", err
		}
		return doc.Value().GetString(recordIdKey), nil
	}
	return "", ErrNoLastRecordId
}

func (d *indexStorage) Close() (err error) {
	return d.db.Close()
}

func OpenIndexStorage(ctx context.Context, rootPath string) (ds IndexStorage, err error) {
	log.Debug("deletion storage opening")
	dbPath := path.Join(rootPath, IndexStorageName)
	err = os.MkdirAll(dbPath, 0755)
	if err != nil {
		return
	}
	dbPath = path.Join(dbPath, "store.db")
	db, err := anystore.Open(ctx, dbPath, nil)
	if err != nil {
		return
	}
	coll, err := db.Collection(ctx, collectionName)
	if err != nil {
		return
	}
	info := anystore.IndexInfo{
		Fields: []string{recordIdKey},
		Unique: true,
	}
	err = coll.EnsureIndex(ctx, info)
	if err != nil {
		return
	}
	ds = &indexStorage{db: db, coll: coll, arena: &anyenc.Arena{}}
	return
}
