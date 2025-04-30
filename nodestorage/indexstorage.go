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
	SpaceStatusRemovePrepare
)

var (
	ErrUnknownSpaceId = errors.New("unknown space id")
	ErrNoLastRecordId = errors.New("no last record id")
)

const (
	IndexStorageName = ".index"
	delCollName      = "deletionIndex"
	hashCollName     = "hashesIndex"
	newHashKey       = "nh"
	oldHashKey       = "oh"
	statusKey        = "s"
	recordIdKey      = "r"
)

type IndexStorage interface {
	UpdateHash(ctx context.Context, update SpaceUpdate) (err error)
	RemoveHash(ctx context.Context, spaceId string) (err error)
	ReadHashes(ctx context.Context, iterFunc func(update SpaceUpdate) (bool, error)) (err error)
	SetSpaceStatus(ctx context.Context, spaceId string, status SpaceStatus, recId string) (err error)
	SpaceStatus(ctx context.Context, spaceId string) (status SpaceStatus, err error)
	LastRecordId(ctx context.Context) (id string, err error)
	Close() (err error)
}

type indexStorage struct {
	db         anystore.DB
	statusColl anystore.Collection
	hashesColl anystore.Collection
	arenaPool  *anyenc.ArenaPool
}

func (d *indexStorage) UpdateHash(ctx context.Context, update SpaceUpdate) (err error) {
	tx, err := d.hashesColl.WriteTx(ctx)
	if err != nil {
		return err
	}
	arena := d.arenaPool.Get()
	defer d.arenaPool.Put(arena)
	doc := arena.NewObject()
	doc.Set("id", arena.NewString(update.SpaceId))
	doc.Set(oldHashKey, arena.NewString(update.OldHash))
	doc.Set(newHashKey, arena.NewString(update.NewHash))
	err = d.hashesColl.UpsertOne(tx.Context(), doc)
	if err != nil {
		tx.Rollback()
		return
	}
	return tx.Commit()
}

func (d *indexStorage) RemoveHash(ctx context.Context, spaceId string) (err error) {
	tx, err := d.hashesColl.WriteTx(ctx)
	if err != nil {
		return
	}
	err = d.removeHashTx(tx.Context(), spaceId)
	if err != nil {
		tx.Rollback()
		return
	}
	return tx.Commit()
}

func (d *indexStorage) removeHashTx(ctx context.Context, spaceId string) (err error) {
	err = d.hashesColl.DeleteId(ctx, spaceId)
	if errors.Is(err, anystore.ErrDocNotFound) {
		return nil
	}
	return err
}

func (d *indexStorage) ReadHashes(ctx context.Context, iterFunc func(update SpaceUpdate) (bool, error)) (err error) {
	iter, err := d.hashesColl.Find(query.Key{Path: []string{"id"}, Filter: query.All{}}).Sort("id").Iter(ctx)
	if err != nil {
		return
	}
	defer iter.Close()
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return err
		}
		cont, err := iterFunc(SpaceUpdate{
			SpaceId: doc.Value().GetString("id"),
			OldHash: doc.Value().GetString(oldHashKey),
			NewHash: doc.Value().GetString(newHashKey),
		})
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (d *indexStorage) SpaceStatus(ctx context.Context, spaceId string) (status SpaceStatus, err error) {
	doc, err := d.statusColl.FindId(ctx, spaceId)
	if err != nil {
		err = fmt.Errorf("find id: %w, %w", err, ErrUnknownSpaceId)
		return
	}
	return SpaceStatus(doc.Value().GetInt(statusKey)), nil
}

func (d *indexStorage) SetSpaceStatus(ctx context.Context, spaceId string, status SpaceStatus, recId string) (err error) {
	tx, err := d.statusColl.WriteTx(ctx)
	if err != nil {
		return
	}
	arena := d.arenaPool.Get()
	defer d.arenaPool.Put(arena)
	doc := arena.NewObject()
	doc.Set("id", arena.NewString(spaceId))
	doc.Set(statusKey, arena.NewNumberFloat64(float64(status)))
	doc.Set(recordIdKey, arena.NewString(recId))
	err = d.statusColl.UpsertOne(tx.Context(), doc)
	if err != nil {
		tx.Rollback()
		return
	}
	if status == SpaceStatusRemove {
		err = d.removeHashTx(tx.Context(), spaceId)
		if err != nil {
			tx.Rollback()
			return
		}
	}
	return tx.Commit()
}

func (d *indexStorage) LastRecordId(ctx context.Context) (id string, err error) {
	iter, err := d.statusColl.Find(query.All{}).Sort("-" + recordIdKey).Iter(ctx)
	if err != nil {
		return
	}
	defer iter.Close()
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
	statusColl, err := db.Collection(ctx, delCollName)
	if err != nil {
		return
	}
	hashesColl, err := db.Collection(ctx, hashCollName)
	if err != nil {
		return
	}
	info := anystore.IndexInfo{
		Fields: []string{recordIdKey},
		Unique: true,
	}
	err = statusColl.EnsureIndex(ctx, info)
	if err != nil {
		return
	}
	ds = &indexStorage{db: db, statusColl: statusColl, hashesColl: hashesColl, arenaPool: &anyenc.ArenaPool{}}
	return
}
