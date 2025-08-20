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
	IndexStorageName       = ".index"
	delCollName            = "deletionIndex"
	hashCollName           = "hashesIndex"
	migrationStateCollName = "migrationState"
	newHashKey             = "nh"
	oldHashKey             = "oh"
	statusKey              = "s"
	recordIdKey            = "r"
	diffMigrationKey       = "diffState"
	diffVersionKey         = "diffVersion"
)

type IndexStorage interface {
	UpdateHash(ctx context.Context, update SpaceUpdate) (err error)
	RemoveHash(ctx context.Context, spaceId string) (err error)
	ReadHashes(ctx context.Context, iterFunc func(update SpaceUpdate) (bool, error)) (err error)
	UpdateHashes(ctx context.Context, updateFunc func(spaceId, newHash, oldHash string) (newNewHash, newOldHash string, shouldUpdate bool)) (err error)
	SetSpaceStatus(ctx context.Context, spaceId string, status SpaceStatus, recId string) (err error)
	SpaceStatus(ctx context.Context, spaceId string) (status SpaceStatus, err error)
	LastRecordId(ctx context.Context) (id string, err error)
	GetDiffMigrationVersion(ctx context.Context) (version int, err error)
	SetDiffMigrationVersion(ctx context.Context, version int) (err error)
	RunMigrations(ctx context.Context) (err error)
	Close() (err error)
}

type indexStorage struct {
	db         anystore.DB
	statusColl anystore.Collection
	hashesColl anystore.Collection
	arenaPool  *anyenc.ArenaPool
}

func (d *indexStorage) UpdateHash(ctx context.Context, update SpaceUpdate) (err error) {
	arena := d.arenaPool.Get()
	defer d.arenaPool.Put(arena)
	doc := arena.NewObject()
	doc.Set("id", arena.NewString(update.SpaceId))
	doc.Set(oldHashKey, arena.NewString(update.OldHash))
	doc.Set(newHashKey, arena.NewString(update.NewHash))
	return d.hashesColl.UpsertOne(ctx, doc)
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

func (d *indexStorage) RunMigrations(ctx context.Context) (err error) {
	diffMigration, err := newDiffMigration(d, log)
	if err != nil {
		return fmt.Errorf("failed to create diff migration: %w", err)
	}

	if err := diffMigration.Run(ctx); err != nil {
		return fmt.Errorf("diff migration failed: %w", err)
	}

	return nil
}

func (d *indexStorage) UpdateHashes(ctx context.Context, updateFunc func(spaceId, newHash, oldHash string) (newNewHash, newOldHash string, shouldUpdate bool)) (err error) {
	iter, err := d.hashesColl.Find(query.All{}).Iter(ctx)
	if err != nil {
		return err
	}
	defer iter.Close()

	tx, err := d.hashesColl.WriteTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return err
		}

		value := doc.Value()
		spaceId := value.GetString("id")
		newHash := value.GetString(newHashKey)
		oldHash := value.GetString(oldHashKey)

		newNewHash, newOldHash, shouldUpdate := updateFunc(spaceId, newHash, oldHash)
		if !shouldUpdate {
			continue
		}

		arena := d.arenaPool.Get()
		updatedDoc := arena.NewObject()
		updatedDoc.Set("id", arena.NewString(spaceId))
		updatedDoc.Set(newHashKey, arena.NewString(newNewHash))
		updatedDoc.Set(oldHashKey, arena.NewString(newOldHash))

		err = d.hashesColl.UpsertOne(tx.Context(), updatedDoc)
		d.arenaPool.Put(arena)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *indexStorage) GetDiffMigrationVersion(ctx context.Context) (version int, err error) {
	migrationColl, err := d.db.Collection(ctx, migrationStateCollName)
	if err != nil {
		return 0, err
	}

	doc, err := migrationColl.FindId(ctx, diffMigrationKey)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return int(doc.Value().GetFloat64(diffVersionKey)), nil
}

func (d *indexStorage) SetDiffMigrationVersion(ctx context.Context, version int) (err error) {
	migrationColl, err := d.db.Collection(ctx, migrationStateCollName)
	if err != nil {
		return err
	}

	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
		if v == nil {
			v = a.NewObject()
			v.Set("id", a.NewString(diffMigrationKey))
		}
		v.Set(diffVersionKey, a.NewNumberFloat64(float64(version)))
		return v, true, nil
	})

	_, err = migrationColl.UpsertId(ctx, diffMigrationKey, mod)
	return err
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
