package nodestorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
	"go.uber.org/zap"
)

type SpaceStatus int

const (
	SpaceStatusOk SpaceStatus = iota
	SpaceStatusRemove
	SpaceStatusRemovePrepare
	SpaceStatusArchived
)

var (
	ErrUnknownSpaceId = errors.New("unknown space id")
	ErrNoLastRecordId = errors.New("no last record id")
)

const (
	IndexStorageName           = ".index"
	migrationStateCollName     = "migrationState"
	spaceCollName              = "space"
	settingsCollName           = "settings"
	newHashKey                 = "nh"
	oldHashKey                 = "oh"
	statusKey                  = "s"
	lastAccessKey              = "la"
	valueKey                   = "v"
	archiveSizeCompressedKey   = "asc"
	archiveSizeUncompressedKey = "asu"
	diffMigrationKey           = "diffState"
	diffVersionKey             = "diffVersion"

	lastDeletionIdKey = "lastDeletionId"
)

type IndexStorage interface {
	UpdateHash(ctx context.Context, updates ...SpaceUpdate) (err error)
	ReadHashes(ctx context.Context, iterFunc func(update SpaceUpdate) (bool, error)) (err error)
	UpdateHashes(ctx context.Context, updateFunc func(spaceId, newHash, oldHash string) (newNewHash, newOldHash string, shouldUpdate bool)) (err error)
	SetSpaceStatus(ctx context.Context, spaceId string, status SpaceStatus, recId string) (err error)
	SpaceStatus(ctx context.Context, spaceId string) (status SpaceStatus, err error)
	MarkArchived(ctx context.Context, spaceId string, compressedSize, uncompressedSize int64) (err error)
	LastRecordId(ctx context.Context) (id string, err error)
	FindOldestInactiveSpace(ctx context.Context, olderThan time.Duration) (spaceId string, err error)

	UpdateLastAccess(ctx context.Context, spaceId string) (err error)
	GetDiffMigrationVersion(ctx context.Context) (version int, err error)
	SetDiffMigrationVersion(ctx context.Context, version int) (err error)
	RunMigrations(ctx context.Context) (err error)
	Close() (err error)
}

type indexStorage struct {
	db              anystore.DB
	settingsColl    anystore.Collection
	spaceColl       anystore.Collection
	arenaPool       *anyenc.ArenaPool
	lastAccessCache *sync.Map
}

func (d *indexStorage) UpdateHash(ctx context.Context, updates ...SpaceUpdate) (err error) {
	tx, err := d.db.WriteTx(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	ctx = tx.Context()

	for _, update := range updates {
		_, err = d.spaceColl.UpsertId(ctx, update.SpaceId, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
			if update.Updated.IsZero() {
				update.Updated = time.Now()
			}
			v.Set(oldHashKey, a.NewString(update.OldHash))
			v.Set(newHashKey, a.NewString(update.NewHash))
			v.Set(lastAccessKey, a.NewNumberFloat64(float64(update.Updated.Unix())))
			if v.Get(statusKey) == nil {
				v.Set(statusKey, a.NewNumberInt(int(SpaceStatusOk)))
			}
			d.lastAccessCache.Store(update.SpaceId, update.Updated)
			return v, true, nil
		}))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

var _a = &anyenc.Arena{}

var filterStatusOk = query.Key{
	Path:   []string{statusKey},
	Filter: query.NewCompValue(query.CompOpEq, _a.NewNumberInt(int(SpaceStatusOk))),
}

func (d *indexStorage) ReadHashes(ctx context.Context, iterFunc func(update SpaceUpdate) (bool, error)) (err error) {
	iter, err := d.spaceColl.Find(filterStatusOk).Sort("id").Iter(ctx)
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
			Updated: time.Unix(int64(doc.Value().GetInt(lastAccessKey)), 0),
		})
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (d *indexStorage) SpaceStatus(ctx context.Context, spaceId string) (status SpaceStatus, err error) {
	doc, err := d.spaceColl.FindId(ctx, spaceId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return SpaceStatusOk, nil
		} else {
			err = fmt.Errorf("find id: %w", err)
			return
		}
	}
	return SpaceStatus(doc.Value().GetInt(statusKey)), nil
}

func (d *indexStorage) SetSpaceStatus(ctx context.Context, spaceId string, status SpaceStatus, recId string) (err error) {
	tx, err := d.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		_ = tx.Rollback()
	}()
	ctx = tx.Context()

	_, err = d.spaceColl.UpsertId(ctx, spaceId, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
		v.Set(statusKey, a.NewNumberInt(int(status)))
		v.Set(lastAccessKey, a.NewNumberInt(int(time.Now().Unix())))
		if status == SpaceStatusRemove {
			v.Set(oldHashKey, a.NewNull())
			v.Set(newHashKey, a.NewNull())
		}
		return v, true, nil
	}))
	if err != nil {
		return
	}
	if recId == "" {
		return tx.Commit()
	}
	_, err = d.settingsColl.UpsertId(ctx, lastDeletionIdKey, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
		prevKey := v.GetString(valueKey)
		if prevKey < recId {
			v.Set(valueKey, a.NewString(recId))
			return v, true, nil
		}
		return v, false, nil
	}))
	return tx.Commit()
}

func (d *indexStorage) MarkArchived(ctx context.Context, spaceId string, compressedSize, uncompressedSize int64) (err error) {
	_, err = d.spaceColl.UpdateId(ctx, spaceId, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
		v.Set(archiveSizeCompressedKey, a.NewNumberInt(int(compressedSize)))
		v.Set(archiveSizeUncompressedKey, a.NewNumberInt(int(uncompressedSize)))
		v.Set(statusKey, a.NewNumberInt(int(SpaceStatusArchived)))
		return v, true, nil
	}))
	return err
}

func (d *indexStorage) LastRecordId(ctx context.Context) (id string, err error) {
	doc, err := d.settingsColl.FindId(ctx, lastDeletionIdKey)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return "", ErrNoLastRecordId
		}
		return "", err
	}
	return doc.Value().GetString(valueKey), nil
}

func (d *indexStorage) UpdateLastAccess(ctx context.Context, spaceId string) (err error) {
	now := time.Now()
	if val, ok := d.lastAccessCache.Load(spaceId); ok {
		if val.(time.Time).Add(time.Minute * 5).After(now) {
			return nil
		}
	}
	_, err = d.spaceColl.UpsertId(ctx, spaceId, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
		v.Set(lastAccessKey, a.NewNumberFloat64(float64(now.Unix())))
		return v, true, nil
	}))
	if err != nil {
		return err
	}
	d.lastAccessCache.Store(spaceId, now)
	return nil
}

func (d *indexStorage) FindOldestInactiveSpace(ctx context.Context, olderThan time.Duration) (spaceId string, err error) {
	// cutoff: lastAccess must be strictly earlier than now - olderThan
	cutoffUnix := time.Now().Add(-olderThan).Unix()

	a := d.arenaPool.Get()
	defer d.arenaPool.Put(a)

	// status == Ok AND lastAccess < cutoff
	filter := query.And{
		filterStatusOk,
		query.Key{
			Path:   []string{lastAccessKey},
			Filter: query.NewCompValue(query.CompOpLt, a.NewNumberFloat64(float64(cutoffUnix))),
		},
	}

	iter, err := d.spaceColl.Find(filter).Sort(lastAccessKey).Iter(ctx) // ASC by lastAccess
	if err != nil {
		return "", err
	}
	defer func() {
		_ = iter.Close()
	}()

	if !iter.Next() {
		return "", anystore.ErrDocNotFound
	}

	doc, err := iter.Doc()
	if err != nil {
		return "", err
	}

	spaceId = doc.Value().GetString("id")

	return spaceId, nil
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
	_, err = d.spaceColl.Find(filterStatusOk).Update(ctx, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
		spaceId := v.GetString("id")
		newHash := v.GetString(newHashKey)
		oldHash := v.GetString(oldHashKey)

		newNewHash, newOldHash, shouldUpdate := updateFunc(spaceId, newHash, oldHash)
		if !shouldUpdate {
			return v, false, nil
		}

		v.Set(newHashKey, a.NewString(newNewHash))
		v.Set(oldHashKey, a.NewString(newOldHash))
		if v.Get(statusKey) == nil {
			v.Set(statusKey, a.NewNumberInt(int(SpaceStatusOk)))
		}
		return v, true, nil
	}))
	return
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

	if err = migrateToSingleCollection(ctx, db); err != nil {
		return
	}

	spaceColl, err := db.Collection(ctx, spaceCollName)
	if err != nil {
		return
	}
	settingsColl, err := db.Collection(ctx, settingsCollName)
	if err != nil {
		return
	}

	if err = spaceColl.EnsureIndex(ctx, anystore.IndexInfo{
		Fields: []string{statusKey, lastAccessKey},
	}); err != nil {
		return
	}

	ds = &indexStorage{
		db:              db,
		settingsColl:    settingsColl,
		spaceColl:       spaceColl,
		arenaPool:       &anyenc.ArenaPool{},
		lastAccessCache: &sync.Map{},
	}
	return
}

func migrateToSingleCollection(ctx context.Context, db anystore.DB) (err error) {
	// old names
	const (
		delCollName  = "deletionIndex"
		hashCollName = "hashesIndex"
		recordIdKey  = "r"
	)

	statusColl, err := db.OpenCollection(ctx, delCollName)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			// already migrated
			return nil
		}
		return
	}
	hashesColl, err := db.Collection(ctx, hashCollName)
	if err != nil {
		return
	}

	settingsColl, err := db.Collection(ctx, settingsCollName)
	if err != nil {
		return
	}

	tx, err := db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback() }()
	ctx = tx.Context()

	statusCount, err := statusColl.Count(ctx)
	if err != nil {
		return
	}

	var (
		maxDeletionLogId string
		nowUnix          = time.Now().Unix()
	)

	log.Info("migrating index to single collection: set statuses...", zap.Int("count", statusCount))
	err = func() (err error) {
		st := time.Now()
		iter, err := statusColl.Find(nil).Iter(ctx)
		if err != nil {
			return
		}
		defer func() { _ = iter.Close() }()

		for iter.Next() {
			doc, err := iter.Doc()
			if err != nil {
				return err
			}

			delLogId := doc.Value().GetString(recordIdKey)
			if delLogId > maxDeletionLogId {
				maxDeletionLogId = delLogId
			}
			status := SpaceStatus(doc.Value().GetInt(statusKey))
			_, err = hashesColl.UpsertId(
				ctx,
				doc.Value().GetString("id"),
				query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
					v.Set(statusKey, a.NewNumberInt(int(status)))
					if status == SpaceStatusRemove {
						v.Del(oldHashKey)
						v.Del(newHashKey)
					}
					return v, true, nil
				}))
			if err != nil {
				return err
			}
		}
		log.Info("migrating index to single collection: set statuses done", zap.Duration("dur", time.Since(st)))
		return
	}()
	if err != nil {
		return
	}

	hashesCount, err := hashesColl.Count(ctx)
	if err != nil {
		return
	}

	log.Info("migrating index to single collection: set last access time...", zap.Int("count", hashesCount))
	st := time.Now()
	_, err = hashesColl.Find(nil).Update(ctx, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
		status := SpaceStatus(v.GetInt(statusKey))
		v.Set(statusKey, a.NewNumberInt(int(status)))
		v.Set(lastAccessKey, a.NewNumberInt(int(nowUnix)))
		return v, true, nil
	}))
	if err != nil {
		return
	}
	log.Info("migrating index to single collection: set last access time done", zap.Duration("dur", time.Since(st)))

	a := &anyenc.Arena{}
	lastIdObj := a.NewObject()
	lastIdObj.Set("id", a.NewString(lastDeletionIdKey))
	lastIdObj.Set(valueKey, a.NewString(maxDeletionLogId))
	if err = settingsColl.UpsertOne(ctx, lastIdObj); err != nil {
		return
	}

	st = time.Now()
	log.Info("migrating index to single collection: rename, drop statuses and commit...")
	defer func() {
		log.Info("migrating index to single collection: rename, drop statuses and commit done", zap.Duration("dur", time.Since(st)), zap.Error(err))
	}()
	if err = hashesColl.Rename(ctx, spaceCollName); err != nil {
		return
	}

	if err = statusColl.Drop(ctx); err != nil {
		return
	}

	return tx.Commit()
}
