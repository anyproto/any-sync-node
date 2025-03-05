package oldstorage

import (
	"errors"
	"path"

	"github.com/akrylysov/pogreb"
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

const deletionStorageName = ".deletion"

type DeletionStorage interface {
	SpaceStatus(spaceId string) (status SpaceStatus, err error)
	SetSpaceStatus(spaceId string, status SpaceStatus) (err error)
	LastRecordId() (id string, err error)
	SetLastRecordId(id string) (err error)
	Close() (err error)
}

type deletionStorage struct {
	db   *pogreb.DB
	keys deletionKeys
}

func (d *deletionStorage) SpaceStatus(spaceId string) (status SpaceStatus, err error) {
	statusBytes, err := d.db.Get(d.keys.SpaceStatusKey(spaceId))
	if err != nil {
		return
	}
	if statusBytes == nil {
		err = ErrUnknownSpaceId
		return
	}
	return SpaceStatus(statusBytes[0]), nil
}

func (d *deletionStorage) SetSpaceStatus(spaceId string, status SpaceStatus) (err error) {
	return d.db.Put(d.keys.SpaceStatusKey(spaceId), []byte{byte(status)})
}

func (d *deletionStorage) LastRecordId() (id string, err error) {
	lastRecordBytes, err := d.db.Get(d.keys.LastRecordIdKey())
	if err != nil {
		return
	}
	if lastRecordBytes == nil {
		err = ErrNoLastRecordId
		return
	}
	return string(lastRecordBytes), nil
}

func (d *deletionStorage) SetLastRecordId(id string) (err error) {
	return d.db.Put(d.keys.LastRecordIdKey(), []byte(id))
}

func (d *deletionStorage) Close() (err error) {
	return d.db.Close()
}

func OpenDeletionStorage(rootPath string) (ds DeletionStorage, err error) {
	log.Debug("deletion storage opening")
	dbPath := path.Join(rootPath, deletionStorageName)
	db, err := pogreb.Open(dbPath, defPogrebOptions)
	if err != nil {
		return
	}
	ds = &deletionStorage{db, deletionKeys{}}
	return
}
