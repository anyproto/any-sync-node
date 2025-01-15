package oldstorage

import (
	"context"
	"errors"

	"github.com/akrylysov/pogreb"
	"github.com/anyproto/any-sync/commonspace/spacestorage/oldstorage"
	"github.com/anyproto/any-sync/consensus/consensusproto"
)

var (
	ErrUnknownAclId  = errors.New("unknown acl id")
	ErrUnknownRecord = errors.New("unknown record")
)

type listStorage struct {
	db   *pogreb.DB
	keys aclKeys
	id   string
	root *consensusproto.RawRecordWithId
}

func newListStorage(db *pogreb.DB) (ls oldstorage.ListStorage, err error) {
	keys := aclKeys{}
	rootId, err := db.Get(keys.RootIdKey())
	if err != nil {
		return
	}
	if rootId == nil {
		err = ErrUnknownAclId
		return
	}

	root, err := db.Get(keys.RawRecordKey(string(rootId)))
	if err != nil {
		return
	}
	if root == nil {
		err = ErrUnknownAclId
		return
	}

	rootWithId := &consensusproto.RawRecordWithId{
		Payload: root,
		Id:      string(rootId),
	}

	ls = &listStorage{
		db:   db,
		keys: aclKeys{},
		id:   string(rootId),
		root: rootWithId,
	}
	return
}

func createListStorage(db *pogreb.DB, root *consensusproto.RawRecordWithId) (ls oldstorage.ListStorage, err error) {
	keys := aclKeys{}
	has, err := db.Has(keys.RootIdKey())
	if err != nil {
		return
	}
	if has {
		return newListStorage(db)
	}

	err = db.Put(keys.HeadIdKey(), []byte(root.Id))
	if err != nil {
		return
	}

	err = db.Put(keys.RawRecordKey(root.Id), root.Payload)
	if err != nil {
		return
	}

	err = db.Put(keys.RootIdKey(), []byte(root.Id))
	if err != nil {
		return
	}

	ls = &listStorage{
		db:   db,
		keys: aclKeys{},
		id:   root.Id,
		root: root,
	}
	return
}

func (l *listStorage) Id() string {
	return l.id
}

func (l *listStorage) Root() (*consensusproto.RawRecordWithId, error) {
	return l.root, nil
}

func (l *listStorage) Head() (head string, err error) {
	bytes, err := l.db.Get(l.keys.HeadIdKey())
	if err != nil {
		return
	}
	if bytes == nil {
		err = ErrUnknownAclId
		return
	}
	head = string(bytes)
	return
}

func (l *listStorage) GetRawRecord(ctx context.Context, id string) (raw *consensusproto.RawRecordWithId, err error) {
	res, err := l.db.Get(l.keys.RawRecordKey(id))
	if err != nil {
		return
	}
	if res == nil {
		err = ErrUnknownRecord
		return
	}

	raw = &consensusproto.RawRecordWithId{
		Payload: res,
		Id:      id,
	}
	return
}

func (l *listStorage) SetHead(headId string) (err error) {
	return l.db.Put(l.keys.HeadIdKey(), []byte(headId))
}

func (l *listStorage) AddRawRecord(ctx context.Context, rec *consensusproto.RawRecordWithId) error {
	return l.db.Put(l.keys.RawRecordKey(rec.Id), rec.Payload)
}
