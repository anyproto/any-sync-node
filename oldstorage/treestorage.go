package oldstorage

import (
	"context"
	"errors"

	"github.com/akrylysov/pogreb"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage/oldstorage"
)

type treeStorage struct {
	db   *pogreb.DB
	keys treeKeys
	id   string
	root *treechangeproto.RawTreeChangeWithId
}

func newTreeStorage(db *pogreb.DB, treeId string) (ts oldstorage.TreeStorage, err error) {
	keys := newTreeKeys(treeId)
	heads, err := db.Get(keys.HeadsKey())
	if err != nil {
		return
	}
	if heads == nil {
		err = treestorage.ErrUnknownTreeId
		return
	}

	root, err := db.Get(keys.RawChangeKey(treeId))
	if err != nil {
		return
	}
	if root == nil {
		err = treestorage.ErrUnknownTreeId
		return
	}

	rootWithId := &treechangeproto.RawTreeChangeWithId{
		RawChange: root,
		Id:        treeId,
	}
	ts = &treeStorage{
		db:   db,
		keys: keys,
		id:   treeId,
		root: rootWithId,
	}
	return
}

func createTreeStorage(db *pogreb.DB, payload treestorage.TreeStorageCreatePayload) (ts oldstorage.TreeStorage, err error) {
	keys := newTreeKeys(payload.RootRawChange.Id)
	has, err := db.Has(keys.HeadsKey())
	if err != nil {
		return
	}
	if has {
		err = treestorage.ErrTreeExists
		return
	}

	heads := treestorage.CreateHeadsPayload(payload.Heads)

	for _, ch := range payload.Changes {
		err = db.Put(keys.RawChangeKey(ch.Id), ch.GetRawChange())
		if err != nil {
			return
		}
	}

	err = db.Put(keys.RawChangeKey(payload.RootRawChange.Id), payload.RootRawChange.GetRawChange())
	if err != nil {
		return
	}

	err = db.Put(keys.HeadsKey(), heads)
	if err != nil {
		return
	}

	ts = &treeStorage{
		db:   db,
		keys: keys,
		id:   payload.RootRawChange.Id,
		root: payload.RootRawChange,
	}
	return
}

func (t *treeStorage) Id() string {
	return t.id
}

func (t *treeStorage) Root() (raw *treechangeproto.RawTreeChangeWithId, err error) {
	return t.root, nil
}

func (t *treeStorage) Heads() (heads []string, err error) {
	headsBytes, err := t.db.Get(t.keys.HeadsKey())
	if err != nil {
		return
	}
	if headsBytes == nil {
		err = treestorage.ErrUnknownTreeId
		return
	}
	heads = treestorage.ParseHeads(headsBytes)
	return
}

func (t *treeStorage) SetHeads(heads []string) (err error) {
	payload := treestorage.CreateHeadsPayload(heads)
	return t.db.Put(t.keys.HeadsKey(), payload)
}

func (t *treeStorage) AddRawChange(change *treechangeproto.RawTreeChangeWithId) (err error) {
	return t.db.Put(t.keys.RawChangeKey(change.Id), change.RawChange)
}

func (t *treeStorage) GetAppendRawChange(ctx context.Context, buf []byte, id string) (raw *treechangeproto.RawTreeChangeWithId, err error) {
	res, err := t.db.GetAppend(t.keys.RawChangeKey(id), buf)
	if err != nil {
		return
	}
	if res == nil {
		err = treestorage.ErrUnknownChange
	}

	raw = &treechangeproto.RawTreeChangeWithId{
		RawChange: res,
		Id:        id,
	}
	return
}

func (t *treeStorage) GetRawChange(ctx context.Context, id string) (raw *treechangeproto.RawTreeChangeWithId, err error) {
	res, err := t.db.Get(t.keys.RawChangeKey(id))
	if err != nil {
		return
	}
	if res == nil {
		err = treestorage.ErrUnknownChange
	}

	raw = &treechangeproto.RawTreeChangeWithId{
		RawChange: res,
		Id:        id,
	}
	return
}

func (t *treeStorage) HasChange(ctx context.Context, id string) (bool, error) {
	return t.db.Has(t.keys.RawChangeKey(id))
}

func (t *treeStorage) AddRawChangesSetHeads(changes []*treechangeproto.RawTreeChangeWithId, heads []string) error {
	for _, ch := range changes {
		if err := t.AddRawChange(ch); err != nil {
			return err
		}
	}
	return t.SetHeads(heads)
}

func (t *treeStorage) GetAllChangeIds() (storedKeys []string, err error) {
	return t.storedIds()
}

func (t *treeStorage) Delete() (err error) {
	storedKeys, err := t.storedKeys()
	if err != nil {
		return
	}
	for _, k := range storedKeys {
		err = t.db.Delete(k)
		if err != nil {
			return
		}
	}
	return
}

func (t *treeStorage) storedKeys() (keys [][]byte, err error) {
	index := t.db.Items()

	key, _, err := index.Next()
	for err == nil {
		strKey := string(key)
		if t.keys.isTreeRelatedKey(strKey) {
			keys = append(keys, key)
		}
		key, _, err = index.Next()
	}

	if !errors.Is(err, pogreb.ErrIterationDone) {
		return
	}
	err = nil
	return
}

func (t *treeStorage) storedIds() (ids []string, err error) {
	index := t.db.Items()

	key, _, err := index.Next()
	for err == nil {
		strKey := string(key)
		if t.keys.isTreeRelatedKey(strKey) {
			id := getId(strKey)
			if id != "heads" {
				ids = append(ids, getId(strKey))
			}
		}
		key, _, err = index.Next()
	}

	if !errors.Is(err, pogreb.ErrIterationDone) {
		return
	}
	err = nil
	return
}

func getId(key string) (id string) {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			return key[i+1:]
		}
	}
	return key
}
