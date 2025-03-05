package nodestorage

import (
	"context"
	"fmt"
	"testing"

	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
)

type testChangeBuilder struct {
}

func (t testChangeBuilder) Unmarshall(rawIdChange *treechangeproto.RawTreeChangeWithId, verify bool) (ch *objecttree.Change, err error) {
	return &objecttree.Change{IsDerived: false}, nil
}

func (t testChangeBuilder) UnmarshallReduced(rawIdChange *treechangeproto.RawTreeChangeWithId) (ch *objecttree.Change, err error) {
	panic("should not call")
}

func (t testChangeBuilder) Build(payload objecttree.BuilderContent) (ch *objecttree.Change, raw *treechangeproto.RawTreeChangeWithId, err error) {
	panic("should not call")
}

func (t testChangeBuilder) BuildRoot(payload objecttree.InitialContent) (ch *objecttree.Change, raw *treechangeproto.RawTreeChangeWithId, err error) {
	panic("should not call")
}

func (t testChangeBuilder) BuildDerivedRoot(payload objecttree.InitialDerivedContent) (ch *objecttree.Change, raw *treechangeproto.RawTreeChangeWithId, err error) {
	panic("should not call")
}

func (t testChangeBuilder) Marshall(ch *objecttree.Change) (*treechangeproto.RawTreeChangeWithId, error) {
	panic("should not call")
}

func CreateTreeStorage(t *testing.T, storage spacestorage.SpaceStorage, treeLen, changeLen int) {
	for i := 0; i < treeLen; i++ {
		raw := make([]byte, changeLen)
		payload := treestorage.TreeStorageCreatePayload{
			RootRawChange: &treechangeproto.RawTreeChangeWithId{
				Id:        fmt.Sprintf("root-%d", i),
				RawChange: raw,
			},
		}
		_, err := storage.CreateTreeStorage(context.Background(), payload)
		require.NoError(t, err)
	}
}

func GenStorage(t *testing.T, ss NodeStorage, treeLen, changeLen int) spacestorage.SpaceStorage {
	payload := NewStorageCreatePayload(t)
	store, err := ss.CreateSpaceStorage(context.Background(), payload)
	objecttree.StorageChangeBuilder = func(keys crypto.KeyStorage, rootChange *treechangeproto.RawTreeChangeWithId) objecttree.ChangeBuilder {
		return testChangeBuilder{}
	}
	require.NoError(t, err)
	CreateTreeStorage(t, store, treeLen, changeLen)
	return store
}

func NewStorageCreatePayload(t *testing.T) spacestorage.SpaceStorageCreatePayload {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	masterKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	readKey := crypto.NewAES()
	meta := []byte("account")
	payload := commonspace.SpaceCreatePayload{
		SigningKey:     keys.SignKey,
		SpaceType:      "space",
		ReplicationKey: 10,
		SpacePayload:   nil,
		MasterKey:      masterKey,
		ReadKey:        readKey,
		MetadataKey:    metaKey,
		Metadata:       meta,
	}
	createSpace, err := commonspace.StoragePayloadForSpaceCreate(payload)
	require.NoError(t, err)
	return createSpace
}
