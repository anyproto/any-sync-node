package nodestorage

import (
	"fmt"
	"strings"

	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
)

type aclKeys struct {
}

var aclHeadIdKey = []byte("a/headId")
var aclRootIdKey = []byte("a/rootId")

func (a aclKeys) HeadIdKey() []byte {
	return aclHeadIdKey
}

func (a aclKeys) RootIdKey() []byte {
	return aclRootIdKey
}

func (a aclKeys) RawRecordKey(id string) []byte {
	return treestorage.JoinStringsToBytes("a", id)
}

type treeKeys struct {
	id       string
	prefix   string
	headsKey []byte
}

func newTreeKeys(id string) treeKeys {
	return treeKeys{
		id:       id,
		headsKey: treestorage.JoinStringsToBytes("t", id, "heads"),
		prefix:   fmt.Sprintf("t/%s", id),
	}
}

func (t treeKeys) HeadsKey() []byte {
	return t.headsKey
}

func (t treeKeys) RawChangeKey(id string) []byte {
	return treestorage.JoinStringsToBytes("t", t.id, id)
}

func (t treeKeys) isTreeRelatedKey(key string) bool {
	return strings.HasPrefix(key, t.prefix)
}

type spaceKeys struct {
	headerKey []byte
}

func newSpaceKeys(spaceId string) spaceKeys {
	return spaceKeys{headerKey: treestorage.JoinStringsToBytes("s", spaceId)}
}

var (
	spaceIdKey         = []byte("spaceId")
	spaceSettingsIdKey = []byte("spaceSettingsId")
	deletedKey         = []byte("spaceDeleted")
	spaceHashKey       = []byte("spaceHash")
)

func (s spaceKeys) SpaceIdKey() []byte {
	return spaceIdKey
}

func (s spaceKeys) HeaderKey() []byte {
	return s.headerKey
}

func (s spaceKeys) SpaceSettingsIdKey() []byte {
	return spaceSettingsIdKey
}

func (s spaceKeys) SpaceDeletedKey() []byte {
	return deletedKey
}

func (s spaceKeys) TreeDeletedKey(id string) []byte {
	return treestorage.JoinStringsToBytes("del", id)
}

func isTreeHeadsKey(key string) bool {
	return strings.HasPrefix(key, "t/") && strings.HasSuffix(key, "/heads")
}

type deletionKeys struct {
}

func (d deletionKeys) SpaceStatusKey(spaceId string) []byte {
	return treestorage.JoinStringsToBytes("status", spaceId)
}

func (d deletionKeys) LastRecordIdKey() []byte {
	return []byte("lastRecordId")
}

func getRootId(key string) string {
	prefixLen := 2 // len("t/")
	suffixLen := 6 // len("/heads")
	rootLen := len(key) - suffixLen - prefixLen
	sBuf := strings.Builder{}
	sBuf.Grow(rootLen)
	for i := prefixLen; i < prefixLen+rootLen; i++ {
		sBuf.WriteByte(key[i])
	}
	return sBuf.String()
}

func getTreeId(key string) string {
	prefixIdx := strings.IndexByte(key, '/')
	if prefixIdx == -1 {
		return ""
	}
	key = key[prefixIdx+1:]
	nextSlash := strings.IndexByte(key, '/')
	if nextSlash == -1 {
		return key
	} else {
		return key[:nextSlash]
	}
}
