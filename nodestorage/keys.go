package nodestorage

import (
	"bytes"
)

type deletionKeys struct {
}

func (d deletionKeys) SpaceStatusKey(spaceId string) []byte {
	return joinStrings("status", spaceId)
}

func (d deletionKeys) LastRecordIdKey() []byte {
	return []byte("lastRecordId")
}

func joinStrings(strs ...string) []byte {
	var (
		b        bytes.Buffer
		totalLen int
	)
	for _, s := range strs {
		totalLen += len(s)
	}
	// adding separators
	totalLen += len(strs) - 1
	b.Grow(totalLen)
	for idx, s := range strs {
		if idx > 0 {
			b.WriteString("/")
		}
		b.WriteString(s)
	}
	return b.Bytes()
}
