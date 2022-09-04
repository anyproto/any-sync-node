package acl

import (
	"github.com/anytypeio/any-sync/util/cidutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCIDLen(t *testing.T) {
	s, _ := cidutil.NewCidFromBytes([]byte("some data"))
	t.Log(s, len(s))
	b, _ := cidToByte(s)
	t.Log(b, len(b))
	s2, _ := cidToString(b)
	assert.Equal(t, s, s2)
}