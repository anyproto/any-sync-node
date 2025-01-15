package oldstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getTreeId(t *testing.T) {
	assert.Equal(t, "treeId", getTreeId("t/treeId"))
	assert.Equal(t, "treeId", getTreeId("t/treeId/heads"))

}
