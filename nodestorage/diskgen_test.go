package nodestorage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadOrCreateDiskGen(t *testing.T) {
	dir := t.TempDir()

	gen, err := loadOrCreateDiskGen(dir)
	require.NoError(t, err)
	assert.True(t, gen.Fresh())
	assert.NotEmpty(t, gen.GenId)

	gen2, err := loadOrCreateDiskGen(dir)
	require.NoError(t, err)
	assert.False(t, gen2.Fresh())
	assert.Equal(t, gen.GenId, gen2.GenId)

	// corrupted marker regenerates but is not treated as fresh
	require.NoError(t, os.WriteFile(filepath.Join(dir, diskGenFileName), []byte("garbage"), 0o644))
	gen3, err := loadOrCreateDiskGen(dir)
	require.NoError(t, err)
	assert.False(t, gen3.Fresh())
	assert.NotEmpty(t, gen3.GenId)
	assert.NotEqual(t, gen.GenId, gen3.GenId)
}
