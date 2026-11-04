package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFromFile_StorageAnyStoreSynchronous(t *testing.T) {
	for _, tc := range []struct {
		yaml string
		want string
	}{
		{yaml: "", want: ""},
		// a YAML 1.1 boolean word, still decoded as the string it is
		{yaml: "  anyStoreSynchronous: off\n", want: "off"},
		{yaml: "  anyStoreSynchronous: normal\n", want: "normal"},
	} {
		path := filepath.Join(t.TempDir(), "config.yml")
		require.NoError(t, os.WriteFile(path, []byte("storage:\n  anyStorePath: anyDb\n"+tc.yaml), 0o644))
		c, err := NewFromFile(path)
		require.NoError(t, err)
		assert.Equal(t, tc.want, c.GetStorage().AnyStoreSynchronous)
	}
}
