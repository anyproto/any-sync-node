package archivestore

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestArchiveStore(t *testing.T) {
	// skip the test because it needs amazon credentials
	t.Skip()

	a := new(app.App)
	store := New()
	a.Register(&config{})
	a.Register(store)
	require.NoError(t, a.Start(ctx))
	defer a.Close(ctx)

	var dataBytes = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	var data = bytes.NewReader(dataBytes)

	require.NoError(t, store.Put(ctx, "test", data))

	reader, err := store.Get(ctx, "test")
	require.NoError(t, err)

	gotData, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	assert.Equal(t, dataBytes, gotData)

	require.NoError(t, store.Delete(ctx, "test"))

	_, err = store.Get(ctx, "test")
	assert.ErrorIs(t, err, ErrNotFound)
}

type config struct {
}

func (c config) Init(a *app.App) error { return nil }
func (c config) Name() string          { return "config" }

func (c config) GetS3Store() Config {
	return Config{
		Enabled:  true,
		Region:   "us-east-1",
		Endpoint: "https://storage.googleapis.com",
		Bucket:   "anytype-cheggaaa-test",
	}
}
