package archivestore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealS3 exercises the store against a real S3-compatible service.
// Opt-in: set ARCHIVE_TEST_S3_ACCESS_KEY, ARCHIVE_TEST_S3_SECRET_KEY,
// ARCHIVE_TEST_S3_BUCKET and optionally ARCHIVE_TEST_S3_ENDPOINT
// (e.g. https://storage.googleapis.com for GCS interop).
func TestRealS3(t *testing.T) {
	accessKey := os.Getenv("ARCHIVE_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("ARCHIVE_TEST_S3_SECRET_KEY")
	bucket := os.Getenv("ARCHIVE_TEST_S3_BUCKET")
	if accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("real S3 credentials are not configured")
	}
	endpoint := os.Getenv("ARCHIVE_TEST_S3_ENDPOINT")
	region := os.Getenv("ARCHIVE_TEST_S3_REGION")
	if region == "" {
		region = "auto"
	}

	newStore := func(t *testing.T, prefix string) ArchiveStore {
		s := New()
		a := new(app.App)
		a.Register(realTestConfig{cfg: Config{
			Enabled:        true,
			Region:         region,
			Bucket:         bucket,
			Endpoint:       endpoint,
			ForcePathStyle: true,
			KeyPrefix:      prefix,
			Shared:         true,
			Credentials:    Credentials{AccessKey: accessKey, SecretKey: secretKey},
		}}).Register(s)
		require.NoError(t, a.Start(context.Background()))
		t.Cleanup(func() { _ = a.Close(context.Background()) })
		return s
	}

	runId := fmt.Sprintf("reshardtest-%d", time.Now().UnixNano())
	nodeA := newStore(t, runId+"/nodeA")
	nodeB := newStore(t, runId+"/nodeB")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	const name = "space.test"
	payload := []byte("resharding-archive-test-payload")

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), time.Minute)
		defer ccancel()
		_ = nodeA.Delete(cctx, name)
		_ = nodeB.Delete(cctx, name)
	})

	// missing object: Exists false, Get/CopyFrom -> ErrNotFound
	ok, err := nodeA.Exists(ctx, name)
	require.NoError(t, err)
	assert.False(t, ok)
	_, err = nodeA.Get(ctx, name)
	assert.ErrorIs(t, err, ErrNotFound)
	err = nodeB.CopyFrom(ctx, nodeA.Key(name), name)
	assert.ErrorIs(t, err, ErrNotFound)

	// put + head + get on node A
	require.NoError(t, nodeA.Put(ctx, name, bytes.NewReader(payload)))
	ok, err = nodeA.Exists(ctx, name)
	require.NoError(t, err)
	assert.True(t, ok)
	rd, err := nodeA.Get(ctx, name)
	require.NoError(t, err)
	got, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.NoError(t, rd.Close())
	assert.Equal(t, payload, got)

	// server-side copy A -> B (the AdoptArchive transfer path)
	require.NoError(t, nodeB.CopyFrom(ctx, nodeA.Key(name), name))
	ok, err = nodeB.Exists(ctx, name)
	require.NoError(t, err)
	assert.True(t, ok)
	rd, err = nodeB.Get(ctx, name)
	require.NoError(t, err)
	got, err = io.ReadAll(rd)
	require.NoError(t, err)
	require.NoError(t, rd.Close())
	assert.Equal(t, payload, got)

	// list both prefixes (the sweeper path)
	var listedA, listedB []string
	require.NoError(t, nodeA.List(ctx, func(n string, lastModified time.Time) (bool, error) {
		listedA = append(listedA, n)
		assert.WithinDuration(t, time.Now(), lastModified, time.Minute*10)
		return true, nil
	}))
	require.NoError(t, nodeB.List(ctx, func(n string, _ time.Time) (bool, error) {
		listedB = append(listedB, n)
		return true, nil
	}))
	assert.Equal(t, []string{name}, listedA)
	assert.Equal(t, []string{name}, listedB)

	// delete on A must not affect B (per-node prefixes isolate blast radius);
	// this also guards the GCS interop quirk: HEAD can be stale after DELETE,
	// which is why Exists is list-based
	require.NoError(t, nodeA.Delete(ctx, name))
	ok, err = nodeA.Exists(ctx, name)
	require.NoError(t, err)
	assert.False(t, ok)
	ok, err = nodeB.Exists(ctx, name)
	require.NoError(t, err)
	assert.True(t, ok)

	require.NoError(t, nodeB.Delete(ctx, name))
}

type realTestConfig struct {
	cfg Config
}

func (c realTestConfig) Init(_ *app.App) error { return nil }
func (c realTestConfig) Name() string          { return "config" }
func (c realTestConfig) GetS3Store() Config    { return c.cfg }
