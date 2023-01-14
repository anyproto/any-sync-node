package nodehead

import (
	"context"
	"encoding/hex"
	"github.com/anytypeio/any-sync-node/storage"
	"github.com/anytypeio/any-sync-node/testutil/testnodeconf"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/app/ldiff"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"testing"
)

var ctx = context.Background()

func TestNodeHead_SetHead(t *testing.T) {
	fx := newFixture(t)
	defer fx.Finish(t)

	getHash := func(part int) string {
		res, err := fx.Ranges(ctx, part, []ldiff.Range{{
			From:  0,
			To:    math.MaxUint64,
			Limit: 1,
		}}, nil)
		require.NoError(t, err)
		require.Len(t, res, 1)
		return hex.EncodeToString(res[0].Hash)
	}

	t.Run("set head", func(t *testing.T) {
		part, err := fx.SetHead(ctx, "2.2", "head")
		require.NoError(t, err)
		h1 := getHash(part)
		part2, err := fx.SetHead(ctx, "3.2", "head")
		assert.Equal(t, part, part2)
		h2 := getHash(part)
		assert.NotEqual(t, h1, h2)
	})

}

func TestNodeHead_Ranges(t *testing.T) {
	fx := newFixture(t)
	defer fx.Finish(t)

	t.Run("partition not exists", func(t *testing.T) {
		res, err := fx.Ranges(ctx, 1, []ldiff.Range{{
			From:  0,
			To:    math.MaxUint64,
			Limit: 1,
		}}, nil)
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, 0, res[0].Count)
	})
}

func newFixture(t *testing.T) *fixture {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	fx := &fixture{
		NodeHead: New(),
		a:        new(app.App),
		dataPath: tmpDir,
	}
	accServ, confServ := testnodeconf.GenNodeConfig(3)
	fx.a.Register(nodeconf.New()).
		Register(accServ).
		Register(&config{Config: confServ}).
		Register(fx.NodeHead).
		Register(storage.New())
	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	NodeHead
	a        *app.App
	dataPath string
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	if fx.dataPath != "" {
		_ = os.RemoveAll(fx.dataPath)
	}
}

type config struct {
	*testnodeconf.Config
	dataPath string
}

func (c *config) GetStorage() storage.Config {
	return storage.Config{
		Path: c.dataPath,
	}
}
