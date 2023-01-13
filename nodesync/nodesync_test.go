package nodesync

import (
	"context"
	"github.com/anytypeio/any-sync-node/testutil/testnodeconf"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var ctx = context.Background()

func TestNodeSync_getRelatePartitions(t *testing.T) {
	fx := newFixture(t)
	defer fx.Finish(t)
	st := time.Now()
	parts, err := fx.getRelatePartitions()
	require.NoError(t, err)
	t.Log("parts", len(parts), time.Since(st))
	for _, p := range parts {
		assert.Len(t, p.peers, 2)
	}
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		nodeSync: New().(*nodeSync),
		a:        new(app.App),
	}
	accServ, confServ := testnodeconf.GenNodeConfig(9)
	fx.a.Register(nodeconf.New()).
		Register(accServ).
		Register(&config{Config: confServ}).
		Register(fx.nodeSync)
	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	*nodeSync
	a *app.App
}

func (fx *fixture) Finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
}

type config struct {
	*testnodeconf.Config
}

func (c config) GetNodeSync() Config {
	return Config{}
}
