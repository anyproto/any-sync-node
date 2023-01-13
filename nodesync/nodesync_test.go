package nodesync

import (
	"context"
	"github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/anytypeio/any-sync/testutil/accounttest"
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

	conf := newConfig()
	var nodeCount = 9
	var accService accountservice.Service
	for i := 0; i < nodeCount; i++ {
		ac := &accounttest.AccountTestService{}
		require.NoError(t, ac.Init(nil))
		if i == 0 {
			accService = ac
		}
		conf.nodes = append(conf.nodes, ac.NodeConf(nil))
	}

	fx.a.Register(conf).
		Register(nodeconf.New()).
		Register(accService).
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

func newConfig() *config {
	return &config{}
}

type config struct {
	nodes []nodeconf.NodeConfig
}

func (c *config) Init(a *app.App) (err error) { return }
func (c *config) Name() string                { return "config" }

func (c *config) GetNodeSync() Config {
	return Config{
		SyncOnStart: false,
	}
}

func (c *config) GetNodes() []nodeconf.NodeConfig {
	return c.nodes
}
