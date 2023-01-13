package testnodeconf

import (
	"github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/nodeconf"
	"github.com/anytypeio/any-sync/testutil/accounttest"
)

func GenNodeConfig(num int) (accountService accountservice.Service, conf *Config) {
	conf = &Config{}
	if num <= 0 {
		num = 1
	}
	for i := 0; i < num; i++ {
		ac := &accounttest.AccountTestService{}
		if err := ac.Init(nil); err != nil {
			panic(err)
		}
		if i == 0 {
			accountService = ac
		}
		conf.nodes = append(conf.nodes, ac.NodeConf(nil))
	}
	return accountService, conf
}

type Config struct {
	nodes []nodeconf.NodeConfig
}

func (c *Config) Init(a *app.App) (err error) { return }
func (c *Config) Name() string                { return "config" }

func (c *Config) GetNodes() []nodeconf.NodeConfig {
	return c.nodes
}
