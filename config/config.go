package config

import (
	"os"

	commonaccount "github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/config"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/rpc"
	"github.com/anyproto/any-sync/net/rpc/debugserver"
	"github.com/anyproto/any-sync/net/rpc/limiter"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/anyproto/any-sync/nodeconf"
	"gopkg.in/yaml.v3"

	"github.com/anyproto/any-sync-node/nodestorage"
	"github.com/anyproto/any-sync-node/nodesync"
	"github.com/anyproto/any-sync-node/nodesync/hotsync"
)

const CName = "config"

func NewFromFile(path string) (c *Config, err error) {
	c = &Config{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err = yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}
	return
}

type Config struct {
	Drpc                     rpc.Config             `yaml:"drpc"`
	Account                  commonaccount.Config   `yaml:"account"`
	APIServer                debugserver.Config     `yaml:"apiServer"`
	Network                  nodeconf.Configuration `yaml:"network"`
	NetworkStorePath         string                 `yaml:"networkStorePath"`
	NetworkUpdateIntervalSec int                    `yaml:"networkUpdateIntervalSec"`
	Space                    config.Config          `yaml:"space"`
	Storage                  nodestorage.Config     `yaml:"storage"`
	Metric                   metric.Config          `yaml:"metric"`
	Log                      logger.Config          `yaml:"log"`
	NodeSync                 nodesync.Config        `yaml:"nodeSync"`
	Yamux                    yamux.Config           `yaml:"yamux"`
	Limiter                  limiter.Config         `yaml:"limiter"`
	Quic                     quic.Config            `yaml:"quic"`
}

func (c Config) Init(a *app.App) (err error) {
	return
}

func (c Config) Name() (name string) {
	return CName
}

func (c Config) GetDrpc() rpc.Config {
	return c.Drpc
}

func (c Config) GetDebugServer() debugserver.Config {
	return c.APIServer
}

func (c Config) GetAccount() commonaccount.Config {
	return c.Account
}

func (c Config) GetMetric() metric.Config {
	return c.Metric
}

func (c Config) GetSpace() config.Config {
	return c.Space
}

func (c Config) GetStorage() nodestorage.Config {
	return c.Storage
}

func (c Config) GetNodeConf() nodeconf.Configuration {
	return c.Network
}

func (c Config) GetNodeConfStorePath() string {
	return c.NetworkStorePath
}

func (c Config) GetNodeConfUpdateInterval() int {
	return c.NetworkUpdateIntervalSec
}

func (c Config) GetNodeSync() nodesync.Config {
	return c.NodeSync
}

func (c Config) GetLimiterConf() limiter.Config {
	return c.Limiter
}

func (c Config) GetHotSync() hotsync.Config {
	return c.NodeSync.HotSync
}

func (c Config) GetYamux() yamux.Config {
	return c.Yamux
}

func (c Config) GetQuic() quic.Config {
	return c.Quic
}

func (c Config) GetStreamConfig() streampool.StreamConfig {
	return streampool.StreamConfig{
		SendQueueSize:    100,
		DialQueueWorkers: 4,
		DialQueueSize:    1000,
	}
}
