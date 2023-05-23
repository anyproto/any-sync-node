package nodesync

import "github.com/anyproto/any-sync-node/nodesync/hotsync"

type configGetter interface {
	GetNodeSync() Config
}

type Config struct {
	SyncOnStart       bool           `yaml:"syncOnStart"`
	PeriodicSyncHours int            `yaml:"periodicSyncHours"`
	HotSync           hotsync.Config `yaml:"hotSync"`
}
