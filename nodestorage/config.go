package nodestorage

import (
	"fmt"
	"strings"
)

type configGetter interface {
	GetStorage() Config
}

type Config struct {
	Path         string `yaml:"path"`
	AnyStorePath string `yaml:"anyStorePath"`
	// AnyStoreSynchronous is the SQLite synchronous level of the space stores:
	// off, normal or full, in any case; empty means off. With off, a host
	// crash or power loss can lose writes SQLite already reported committed:
	// a node in a network resyncs them from its peers, a single node has none
	// to resync from. SQLite's extra is not accepted: any-store runs in WAL
	// mode, where extra behaves as full. The index store keeps SQLite's
	// default.
	AnyStoreSynchronous string `yaml:"anyStoreSynchronous"`
}

// defaultAnyStoreSynchronous is the space stores' synchronous level when
// AnyStoreSynchronous is empty.
const defaultAnyStoreSynchronous = "off"

// anyStoreSynchronous returns the configured synchronous level in lower case,
// or the default when it is empty.
func (c Config) anyStoreSynchronous() (string, error) {
	if c.AnyStoreSynchronous == "" {
		return defaultAnyStoreSynchronous, nil
	}
	switch level := strings.ToLower(c.AnyStoreSynchronous); level {
	case "off", "normal", "full":
		return level, nil
	}
	return "", fmt.Errorf("storage.anyStoreSynchronous: invalid value %q, want off, normal or full", c.AnyStoreSynchronous)
}
