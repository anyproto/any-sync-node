package nodesync

type configGetter interface {
	GetNodeSync() Config
}

type Config struct {
	SyncOnStart bool `yaml:"syncOnStart"`
}
