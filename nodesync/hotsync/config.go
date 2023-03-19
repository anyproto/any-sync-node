package hotsync

type Config struct {
	SimultaneousRequests int `yaml:"simultaneousRequests"`
}

type configGetter interface {
	GetHotSync() Config
}
