package archive

type configSource interface {
	GetArchive() Config
}

type Config struct {
	Enabled            bool `yaml:"enabled"`
	ArchiveAfterDays   int  `yaml:"archiveAfterDays"`
	CheckPeriodMinutes int  `yaml:"checkPeriodMinutes"`
}
