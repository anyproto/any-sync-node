package archive

type configSource interface {
	GetArchive() Config
}

type Config struct {
	Enabled            bool `yaml:"enabled"`
	ArchiveAfterDays   int  `yaml:"archiveAfterDays"`
	CheckPeriodMinutes int  `yaml:"checkPeriodMinutes"`
	// SweepPeriodHours is the period of the archive-prefix garbage collector
	// (removes stale objects that no longer back an archived space), default 24.
	SweepPeriodHours int `yaml:"sweepPeriodHours"`
}
