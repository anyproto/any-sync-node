package archivestore

type configSource interface {
	GetS3Store() Config
}

type Credentials struct {
	AccessKey string `yaml:"accessKey"`
	SecretKey string `yaml:"secretKey"`
}

type Config struct {
	Enabled        bool        `yaml:"enabled"`
	Profile        string      `yaml:"profile"`
	Region         string      `yaml:"region"`
	Bucket         string      `yaml:"bucket"`
	Endpoint       string      `yaml:"endpoint"`
	Credentials    Credentials `yaml:"credentials"`
	ForcePathStyle bool        `yaml:"forcePathStyle"`
	KeyPrefix      string      `yaml:"keyPrefix"`
}
