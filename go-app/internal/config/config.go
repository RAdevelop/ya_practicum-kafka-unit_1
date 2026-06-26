package config

import (
	"github.com/struct0x/envconfig"
)

type Config struct {
	Producer *producer `envPrefix:"PRODUCER"`
	Consumer *consumer `envPrefix:"CONSUMER"`
}

func (c *Config) Load(envFilePath string) {
	if err := envconfig.Read(c, envconfig.EnvFileLookup(envFilePath)); err != nil {
		panic(err)
	}
}

type producer struct {
	BootstrapServers               string `env:"BOOTSTRAP_SERVERS" envDefault:"kafka-b-1:9092"`
	Acks                           string `env:"ACKS" envDefault:"all"`
	Retries                        int    `env:"RETRIES" envDefault:"10"`
	RetryBackoffMs                 int    `env:"RETRY_BACKOFF_MS" envDefault:"100"`
	EnableIdempotence              bool   `env:"ENABLE_IDEMPOTENCE" envDefault:"true"`
	FlushTimeoutMs                 int    `env:"FLUSH_TIMEOUT_MS" envDefault:"15000"`
	SocketConnectionSetupTimeoutMs int    `env:"SOCKET_CONNECTION_SETUP_TIMEOUT_MS" envDefault:"10000"`
	SocketTimeoutMs                int    `env:"SOCKET_TIMEOUT_MS" envDefault:"30000"`
}

type consumer struct {
	BootstrapServers string `env:"bootstrap.servers" envDefault:"kafka-b-1:9092"`
	GroupId          string `env:"group.id" envDefault:""`
	AutoOffsetReset  string `env:"auto.offset.reset" envDefault:"earliest"`
	EnableAutoCommit bool   `env:"enable.auto.commit" envDefault:"false"`
	FetchMinBytes    int    `env:"fetch.min.bytes" envDefault:"1"`
	FetchMaxWaitMs   int    `env:"fetch.max.wait.ms" envDefault:"100"`
}
