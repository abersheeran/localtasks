package localtasks

import (
	"sync"

	"github.com/caarlos0/env/v9"
	"github.com/google/uuid"
)

type SpeedLimit struct {
	MaxConcurrent int `env:"MAX_CONCURRENT" envDefault:"300"`
}

type Retry struct {
	Timeout     float64 `env:"TIMEOUT" envDefault:"1800"`
	MaxRetries  int     `env:"MAX_RETRIES" envDefault:"0"`
	MinInterval float64 `env:"MIN_INTERVAL" envDefault:"0.1"`
	MaxInterval float64 `env:"MAX_INTERVAL" envDefault:"3600"`
	MaxDoubling int     `env:"MAX_DOUBLING" envDefault:"16"`
}

type Settings struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"INFO"`

	RedisDsn string `env:"REDIS" envDefault:"redis://localhost:6379/0"`

	ApiToken string `env:"API_TOKEN,unset"`

	ConsumerName string `env:"CONSUMER_NAME"`

	SpeedLimit SpeedLimit

	Retry Retry
}

func newSettings() *Settings {
	settings := Settings{}
	if err := env.Parse(&settings); err != nil {
		panic(err)
	}
	if (settings.ConsumerName) == "" {
		settings.ConsumerName = uuid.New().String()
	}
	return &settings
}

var once sync.Once
var settings *Settings

func NewSettings() *Settings {
	once.Do(func() {
		settings = newSettings()
	})
	return settings
}
