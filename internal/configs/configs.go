package configs

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

const (
	opConfig = "configs."

	localConfigPath = "configs/local.yaml"
)

type Configs struct {
	DbConfig    DbConfigs    `yaml:"db" env-required:"true"`
	RedisConfig RedisConfigs `yaml:"redis" env-required:"true"`
	NatsConfig  NatsConfigs  `yaml:"nats" env-required:"true"`
	BaseDir     string
	configPath  string
}

type DbConfigs struct {
	User     string `yaml:"user" env-required:"true"`
	Password string `yaml:"password" env-required:"true"`
	Host     string `yaml:"host" default:"localhost"`
	Port     string `yaml:"port" default:"5432"`
	DbName   string `yaml:"db_name" env-required:"true"`
}

type RedisConfigs struct {
	Addr        string        `yaml:"redis_addr" env-default:"127.0.0.1:6379"`
	Password    string        `yaml:"password"`
	User        string        `yaml:"user"`
	DB          int           `yaml:"db" env-default:"1"`
	MaxRetries  int           `yaml:"max_retries" env-default:"5"`
	RetryDelay  time.Duration `yaml:"retry_delay" env-default:"5s"` // Задержка между переподключениями
	DialTimeout time.Duration `yaml:"dial_timeout" env-default:"10s"`
	Timeout     time.Duration `yaml:"timeout" env-default:"5s"`
}

type NatsConfigs struct {
	QueueName string `yaml:"queue" env-required:"true"`
}

func NewConfigs() (*Configs, error) {
	const op = opConfig + "NewConfigs"

	config := &Configs{}
	if err := config.setBaseDir(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if err := cleanenv.ReadConfig(filepath.Join(config.BaseDir, localConfigPath), config); err != nil {
		return nil, fmt.Errorf("%s: failed to read config: %w", op, err)
	}

	config.configPath = filepath.Join(config.BaseDir, localConfigPath)
	return config, nil
}

func (c *Configs) setBaseDir() error {
	const op = opConfig + "setBaseDir"

	absPath, err := filepath.Abs(localConfigPath)
	if err != nil {
		return fmt.Errorf("%s: cannot get absolute path: %w", op, err)
	}

	c.BaseDir = filepath.Dir(filepath.Dir(absPath))
	return nil
}
